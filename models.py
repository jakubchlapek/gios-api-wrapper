from dataclasses import dataclass, field
from utils import fetch_data, BASE_URL
from requests.exceptions import HTTPError
from datetime import datetime
import asyncio

AMOUNT_PER_PAGE = 500 # 500 is the max for the API

@dataclass
class Installation:
    id: int
    param_name: str
    param_formula: str
    param_code: str
    param_id: int

    def __str__(self):
        return f"installation #{self.id}: '{self.param_formula}'"

    
@dataclass
class Station:
    id: int
    code: str
    name: str
    latitude: float
    longitude: float
    city: dict
    address_street: str
    installations: list[Installation] = field(default_factory=list)


    async def get_installations(self) -> list[Installation, None]:
        """
        For a given station, fetches all of installations with the same station_id from the GIOS API. If necessary, iterates over the pages
        until all are done.

        Returns:
            list[Installation]: A list of Installation objects. 
        """
        current_page = 0
        total_pages = 1
        master_list = []
        updated_total = False
        tasks = []
        while current_page < total_pages:
            try:
                installation_data = await fetch_data(BASE_URL + f"sensors/{self.id}?page={current_page}&size={AMOUNT_PER_PAGE}")
            except HTTPError as e:
                print(f"Error: {e}")
                return []
            if updated_total == False:
                total_pages = installation_data["totalPages"]
                updated_total = True
            current_page += 1
            tasks.append(asyncio.create_task(create_installations(installation_data)))
            #print(f"Completed {current_page}/{total_pages} pages of installations for station {self.id} ({self.name})")
        installations = await asyncio.gather(*tasks)
        for install in installations:
            master_list.extend(install)
        return master_list


    def add_installations(self, installations):
        """
        Appends the argument installation to the installations list of the station.
        """
        if not isinstance(installations, list):
            installations = [installations]
        if not all(isinstance(install, Installation) for install in installations):
            raise ValueError("All installations must be of type Installation")
        for install in installations:
            self.installations.append(install)


    def installation_count(self):
        """
        Returns the amount of installations for the station.
        """
        return len(self.installations)


    def __str__(self):
        installation_str = "\n".join([install.__str__() for install in self.installations])
        return f"Station #{self.id} ({self.name}):\n{installation_str}"


async def create_installations(installation_data: dict) -> list[Installation]:
    """
    Creates a list of Installation objects from the data provided from get_installations. Parses the data and creates the objects from it.

    Args:
        installation_data (dict): The data from the GIOS API.

    Returns:
        list[Installation]: A list of Installation objects.

    Raises:
        ValueError: If the data is not a dictionary/json.
    """
    if not isinstance(installation_data, dict):
        raise ValueError("Data must be a dictionary/json")
    
    # Get the list of installations from data
    installation_data_list = installation_data["Lista stanowisk pomiarowych dla podanej stacji"]

    installations = []
    for data in installation_data_list:
        try:
            installation = Installation(
                id = data["Identyfikator stanowiska"],
                param_name = data["Wskaźnik"],
                param_formula = data["Wskaźnik - wzór"],
                param_code = data["Wskaźnik - kod"],
                param_id = data["Id wskaźnika"],
            )
            installations.append(installation)
        except KeyError:
            print(f"KeyError: {data}")
            continue
    return installations


def create_stations(station_data: dict) -> list[Station]:
    """
    Creates a list of Station objects from the data provided from get_stations. Parses the data and creates the objects from it.

    Args:
        station_data (dict): The data from the GIOS API.

    Returns:
        list[Station]: A list of Station objects.
    """
    if not isinstance(station_data, dict):
        raise ValueError("Data must be a dictionary/json")
    
    # Get the list of stations from data
    station_data_list = station_data["Lista stacji pomiarowych"]

    stations = []
    for data in station_data_list:
        try:
            station = Station(
                id = data["Identyfikator stacji"],
                code = data["Kod stacji"],
                name = data["Nazwa stacji"],
                latitude = data["WGS84 \u03c6 N"],
                longitude = data["WGS84 \u03bb E"],
                city = {
                    "name": data["Nazwa miasta"],
                    "city_id": data["Identyfikator miasta"],
                    "commune": data["Gmina"],
                    "district": data["Powiat"],
                    "province": data["Wojew\u00f3dztwo"]
                },
                address_street = data["Ulica"],                
            )
            stations.append(station)
        except KeyError:
            print(f"KeyError: {data}")
            continue
    return stations


async def get_stations(echo: bool = True) -> list[Station]:
    """
    Fetches all of the stations from the GIOS API. If necessary, iterates over the pages until all are done.

    Returns:
        list[Station]: A list of Station objects.
    """
    current_page = 0
    total_pages = 1
    master_list = []
    updated_total = False # flag to update total_pages as we can't get it before starting the scraping
    current_time = datetime.now() # measuring time
    tasks = []
    print(f"Starting scraping...")
    while current_page < total_pages:
        try:
            station_data = await fetch_data(BASE_URL + f"findAll?page={current_page}&size={AMOUNT_PER_PAGE}")
        except Exception as e:
            raise e
        if updated_total == False: # update total_pages on first iteration only
            total_pages = station_data["totalPages"] 
            updated_total = True
        current_page += 1

        stations = create_stations(station_data)
        for station in stations:
            tasks.append(asyncio.create_task(station.get_installations()))
        master_list.extend(stations)
        print(f"Completed scraping {current_page}/{total_pages} pages of stations.")

    await asyncio.gather(*tasks)
    for station, installs in zip(master_list, await asyncio.gather(*tasks)):
        station.add_installations(installs)
    print(f"Scraping completed in {datetime.now() - current_time}.")
    if echo:
        output_stations(master_list) # Helper function for a task
    return master_list

def output_stations(stations: list) -> None:
    for station in stations:
        print(station.__str__())