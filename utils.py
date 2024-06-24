import aiohttp
import asyncio
import json
from time import sleep

BASE_URL = "https://api.gios.gov.pl/pjp-api/v1/rest/station/"


async def fetch_data(url: str, max_retries=5) -> dict:
    """
    Fetches data from the provided URL and returns it as a dictionary. In case of error 429 (too many requests), 
    it will retry the request up to max_retries times with increasing wait times.

    Args:
        url (str): The URL to fetch data from.
        max_retries (int): The amount of retries to make if the request fails.

    Returns:
        dict: The data fetched from the URL.

    Raises:
        Exception: If the request fails after all retries.
    """
    wait_time = 1
    retry_count = 0
    
    async with aiohttp.ClientSession() as session:
        while retry_count < max_retries:
            try:
                async with session.get(url) as response:
                    if response.status == 429:
                        retry_count += 1
                        print(f"HTTP 429 Error: Too many requests, waiting {wait_time} seconds")
                        await asyncio.sleep(wait_time)
                        wait_time *= 2
                        continue
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientResponseError as e:
                print(f"HTTP Error: {e}")
                break
            except Exception as e:
                print(f"Error: {e}")
                break
        raise Exception("Failed to fetch data")
    

