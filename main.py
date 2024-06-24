import asyncio
from models import get_stations

if __name__ == "__main__":
    stations = asyncio.run(get_stations(echo=True))
        


