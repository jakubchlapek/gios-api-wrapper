A quick project for a task I had. App that asychronously scrapes data from the "Jakość Powietrza GIOŚ" API with use for asyncio.

# Usage
Run main.py

The client-side function is get_stations() which has a boolean "echo" parameter. When set to True (default), it outputs all stations and their assigned installments with their names.
To use it you need to wrap it in asyncio.run()
