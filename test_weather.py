import asyncio
from weather_arb.data_fetcher import fetch_open_meteo_forecast
async def main():
    res = await fetch_open_meteo_forecast("NYC")
    print(res)
asyncio.run(main())
