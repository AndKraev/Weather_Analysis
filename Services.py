import asyncio
import os
import shutil
import tempfile
import time
from pathlib import Path
from zipfile import ZipFile, is_zipfile

import aiohttp
import pandas as pd


class FileHandler:
    def __init__(self, input_folder, output_folder=None):
        self.input_path = input_folder
        self.output_path = output_folder
        self.temp_path = Path(tempfile.mkdtemp())
        self.hotels_df = None

        self.unzip_files()
        self.read_csv()
        self.clear_rows()

    def __del__(self):
        shutil.rmtree(self.temp_path)

    def unzip_files(self):
        for file in self.input_path.iterdir():
            if is_zipfile(file):
                with ZipFile(file, mode="r") as archive:
                    archive.extractall(path=self.temp_path)

    def read_csv(self):
        self.hotels_df = pd.concat(
            [
                pd.read_csv(f, usecols=[1, 2, 3, 4, 5])
                for f in self.temp_path.iterdir()
                if f.name.endswith(".csv")
            ]
        )

    def clear_rows(self):
        df = self.hotels_df.dropna()

        # Delete rows with non-float values in coordinates
        df = df[df["Latitude"].apply(self.is_float)]
        df = df[df["Longitude"].apply(self.is_float)]

        # Convert to Float
        df["Latitude"] = df["Latitude"].astype(float)
        df["Longitude"] = df["Longitude"].astype(float)

        # Delete rows with wrong values in coordinates
        df = df[df["Latitude"].apply(lambda x: abs(x) <= 90)]
        df = df[df["Longitude"].apply(lambda x: abs(x) <= 180)]

        self.hotels_df = df

    def create_folders(self, location):
        for country, city in location:
            city_path = self.output_path / country / city
            city_path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def is_float(string):
        try:
            float(string)
            return True
        except ValueError:
            return False


class PickPoint:
    def __init__(self, locations, threads, max_requests=None):
        self.locations = locations
        self.max_requests = max_requests
        self.urls_list = self.create_api_ulr_list()
        self.results = self.sort_results(
            AsyncGetAPI(self.urls_list, threads, max_requests=self.max_requests).results
        )

    def create_api_ulr_list(self):
        api = os.environ["PickPoint_API"]
        return [
            f"https://api.pickpoint.io/v1/reverse/?key={api}&lat={lat}&lon={lon}"
            f"&accept-language=en-US"
            for lat, lon in self.locations
        ]

    def sort_results(self, results):
        return [results[url]["display_name"] for url in self.urls_list]


class OpenWeather(PickPoint):
    def create_api_ulr_list(self):
        self.max_requests = 60
        api = os.environ["OpenWeather_API"]
        now = int(time.time())
        urls_list = []

        for lat, lon in self.locations.values():
            urls_list.append(
                f"https://api.openweathermap.org/data/2.5/onecall?"
                f"lat={lat}&lon={lon}&exclude=hourly,minutely,"
                f"alerts&units=metric&appid={api}"
            )

            for days in range(1, 6):
                date_time = now - 86400 * days
                urls_list.append(
                    f"http://api.openweathermap.org/data/2.5/onecall/"
                    f"timemachine?lat={lat}&lon={lon}&dt={date_time}"
                    f"&units=metric&appid={api}"
                )

        return urls_list

    def sort_results(self, results):
        sorted_weather = [results[url] for url in self.urls_list]
        all_results = {}

        for num, location in enumerate(self.locations):
            city_weather_list = sorted_weather[num * 6 : (num + 1) * 6]
            city_result = [
                (
                    city_weather_list[0]["daily"][day]["dt"],
                    city_weather_list[0]["daily"][day]["temp"]["min"],
                    city_weather_list[0]["daily"][day]["temp"]["max"],
                )
                for day in range(6)
            ]

            for day in range(1, 6):
                weather = city_weather_list[day]
                temp = [w["temp"] for w in weather["hourly"]]
                city_result.append((weather["current"]["dt"], min(temp), max(temp)))

            all_results[location] = sorted(city_result, key=lambda x: x[0])

        return all_results


class AsyncGetAPI:
    def __init__(self, url_list, max_treads, max_requests=None):
        self.url_list = url_list
        self.max_treads = max(max_treads, len(url_list))
        self.max_requests = max_requests
        self.requests_count = 0
        self.results = {}

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main(loop))

    async def main(self, loop):
        queue = asyncio.Queue()

        for url in self.url_list:
            queue.put_nowait(url)

        async with aiohttp.ClientSession(loop=loop) as session:
            workers = [
                asyncio.create_task(self.worker(queue, session))
                for _ in range(self.max_treads)
            ]
            await queue.join()

            for worker in workers:
                worker.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

    async def worker(self, queue, session):
        while True:
            url = await queue.get()
            await self.fetch(url, session)
            queue.task_done()

    async def fetch(self, url, session):
        for tries in range(10):
            if self.requests_count == self.max_requests:
                time.sleep(60)
                self.requests_count = 0

            self.requests_count += 1

            try:
                async with session.get(url) as response:
                    self.results[url] = await response.json()
                    break
            except:
                await asyncio.sleep(1)


if __name__ == "__main__":
    c = [(59.53732843321865, 34.02956211486147)]
    w = {"London": (59.53732843321865, 34.02956211486147)}
    print(PickPoint(c).results)
    print(OpenWeather(w).results)
