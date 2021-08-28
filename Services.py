import asyncio
import os
import random
import shutil
import tempfile
import time
from pathlib import Path
from typing import List
from zipfile import ZipFile, is_zipfile

import aiohttp
import pandas as pd
from geopy import PickPoint, adapters


class FileHandler:
    def __init__(self, input_folder, output_folder=None):
        self.input_path = input_folder
        self.output_path = output_folder
        self.temp_path = Path(tempfile.mkdtemp())
        self.hotels_df = None

        self.unzip_files()
        self.read_csv()
        self.clear_rows()

        # self.create_folders()

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


class AsyncGeopy:
    """Service class receives a list of urls and gets text from pages with async"""

    def __init__(self, urls: List[str]) -> None:
        """Construction method

        :param urls: List of urls that will be fetched with async
        :type urls: List of strings
        :return: None
        :rtype: NoneType
        """
        self.urls = urls

    def get(self) -> List[str]:
        """Function that creates loop and waits until all runs will be completed

        :return: Returns list of received texts from pages with
        :rtype: List of strings
        """
        loop = asyncio.get_event_loop()
        pages = loop.run_until_complete(self.main(loop))
        return pages

    async def main(self, loop: "loop") -> "loop":
        """Function that creates tasks and waits for results to gather

        :param loop: Object loop
        :type loop: Object
        :return: Finished loop
        :rtype: Loop
        """

        tasks = [self.fetch(url) for url in self.urls]
        results = await asyncio.gather(*tasks)
        return results

    @staticmethod
    async def fetch(url: str) -> str:
        """Static method that fetches text from received url

        :param url: Url that will be fetched
        :type url: String
        :return: Returns received text from url
        :rtype: String
        """
        for _ in range(10):
            try:
                async with PickPoint(
                    api_key=os.environ["PickPoint_API"],
                    adapter_factory=adapters.AioHTTPAdapter,
                ) as geolocator:
                    result = await geolocator.reverse(url).address
                    return result
            except:
                await asyncio.sleep(random.randint(1, 3))


class AsyncOpenWeather:
    """Service class receives a list of urls and gets text from pages with async"""

    def __init__(self, coordinate_list, history_days=5, max_requests=60) -> None:
        """Construction method

        :param coordinate_list: List of urls that will be fetched with async
        :type coordinate_list: List of strings
        :return: None
        :rtype: NoneType
        """
        self.coordinate_list = coordinate_list
        self.history_days = history_days
        self.max_requests = max_requests
        self.requests_count = 0
        self.api_key = os.environ["OpenWeather_API"]
        self.now = int(time.time())
        self.urls = []
        self.create_urls()

    def get(self) -> List[str]:
        """Function that creates loop and waits until all runs will be completed

        :return: Returns list of received texts from pages with
        :rtype: List of strings
        """
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.main(loop))

    async def main(self, loop: "loop") -> "loop":
        """Function that creates tasks and waits for results to gather

        :param loop: Object loop
        :type loop: Object
        :return: Finished loop
        :rtype: Loop
        """
        async with aiohttp.ClientSession(loop=loop) as session:
            tasks = [self.fetch(session, url) for url in self.urls]
            results = await asyncio.gather(*tasks)
            return results

    def create_urls(self):

        for loc in self.coordinate_list:
            self.urls.append(
                f"https://api.openweathermap.org/data/2.5/onecall?"
                f"lat={loc[0]}&lon={loc[1]}&exclude=hourly,minutely,"
                f"alerts&units=metric&appid={self.api_key}"
            )

            for days in range(1, self.history_days + 1):
                get_time = self.now - 86400 * days
                self.urls.append(
                    f"http://api.openweathermap.org/data/2.5/onecall/"
                    f"timemachine?lat={loc[0]}&lon={loc[1]}&dt={get_time}"
                    f"&units=metric&appid={self.api_key}"
                )

    async def fetch(self, session: "session", url: str) -> str:
        """Static method that fetches text from received url

        :param url: Url that will be fetched
        :type url: String
        :return: Returns received text from url
        :rtype: String
        """
        for try_ in range(10):
            if self.requests_count == self.max_requests:
                self.requests_count = 0
                time.sleep(60)

            try:
                self.requests_count += 1
                async with session.get(url) as response:
                    return await response.json()
            except:
                await asyncio.sleep(1)
