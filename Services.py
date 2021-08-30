import asyncio
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import List
from zipfile import ZipFile, is_zipfile

import aiohttp
import pandas as pd

import Setup


class FileHandler:
    def __init__(self, input_folder, output_folder):
        self.input_path = input_folder
        self.output_path = output_folder
        self.temp_path = Path(tempfile.mkdtemp())
        self.unzip_files()
        self.hotels_df = self.read_csv()
        self.clear_rows()

    def __del__(self):
        try:
            shutil.rmtree(self.temp_path)
        except FileNotFoundError:
            pass

    def unzip_files(self):
        for file in self.input_path.iterdir():
            if is_zipfile(file):
                with ZipFile(file, mode="r") as archive:
                    archive.extractall(path=self.temp_path)

    def read_csv(self):
        return pd.concat(
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
    """A class for working with PickPoint API to get addresses for coordinates.
    Receives a list of tuples with latitude and longitude. Creates url addresses to
    PickPoint servers and forwards this list of urls to AsyncGetAPI to obtain data.
    Once receives results it creates list of results ordered by an order of received
    locations and stores it in results attribute"""

    def __init__(self, locations: List[tuple[float, float]], threads: int, max_requests: int = None):
        """Initialize method which runs work and fetches data within AsyncGetAPI

        :param locations: A list of tuples with coordinates where the first float if
        latitude and the second float is longitude of place to fetch an address
        :type locations: A list of tuples with floats.
        :param threads: Maximum number of threads that will be used when AsyncGetAPI
        will be called.
        :type threads: Integer
        :param max_requests: Maximum number of requests in a minute that will be used
        when AsyncGetAPI will be called.
        :return: None
        :rtype: NoneType
        """
        self.locations = locations
        self.max_requests = max_requests
        self.urls_list = self.create_api_ulr_list()
        self.results = self.sort_results(
            AsyncGetAPI(self.urls_list, threads, max_requests=self.max_requests).results
        )

    def create_api_ulr_list(self) -> List[str]:
        """Creates a list of urls for fetching data from PickPoint servers from a list
        of tuples with coordinates. Takes API key from Setup.py. So the key must be
        filled in.

        :return: List with urls to use with AsyncGetAPI
        :rtype: List with strings
        """
        api = Setup.pickpoint_api
        return [
            f"https://api.pickpoint.io/v1/reverse/?key={api}&lat={lat}&lon={lon}"
            f"&accept-language=en-US"
            for lat, lon in self.locations
        ]

    def sort_results(self, results: dict) -> List:
        """Creates a list from AsyncGetAPI ordered by an order of locations that were
        received.

        :param results: Results that were obtained from AsyncGetAPI
        :type results: Dictionary
        :return: A list of results from AsyncGetAPI ordered by an order of locations
        :rtype: List
        """
        return [results[url]["display_name"] for url in self.urls_list]


class OpenWeather:
    def __init__(self, locations, threads, max_requests=60):
        self.max_requests = max_requests
        self.locations = locations
        self.max_requests = max_requests
        self.threads = threads
        self.run()

    def run(self):
        self.urls_list = self.create_api_ulr_list()
        self.results = self.sort_results(
            AsyncGetAPI(
                self.urls_list, self.threads, max_requests=self.max_requests
            ).results
        )

    def create_api_ulr_list(self):
        api = Setup.openweather_api
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
    """Class receives a list of urls and gets JSON responses with asyncio. The number
    of threads is limited with a parameter max_workers or a number of ulrs that should
    be parsed, takes the less. Results are stored in an attribute results as a
    dictionary where keys are string urls and values a JSON responses. Maximum requests
    in a minute may be set with a parameter max_requests (default is not limited)."""

    def __init__(
            self, url_list: List[str], max_workers: int, max_requests: int = None
    ) -> None:
        """Initialize method. Constructs instance and creates a loop.

        :param url_list: List of urls that must be parsed
        :type url_list: List with strings
        :param max_workers: Number of workers that will be spawned
        :type max_workers: Integer
        :param max_requests:  Limit of requests in a minute (default if None)
        :type max_requests: Integer
        :return: None
        :rtype: NoneType
        """
        self.url_list = url_list
        self.max_treads = max(max_workers, len(url_list))
        self.max_requests = max_requests
        self.requests_count = 0
        self.results = {}

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main(loop))

    async def main(self, loop: asyncio.get_event_loop) -> None:
        """Method that creates a queue with urls that must be parsed, creates workers
        in accordance with a parameter max_threads and waits for all tasks to be
        completed. One queue is empty it kills workers and waits until all is done.

        :param loop: loop object
        :type loop: asyncio.get_event_loop
        :return: None
        :rtype: NoneType
        """
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

    async def worker(
            self, queue: asyncio.Queue, session: aiohttp.ClientSession
    ) -> None:
        """Method creates a worker that infinitely takes task one by one from queue,
        forwards it to fetch method and waits for task to be completed

        :param queue: A queue object from asyncio with url tasks
        :type queue: asyncio.Queue
        :param session: aiohttp session object
        :type session: aiohttp.ClientSession
        :return: None
        :rtype: NoneType
        """
        while True:
            url = await queue.get()
            await self.fetch(url, session)
            queue.task_done()

    async def fetch(self, url: str, session: aiohttp.ClientSession) -> None:
        """Method that fetches json from url. It performs 10 tries before giving up
        with a 1 second delay. If maximum number requests equal to maximum number of
        requests, it sleeps for 60 seconds. Updates results attribute with a JSON
        result.

        :param url: A url for parse
        :type url: String
        :param session: aiohttp session object
        :type session: aiohttp.ClientSession
        :return: None
        :rtype: NoneType
        """
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
    # print(PickPoint(c).results)
    print(OpenWeather(w, 100).results)
