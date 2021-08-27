import asyncio
import json
import os
import random
import shutil
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import List
from zipfile import ZipFile, is_zipfile
from datetime import datetime

import pandas as pd
import requests as requests
from geopy import PickPoint, adapters
import matplotlib.pyplot as plt


class WeatherAnalysis:
    def __init__(self, input_folder, output_folder=None):
        self.input_path = Path(input_folder)
        self.output_path = (
            Path(output_folder) if output_folder else self.input_path / "Output"
        )
        self.most_hotels = {}
        self.city_weather = {}
        self.city_center = {}

        fh = FileHandler(self.input_path, self.output_path)
        self.hotels_df = fh.hotels_df
        self.count_hotels()
        self.get_most_dfs()
        self.get_city_centers()
        self.get_weather()
        fh.create_folders(self.most_hotels)
        self.create_charts()
        self.max_temp()
        print("End")

    def count_hotels(self):
        hotel_counter = defaultdict(Counter)

        for index, row in self.hotels_df.iterrows():
            hotel_counter[row["Country"]][row["City"]] += 1

        self.most_hotels.update(
            {
                (key, val.most_common(1)[0][0]): None
                for key, val in hotel_counter.items()
            }
        )

    def get_most_dfs(self):
        for country, city in self.most_hotels:
            filtered_hotels = self.hotels_df[self.hotels_df["Country"] == country]
            filtered_hotels = filtered_hotels[filtered_hotels["City"] == city]
            self.most_hotels[(country, city)] = filtered_hotels

    def get_city_centers(self):
        for location, df in self.most_hotels.items():
            latitude = (df["Latitude"].min() + df["Latitude"].max()) / 2
            longitude = (df["Longitude"].min() + df["Longitude"].max()) / 2
            self.city_center[location] = (latitude, longitude)

    def get_weather(self):
        api_key = os.environ["OpenWeather_API"]

        for location in self.most_hotels:
            lat = self.city_center[location][0]
            lon = self.city_center[location][1]
            url = f"https://api.openweathermap.org/data/2.5/onecall?" \
                  f"lat={lat}&lon={lon}&exclude=hourly,minutely,alerts&units=metric&" \
                  f"appid={api_key}"
            fc = requests.get(url).json()
            print(fc)
            weather = [
                (
                    fc["daily"][day]["dt"],
                    fc["daily"][day]["temp"]["min"],
                    fc["daily"][day]["temp"]["max"],
                )
                for day in range(6)
            ]

            now = weather[0][0]
            for days in range(1, 6):
                time = now - 86400 * days
                url = f"http://api.openweathermap.org/data/2.5/onecall/timemachine?" \
                      f"lat={lat}&lon={lon}&dt={time}&units=metric&appid={api_key}"
                hd = requests.get(url).json()
                temp = [d["temp"] for d in hd["hourly"]]
                weather.append((time, min(temp), max(temp)))

            self.city_weather[location] = sorted(weather, key=lambda x: x[0])

    def create_charts(self):
        for country, city in self.most_hotels:
            fig = plt.figure()
            weather = self.city_weather[(country, city)]
            plt.plot([datetime.fromtimestamp(d[0]).strftime("%d.%m") for d in weather],
                     [d[1] for d in weather])
            plt.plot([datetime.fromtimestamp(d[0]).strftime("%d.%m") for d in weather],
                     [d[2] for d in weather])
            fig.savefig(self.output_path / country / city / "chart.png")
            break

    def max_temp(self):
        weather = list(self.city_weather.items())
        max_temp_city, max_temp_date = self.max_temp_city(weather)
        min_temp_city, min_temp_date = self.min_temp_city(weather)
        delta_temp_city, delta_temp_date = self.delta_temp(weather)
        delta_max_temp = self.delta_max_temp(weather)
        data = {
            "Maximum Temperature": {
                "City": max_temp_city,
                "Date": datetime.fromtimestamp(max_temp_date).strftime("%d.%m.%Y")
            },
            "Minimum Temperature": {
                "City": min_temp_city,
                "Date": datetime.fromtimestamp(min_temp_date).strftime("%d.%m.%Y")
            },
            "Maximum delta of maximum temperatures": {
                "City": delta_max_temp,
            },
            "Maximum delta of minimum and maximum temperatures": {
                "City": delta_temp_city,
                "Date": datetime.fromtimestamp(delta_temp_date).strftime("%d.%m.%Y")
            }
        }
        with open(self.output_path / "analysis.json", mode="w") as fl:
            json.dump(data, fl, ensure_ascii=False, indent=4)

    @staticmethod
    def max_temp_city(data):
        data = sorted(data, key=lambda x: max(t[2] for t in x[1]))[-1]
        return data[0][1], sorted(data[1], key=lambda x: x[1])[-1][0]

    @staticmethod
    def min_temp_city(data):
        data = sorted(data, key=lambda x: max(t[1] for t in x[1]))[0]
        return data[0][1], sorted(data[1], key=lambda x: x[2])[0][0]

    @staticmethod
    def delta_max_temp(data):
        data = sorted(
            data, key=lambda x: max(t[2] for t in x[1]) - min(t[2] for t in x[1])
        )[-1]
        return data[0][1]

    @staticmethod
    def delta_temp(data):
        data = sorted(data, key=lambda x: max(t[2] - t[1] for t in x[1]))[-1]
        return data[0][1], sorted(data[1], key=lambda x: x[2] - x[1])[-1][0]

    # def update_df(self):
    # filtered_hotels = filtered_hotels[["Name", 'Address', "Latitude", "Longitude"]]


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


class AsyncFetch:
    """Service class receives a list of urls and gets text from pages with async"""

    def __init__(self, urls: List[str]) -> None:
        """Construction method

        :param urls: List of urls that will be fetched with async
        :type urls: List of strings
        :return: None
        :rtype: NoneType
        """
        self.urls = urls
        with open("cache.json", "r") as fl:
            self.cache = json.load(fl)
        print(f"{len(self.cache)=}")

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
        with open("cache.json", "w") as fl:
            json.dump(self.cache, fl)
        return results

    async def fetch(self, coordinates: str) -> str:
        """Static method that fetches text from received url

        :param coordinates: Url that will be fetched
        :type coordinates: String
        :return: Returns received text from url
        :rtype: String
        """
        # if url in cache:
        #     return cache
        if coordinates in self.cache:
            return self.cache[coordinates]
        for _ in range(10):
            try:
                async with PickPoint(
                    api_key=os.environ["PickPoint_API"],
                    adapter_factory=adapters.AioHTTPAdapter,
                ) as geolocator:
                    result = await geolocator.reverse(coordinates).address
                    self.cache.update({coordinates: result})
                    return result
            except:
                await asyncio.sleep(random.randint(1, 3))


if __name__ == "__main__":
    test = WeatherAnalysis(r"D:\PyProjects\Weather_Analysis\Data")

    # print(AsyncFetch(["29.970456, -95.558938", "29.970456, -95.558938"]).get())
