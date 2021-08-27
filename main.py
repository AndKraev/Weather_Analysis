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

import pandas as pd
from geopy import PickPoint, adapters


class FileHandler:
    def __init__(self, input_folder, output_folder=None):
        self.temp_path = Path(tempfile.mkdtemp())
        self.input_path = Path(input_folder)
        self.output_path = (
            Path(output_folder) if output_folder else self.input_path / "Output"
        )
        self.hotel_counter = defaultdict(Counter)

    def __del__(self):
        shutil.rmtree(self.temp_path)

    def main(self):
        self.unzip_files()
        hotels = self.read_csv()
        hotels = self.clear_rows(hotels)
        print(self.count_hotels(hotels))

    def unzip_files(self):
        for file in self.input_path.iterdir():
            if is_zipfile(file):
                with ZipFile(file, mode="r") as archive:
                    archive.extractall(path=self.temp_path)

    def read_csv(self) -> pd.DataFrame:
        return pd.concat(
            [
                pd.read_csv(f, usecols=[1, 2, 3, 4, 5])
                for f in self.temp_path.iterdir()
                if f.name.endswith(".csv")
            ]
        )

    def clear_rows(self, dataframe):
        df = dataframe.dropna()

        # Delete rows with non-float values in coordinates
        df = df[df["Latitude"].apply(self.is_float)]
        df = df[df["Longitude"].apply(self.is_float)]

        # Delete rows with wrong values in coordinates
        df = df[df["Latitude"].apply(lambda x: abs(float(x)) <= 90)]
        df = df[df["Longitude"].apply(lambda x: abs(float(x)) <= 180)]

        return df

    def count_hotels(self, df):
        for index, row in df.iterrows():
            self.hotel_counter[row["Country"]][row["City"]] += 1

        return {key: val.most_common(1)[0][0] for key, val in self.hotel_counter.items()}

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
                    result = await geolocator.reverse(coordinates)
                    self.cache.update({coordinates: result.address})
                    return result.address
            except ValueError:
                await asyncio.sleep(random.randint(1, 1))


if __name__ == "__main__":
    test = FileHandler(r"D:\PyProjects\Weather_Analysis\Data")
    test.main()

    # print(AsyncFetch(["45.787482, 4.7648"] * 1).get())
