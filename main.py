import shutil
from collections import defaultdict, Counter
import csv
import tempfile
from pathlib import Path
from zipfile import ZipFile, is_zipfile
import pandas as pd


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

    def count_hotels(self):
        self.unzip_files()
        print(self.read_csv())

    def unzip_files(self):
        for file in self.input_path.iterdir():
            if is_zipfile(file):
                with ZipFile(file, mode="r") as archive:
                    archive.extractall(path=self.temp_path)

    def read_csv(self):
        hotels_df = pd.concat(
            [
                pd.read_csv(file, usecols=[1, 4, 5])
                for file in self.temp_path.iterdir()
                if file.name.endswith(".csv")
            ]
        ).dropna()
        hotels_df = hotels_df[hotels_df["Latitude"].apply(self.is_float)]
        hotels_df = hotels_df[hotels_df["Longitude"].apply(self.is_float)]
        hotels_df['Latitude'] = hotels_df['Latitude'].astype(float)
        hotels_df['Longitude'] = hotels_df['Longitude'].astype(float)
        hotels_df = hotels_df[hotels_df["Latitude"].apply(lambda x: abs(x) <= 90)]
        hotels_df = hotels_df[hotels_df["Longitude"].apply(lambda x: abs(x) <= 180)]
        return hotels_df

    @staticmethod
    def check_row(row):
        if all([row["Name"], row["Latitude"], row["Longitude"]]):
            return all(
                [abs(float(row["Latitude"])) <= 90, abs(float(row["Longitude"])) <= 180]
            )

    @staticmethod
    def is_float(string):
        try:
            float(string)
            return True
        except ValueError:
            return False



if __name__ == "__main__":
    hotels = FileHandler(r"D:\PyProjects\Weather_Analysis\Data")
    hotels.count_hotels()
