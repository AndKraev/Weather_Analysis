import json
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from Services import FileHandler, OpenWeather, PickPoint


@dataclass
class City:
    name: str
    country: str
    hotels: pd.DataFrame
    latitude: float = None
    longitude: float = None
    weather: list = None

    def __post_init__(self):
        self.latitude = (self.hotels["Latitude"].min() + self.hotels[
            "Latitude"].max()) / 2
        self.longitude = (self.hotels["Longitude"].min() + self.hotels[
            "Longitude"].max()) / 2


@dataclass
class TempData:
    temp: float = float("-inf")
    date: int = None
    city: str = None


class AnalyseWeather:
    def __init__(self, input_folder, output_folder, max_workers):
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.max_workers = max_workers
        self.all_hotels = None
        self.hotels_counter = defaultdict(Counter)
        self.cities = []

    def run(self):
        all_hotels = FileHandler(self.input_folder, self.output_folder).hotels_df
        self.count_hotels_in_cities(all_hotels)
        self.build_cities_with_most_hotels(all_hotels)

        self.create_output_folders()

        self.fetch_city_weather()
        self.find_cities_and_dates_with_top_temp_values()
        self.create_temp_charts()
        self.create_csv_files()

    def count_hotels_in_cities(self, all_hotels):
        for _, row in all_hotels.iterrows():
            self.hotels_counter[row["Country"]][row["City"]] += 1

    def build_cities_with_most_hotels(self, all_hotels):
        for country, cities in self.hotels_counter.items():
            city = cities.most_common(1)[0][0]
            self.cities.append(
                City(name=city, country=country,
                     hotels=all_hotels[all_hotels["City"] == city])
            )

    def fetch_city_weather(self):
        weather_list = OpenWeather(
            [(city.latitude, city.longitude) for city in self.cities],
            threads=self.max_workers).results

        for city, weather in zip(self.cities, weather_list):
            city.weather = weather

    def create_output_folders(self):
        for city in self.cities:
            Path(self.output_folder / city.country / city.name).mkdir(parents=True,
                                                                      exist_ok=True)

    def find_cities_and_dates_with_top_temp_values(self):
        max_temp = TempData()
        min_temp = TempData(temp=float("+inf"))
        delta_max_temp = TempData()
        delta_max_min_temp = TempData()

        for city in self.cities:
            city_max_temp = max([day[2] for day in city.weather])
            city_min_temp = min([day[1] for day in city.weather])
            city_delta_max_temp = city_max_temp - min([day[2] for day in city.weather])
            city_delta_max_min_temp = max([day[2] - day[1] for day in city.weather])

            if city_max_temp > max_temp.temp:
                max_temp.temp = city_max_temp
                max_temp.city = city.name
                max_temp.date = sorted(city.weather, key=lambda x: x[2])[-1][0]

            if city_min_temp < min_temp.temp:
                min_temp.temp = city_min_temp
                min_temp.city = city.name
                min_temp.date = sorted(city.weather, key=lambda x: x[1])[0][0]

            if city_delta_max_temp > delta_max_temp.temp:
                delta_max_temp.temp = city_delta_max_temp
                delta_max_temp.city = city.name

            if city_delta_max_min_temp > delta_max_min_temp.temp:
                delta_max_min_temp.temp = city_max_temp
                delta_max_min_temp.city = city.name
                delta_max_min_temp.date = sorted(
                    city.weather, key=lambda x: x[2] - x[1]
                )[-1][0]

        self.create_json_with_analysis(
            max_temp, min_temp, delta_max_temp, delta_max_min_temp
        )

    def create_json_with_analysis(self, max_temp, min_temp, delta_max_temp, delta_max_min_temp):
        data = {
            "Maximum Temperature": {
                "City": max_temp.city,
                "Date": datetime.fromtimestamp(max_temp.date).strftime("%d.%m.%Y"),
            },
            "Minimum Temperature": {
                "City": min_temp.city,
                "Date": datetime.fromtimestamp(min_temp.date).strftime("%d.%m.%Y"),
            },
            "Maximum delta of maximum temperatures": {
                "City": delta_max_temp.city,
            },
            "Maximum delta of minimum and maximum temperatures": {
                "City": delta_max_min_temp.city,
                "Date": datetime.fromtimestamp(delta_max_min_temp.date).strftime("%d.%m.%Y"),
            },
        }

        with open(self.output_folder / "analysis.json", mode="w") as fl:
            json.dump(data, fl, ensure_ascii=False, indent=4)

    def create_temp_charts(self):
        for city in self.cities:
            fig = plt.figure()

            for num in range(1, 3):
                plt.plot(
                    [datetime.fromtimestamp(d[0]).strftime("%d.%m") for d in
                     city.weather],
                    [d[num] for d in city.weather],
                )

            fig.savefig(self.output_folder / city.country / city.name / "chart.png")

    def create_csv_files(self):
        all_hotels = pd.concat([city.hotels[:3] for city in self.cities])
        addresses = PickPoint([(row["Latitude"], row["Longitude"]) for _, row in all_hotels.iterrows()], self.max_workers).results
        all_hotels["Address"] = addresses
        for city in self.cities:
            city_df = all_hotels[
                (all_hotels["Country"] == city.country) & (all_hotels["City"] == city.name)
                ][["Name", "Address", "Latitude", "Longitude"]]
            city_df.to_csv(
                self.output_folder / city.country / city.name / "Hotels.csv", index=False
            )


if __name__ == '__main__':
    w = AnalyseWeather(r"D:\PyProjects\Weather_Analysis\tests\Data",
                       r"D:\PyProjects\Weather_Analysis\tests\Data\Output1", 100)
    w.run()
    ...
