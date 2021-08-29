import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from Services import AsyncGetAPI, FileHandler, OpenWeather, PickPoint


class WeatherAnalysis:
    def __init__(self, indir, outdir=None, max_hotels=3, threads=100):
        self.input_path = Path(indir)
        self.output_path = Path(outdir) if outdir else self.input_path / "Output"
        self.max_hotels = max_hotels
        self.threads = threads
        self.hotels_df = None
        self.most_hotels = None
        self.city_center = None
        self.city_weather = None

    def run(self):
        fh = FileHandler(self.input_path, self.output_path)
        self.hotels_df = fh.hotels_df
        self.most_hotels = self.find_cities_with_most_hotels()
        self.get_most_dfs()
        self.city_center = self.get_city_centers()
        self.city_weather = OpenWeather(self.city_center, self.threads).results
        fh.create_folders(self.most_hotels)
        self.create_charts()
        self.create_json_wth_analysis()
        self.hotels_to_csv()

    def find_cities_with_most_hotels(self):
        hotel_counter = defaultdict(Counter)

        for index, row in self.hotels_df.iterrows():
            hotel_counter[row["Country"]][row["City"]] += 1

        return {
            (key, val.most_common(1)[0][0]): None for key, val in hotel_counter.items()
        }

    def get_most_dfs(self):
        for country, city in self.most_hotels:
            filtered_hotels = self.hotels_df[self.hotels_df["Country"] == country]
            filtered_hotels = filtered_hotels[filtered_hotels["City"] == city]
            self.most_hotels[(country, city)] = filtered_hotels

    def get_city_centers(self):
        city_centers = {}

        for location, df in self.most_hotels.items():
            latitude = (df["Latitude"].min() + df["Latitude"].max()) / 2
            longitude = (df["Longitude"].min() + df["Longitude"].max()) / 2
            city_centers[location] = (latitude, longitude)

        return city_centers

    def create_charts(self):
        for country, city in self.most_hotels:
            fig = plt.figure()
            weather = self.city_weather[(country, city)]
            plt.plot(
                [datetime.fromtimestamp(d[0]).strftime("%d.%m") for d in weather],
                [d[1] for d in weather],
            )
            plt.plot(
                [datetime.fromtimestamp(d[0]).strftime("%d.%m") for d in weather],
                [d[2] for d in weather],
            )
            fig.savefig(self.output_path / country / city / "chart.png")

    def create_json_wth_analysis(self):
        weather = list(self.city_weather.items())
        max_temp_city, max_temp_date = self.max_temp_city(weather)
        min_temp_city, min_temp_date = self.min_temp_city(weather)
        delta_temp_city, delta_temp_date = self.delta_temp(weather)
        delta_max_temp = self.delta_max_temp(weather)
        data = {
            "Maximum Temperature": {
                "City": max_temp_city,
                "Date": datetime.fromtimestamp(max_temp_date).strftime("%d.%m.%Y"),
            },
            "Minimum Temperature": {
                "City": min_temp_city,
                "Date": datetime.fromtimestamp(min_temp_date).strftime("%d.%m.%Y"),
            },
            "Maximum delta of maximum temperatures": {
                "City": delta_max_temp,
            },
            "Maximum delta of minimum and maximum temperatures": {
                "City": delta_temp_city,
                "Date": datetime.fromtimestamp(delta_temp_date).strftime("%d.%m.%Y"),
            },
        }
        with open(self.output_path / "analysis.json", mode="w") as fl:
            json.dump(data, fl, ensure_ascii=False, indent=4)

    @staticmethod
    def max_temp_city(data):
        data = sorted(data, key=lambda x: max(t[2] for t in x[1]))[-1]
        return data[0][1], sorted(data[1], key=lambda x: x[1])[-1][0]

    @staticmethod
    def min_temp_city(data):
        data = sorted(data, key=lambda x: min(t[1] for t in x[1]))[0]
        return data[0][1], sorted(data[1], key=lambda x: x[1])[0][0]

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

    def hotels_to_csv(self):
        hotels_df = pd.concat(
            [df[: self.max_hotels] for df in self.most_hotels.values()]
        )
        hotels_df["Address"] = PickPoint(
            [(row["Latitude"], row["Longitude"]) for _, row in hotels_df.iterrows()],
            self.threads,
        ).results

        for country, city in self.most_hotels:
            city_df = hotels_df[
                (hotels_df["Country"] == country) & (hotels_df["City"] == city)
            ][["Name", "Address", "Latitude", "Longitude"]]
            city_df.to_csv(
                self.output_path / country / city / "Hotels.csv", index=False
            )


if __name__ == "__main__":
    test = WeatherAnalysis(r"D:\PyProjects\Weather_Analysis\tests\Data").run()
    # print("End")
