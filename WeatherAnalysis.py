import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt

from Services import AsyncGeopy, AsyncOpenWeather, FileHandler


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
        weather_list = AsyncOpenWeather(list(self.city_center.values())).get()

        for ind, city in enumerate(self.city_center):
            weather = weather_list[ind * 6]
            self.city_weather[city] = [
                (
                    weather["daily"][day]["dt"],
                    weather["daily"][day]["temp"]["min"],
                    weather["daily"][day]["temp"]["max"],
                )
                for day in range(6)
            ]

            for day in range(1, 6):
                weather = weather_list[ind * 6 + day]
                temp = [w["temp"] for w in weather["hourly"]]
                self.city_weather[city].append(
                    (weather["current"]["dt"], min(temp), max(temp))
                )

            self.city_weather[city].sort(key=lambda x: x[0])

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

    def max_temp(self):
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
