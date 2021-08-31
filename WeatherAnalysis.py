"""Contains dataclass City, dataclass TempData and class WeatherAnalysis with a
business logic
"""
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from Services import FileHandler, OpenWeather, PickPoint


@dataclass
class City:
    """Dataclass that stores data about a city. Once instance is created it calculates
    centers of the city

    Constructor arguments:
        :param name: City name
        :param country: Country of the city
        :param hotels: Pandas Dataframe with hotels of the city
        :param latitude: Latitude of the city center
        :param longitude: Longitude of the city center
        :param weather: Weather data of the city center
    """

    name: str
    country: str
    hotels: pd.DataFrame
    latitude: float = None
    longitude: float = None
    weather: list = None

    def __post_init__(self) -> None:
        """Post construction method that calculates and stores city center latitude and
        longitude if these parameters have not been set

        :return: None
        :rtype: NoneType
        """
        if not self.latitude and not self.longitude:
            self.latitude = (
                self.hotels["Latitude"].min() + self.hotels["Latitude"].max()
            ) / 2
            self.longitude = (
                self.hotels["Longitude"].min() + self.hotels["Longitude"].max()
            ) / 2


@dataclass
class TempData:
    """Dataclass that stores data about temperature

    Constructor arguments:
        :param temp: Temperature
        :param date: Date of Temperature
        :param city: City name

    """

    temp: float = float("-inf")
    date: int = None
    city: str = None


class WeatherAnalysis:
    """Fetches data analyzes, addresses and writes data to files.
    Unzips files from input folder to a temporary folder, reads all csv files from the
    temporary folder, finds cities with most numbers of hotels in each country, creates
    City dataclass for each such city. Once done fetches weather data for each city:
    current weather, forecast for 5 days and weather history for the last 5 days.
    Writes top temperature values for all cities to JSON file: city and date for maximum
    temperature, city and date for minimum temperature, city for a maximum delta between
    maximum maximum temperature value and minimum maximum temperature value, city and
    date for a day delta between maximum temperature value and minimum temperature
    value. Writes a temperature chart for each city and csv files with hotels. To run
    instance run method must be used.

    Constructor arguments:
        :param input_folder: Path to input folder with zip files.
        :type input_folder: String.
        :param output_folder: Path to out folder. By default is None and creates a
        folder "Output" inside the input folder.
        :type input_folder: Optional string.
        :param max_workers: A maximum number of threads to be used for fetching data.
            By default is 1000.
        :type max_workers: Integer
        :param max_hotels: A maximum number of hotels that will be written to csv files.
            By default is 100.
        :type max_hotels: Integer
        :return: None
        :rtype: NoneType
    """

    def __init__(
        self,
        input_folder: str,
        output_folder: str = None,
        max_workers: int = 1000,
        max_hotels: int = 100,
    ) -> None:
        """Constructor method"""
        self.input_folder = Path(input_folder)
        self.output_folder = (
            Path(output_folder) if output_folder else self.input_folder / "Output"
        )
        self.max_workers = max_workers
        self.max_hotels = max_hotels
        self._hotels_counter = defaultdict(Counter)
        self._cities = []

    def run(self) -> None:
        """Runs instance methods to obtain and write data.  Unzips files from input
        folder to a temporary folder, reads all csv files from the temporary folder,
        finds cities with most numbers of hotels in each country, creates City
        dataclass for each such city. Once done fetches weather data for each city:
        current weather, forecast for 5 days and weather history for the last 5 days.
        Writes top temperature values for all cities to JSON file: city and date for
        maximum temperature, city and date for minimum temperature, city for a maximum
        delta between maximum maximum temperature value and minimum maximum temperature
        value, city and date for a day delta between maximum temperature value and
        minimum temperature value. Writes a temperature chart for each city and csv
        files with hotels.

        :return: None
        :rtype: NoneType
        """
        all_hotels = FileHandler(self.input_folder, self.output_folder).hotels_df
        self._count_hotels_in_cities(all_hotels)
        self._build_cities_with_most_hotels(all_hotels)
        self._create_output_folders()
        self._fetch_city_weather()
        self._find_cities_and_dates_with_top_temp_values()
        self._create_temp_charts()
        self.create_csv_files()

    def _count_hotels_in_cities(self, all_hotels: pd.DataFrame) -> None:
        """Counts hotels in each city and save data to the attribute _hotels_counter as
        a default dictionary with Counters

        :param all_hotels: Dataframe with all hotels
        :type all_hotels: pd.DataFrame
        :return: None
        :rtype: NoneType
        """
        for _, row in all_hotels.iterrows():
            self._hotels_counter[row["Country"]][row["City"]] += 1

    def _build_cities_with_most_hotels(self, all_hotels: pd.DataFrame) -> None:
        """Creates City Dataclass for a city in each country with most number of hotels

        :param all_hotels: Dataframe with all hotels
        :type all_hotels: pd.DataFrame
        :return: None
        :rtype: NoneType
        """
        for country, cities in self._hotels_counter.items():
            city = cities.most_common(1)[0][0]
            self._cities.append(
                City(
                    name=city,
                    country=country,
                    hotels=all_hotels[all_hotels["City"] == city],
                )
            )

    def _fetch_city_weather(self) -> None:
        """Fetches weather with OpenWeather for each city from the attribute cities
        and saves data to these city objects.

        :return: None
        :rtype: NoneType
        """
        weather_list = OpenWeather(
            [(city.latitude, city.longitude) for city in self._cities],
            threads=self.max_workers,
        ).results

        for city, weather in zip(self._cities, weather_list):
            city.weather = weather

    def _create_output_folders(self) -> None:
        """Creates output folder and folders in the output folder for each city with a
        pattern: output_folder / city.country / city.name.

        :return: None
        :rtype: NoneType
        """
        for city in self._cities:
            Path(self.output_folder / city.country / city.name).mkdir(
                parents=True, exist_ok=True
            )

    def _find_cities_and_dates_with_top_temp_values(self) -> None:
        """Creates JSON file with top temperature data which contains following:
        city and date for maximum temperature, city and date for minimum temperature,
        city for a maximum delta between maximum maximum temperature value and minimum
        maximum temperature value, city and date for a day delta between maximum
        temperature value and minimum temperature value.

        :return: None
        :rtype: NoneType
        """
        max_temp = self._find_max_temp()
        min_temp = self._find_min_temp()
        delta_max_temp = self._find_delta_max_temp()
        delta_max_min_temp = self._find_delta_max_min_temp()
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
                "Date": datetime.fromtimestamp(delta_max_min_temp.date).strftime(
                    "%d.%m.%Y"
                ),
            },
        }

        with open(self.output_folder / "analysis.json", mode="w") as fl:
            json.dump(data, fl, ensure_ascii=False, indent=4)

    def _find_max_temp(self) -> TempData:
        """Searches for a city and date with a maximum temperature in the attribute
        cities.

        :return: city, date,with a maximum temperature
        :rtype: TempData
        """
        temp = TempData()

        for city in self._cities:
            city_max_temp = max([day[2] for day in city.weather])
            if city_max_temp > temp.temp:
                temp.temp = city_max_temp
                temp.city = city.name
                temp.date = sorted(city.weather, key=lambda x: x[2])[-1][0]

        return temp

    def _find_min_temp(self) -> TempData:
        """Searches for a city with a minimum temperature in the attribute
        cities.

        :return: city, date with a minimum temperature
        :rtype: TempData
        """
        temp = TempData(temp=float("+inf"))

        for city in self._cities:
            city_min_temp = min([day[1] for day in city.weather])
            if city_min_temp < temp.temp:
                temp.temp = city_min_temp
                temp.city = city.name
                temp.date = sorted(city.weather, key=lambda x: x[1])[0][0]

        return temp

    def _find_delta_max_temp(self) -> TempData:
        """Searches a maximum delta between maximum maximum temperature value and
        minimum maximum temperature value in the attribute cities.

        :return: city with a maximum delta between maximum maximum temperature and
        minimum maximum temperature values.
        :rtype: TempData
        """
        delta_max_temp = TempData()

        for city in self._cities:
            city_delta_max_temp = max([day[2] for day in city.weather]) - min(
                [day[2] for day in city.weather]
            )
            if city_delta_max_temp > delta_max_temp.temp:
                delta_max_temp.temp = city_delta_max_temp
                delta_max_temp.city = city.name

        return delta_max_temp

    def _find_delta_max_min_temp(self) -> TempData:
        """Searches city and date for a maximum delta between maximum temperature value
        and minimum temperature value.

        :return: ity and date for a maximum delta between maximum temperature value
        and minimum temperature value
        :rtype: TempData
        """
        delta_max_min_temp = TempData()

        for city in self._cities:
            city_delta_max_min_temp = max([day[2] - day[1] for day in city.weather])
            if city_delta_max_min_temp > delta_max_min_temp.temp:
                delta_max_min_temp.temp = city_delta_max_min_temp
                delta_max_min_temp.city = city.name
                delta_max_min_temp.date = sorted(
                    city.weather, key=lambda x: x[2] - x[1]
                )[-1][0]

        return delta_max_min_temp

    def _create_temp_charts(self) -> None:
        """Creates charts of the maximum and minimum temperature for each city.

        :return: None
        :rtype: NoneType
        """
        for city in self._cities:
            fig = plt.figure()

            for num in range(1, 3):
                plt.plot(
                    [
                        datetime.fromtimestamp(d[0]).strftime("%d.%m")
                        for d in city.weather
                    ],
                    [d[num] for d in city.weather],
                )

            fig.savefig(self.output_folder / city.country / city.name / "chart.png")

    def create_csv_files(self) -> None:
        """Creates csv_files with hotels for each city. Max hotels is determined by a
        max hotels attribute.

        :return: None
        :rtype: NoneType
        """
        all_hotels = pd.concat(
            [city.hotels[: self.max_hotels] for city in self._cities]
        )
        addresses = PickPoint(
            [(row["Latitude"], row["Longitude"]) for _, row in all_hotels.iterrows()],
            self.max_workers,
        ).results
        all_hotels["Address"] = addresses
        for city in self._cities:
            city_df = all_hotels[
                (all_hotels["Country"] == city.country)
                & (all_hotels["City"] == city.name)
            ][["Name", "Address", "Latitude", "Longitude"]]
            city_df.to_csv(
                self.output_folder / city.country / city.name / "Hotels.csv",
                index=False,
            )
