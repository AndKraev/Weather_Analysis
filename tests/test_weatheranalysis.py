from collections import defaultdict, Counter
from copy import deepcopy
from unittest.mock import patch, Mock

import pandas as pd
import pytest

from WeatherAnalysis import City, TempData, WeatherAnalysis


@pytest.fixture
def analyser(tmp_path):
    tmp = tmp_path
    return WeatherAnalysis(tmp)


@pytest.fixture
def hotels():
    return pd.DataFrame({"Country": ["FR", "FR", "FR"],
                         "City": ["Paris", "Marseille", "Paris"],
                         "Latitude": [2, 4, 8],
                         "Longitude": [4, 6, 10]})


@pytest.fixture()
def city(hotels):
    return City(name="Paris", country="FR", hotels=hotels, weather=[1])


def test_dataclass_city(city, hotels):
    assert city.name == "Paris"
    assert city.country == "FR"
    assert city.hotels.equals(hotels)
    assert city.latitude == 5
    assert city.longitude == 7
    assert city.weather == [1]


def test_dataclass_tempdata():
    t = TempData(date=1, city="A")
    assert t.city == "A"
    assert t.date == 1
    assert t.temp == float("-inf")


def test_weatheranalysis_count_hotels_in_cities(analyser, hotels):
    analyser.count_hotels_in_cities(pd.DataFrame(hotels))
    expected = defaultdict(Counter, {'FR': Counter({'Paris': 2, 'Marseille': 1})})
    assert analyser.hotels_counter == expected


def test_weatheranalysis_build_cities_with_most_hotels(analyser, hotels):
    analyser.hotels_counter = defaultdict(Counter, {'FR': Counter({'Paris': 2, 'Marseille': 1})})
    analyser.build_cities_with_most_hotels(hotels)
    expected_city = analyser.cities[0]
    assert len(analyser.cities) == 1
    assert expected_city.name == "Paris"
    assert expected_city.country == "FR"
    assert expected_city.latitude == 5
    assert expected_city.longitude == 7
    assert expected_city.hotels.equals(hotels[hotels["City"] == "Paris"])


def test_weatheranalysis_fetch_city_weather(analyser, city):
    with patch("WeatherAnalysis.OpenWeather") as mocked:
        response = Mock()
        response.results = [2]
        mocked.return_value = response
        city_1 = city
        analyser.cities = [city_1]
        analyser.fetch_city_weather()
        assert analyser.cities[0].weather == 2


def test_weatheranalysis_create_output_folders(analyser, city):
    with patch("WeatherAnalysis.Path") as mocked_path:
        response = Mock()
        mocked_path.return_value = response
        analyser.cities = [city, city]
        analyser.create_output_folders()
        mocked_path.assert_called_with(analyser.output_folder / city.country / city.name)
        assert response.mkdir.call_count == 2


def test_find_max_temp(analyser):
    city1 = City(name="Houston", weather=[(1, 0, 0), (2, 0, 30.5)], country=None,
                 hotels=None, longitude=1, latitude=1)
    city2 = City(name="London", weather=[(1, 0, 0), (2, 0, 0)], country=None,
                 hotels=None, longitude=1, latitude=1)
    analyser.cities = [city1, city2]
    temp = analyser.find_max_temp()
    assert temp.city == "Houston"
    assert temp.date == 2


def test_find_min_temp(analyser):
    city1 = City(name="Houston", weather=[(1, 0, 0), (2, 0, 0)], country=None,
                 hotels=None, longitude=1, latitude=1)
    city2 = City(name="London", weather=[(1, 0, 0), (2, -30, 0)], country=None,
                 hotels=None, longitude=1, latitude=1)
    analyser.cities = [city1, city2]
    temp = analyser.find_min_temp()
    assert temp.city == "London"
    assert temp.date == 2


def test_find_delta_max_temp(analyser):
    city1 = City(name="Houston", weather=[(1, 0, 10), (2, 0, 5)], country=None,
                 hotels=None, longitude=1, latitude=1)
    city2 = City(name="London", weather=[(1, 0, 6), (2, 0, 10)], country=None,
                 hotels=None, longitude=1, latitude=1)
    analyser.cities = [city1, city2]
    temp = analyser.find_delta_max_temp()
    assert temp.city == "Houston"


def test_find_delta_max_min_temp(analyser):
    city1 = City(name="Houston", weather=[(1, 8, 10), (2, 15, 20)], country=None,
                 hotels=None, longitude=1, latitude=1)
    city2 = City(name="London", weather=[(1, -10.5, 10), (2, 17, 19)], country=None,
                 hotels=None, longitude=1, latitude=1)
    analyser.cities = [city1, city2]
    temp = analyser.find_delta_max_min_temp()
    assert temp.city == "London"
    assert temp.date == 1


@patch("WeatherAnalysis.json")
@patch("WeatherAnalysis.open")
@patch("WeatherAnalysis.WeatherAnalysis.find_delta_max_min_temp")
@patch("WeatherAnalysis.WeatherAnalysis.find_delta_max_temp")
@patch("WeatherAnalysis.WeatherAnalysis.find_min_temp")
@patch("WeatherAnalysis.WeatherAnalysis.find_max_temp")
def test_find_cities_and_dates_with_top_temp_values(
        max_temp, min_temp,
        delta_max_temp,
        delta_max_min_temp,
        mocked_open,
        mocked_json,
        analyser):
    max_temp.return_value = TempData(city="A", date=1)
    min_temp.return_value = TempData(city="B", date=2)
    delta_max_temp.return_value = TempData(city="C", date=3)
    delta_max_min_temp.return_value = TempData(city="D", date=4)
    mocked_open.return_value = analyser.input_folder
    data = {
        "Maximum Temperature": {
            "City": "A",
            "Date": "01.01.1970",
        },
        "Minimum Temperature": {
            "City": "B",
            "Date": "01.01.1970",
        },
        "Maximum delta of maximum temperatures": {
            "City": "C",
        },
        "Maximum delta of minimum and maximum temperatures": {
            "City": "D",
            "Date": "01.01.1970",
        },
    }

    analyser.find_cities_and_dates_with_top_temp_values()
    mocked_json.dump.assert_called_once_with(
        data, analyser.input_folder, ensure_ascii=False, indent=4
    )


def test_create_temp_charts(analyser):
    city = City(name="Houston", weather=[(1, 8, 10), (90000, 10, 12)], country="US",
                 hotels=None, longitude=1, latitude=1)
    analyser.cities = [city]
    with patch("WeatherAnalysis.plt") as mocked_plt:
        mocked_fig = Mock()
        mocked_plt.figure.return_value = mocked_fig

        analyser.create_temp_charts()

        mocked_plt.plot.has_calls(
            (["01.01", "02.01"], [8, 10]), (["01.01", "02.01"], [10, 12])
        )
        mocked_fig.savefig.assert_called_once_with(
            analyser.output_folder / "US" / "Houston" / "chart.png"
        )

"""
@pytest.fixture
def weatheranalysis(tmp_path):
    tmp = tmp_path
    return WeatherAnalysis(tmp)


def test_weatheranalysis_find_cities_with_most_hotels(weatheranalysis):
    hotels = {"Country": ["FR", "FR", "FR"], "City": ["Paris", "Marseille", "Paris"]}
    weatheranalysis.hotels_df = pd.DataFrame(hotels)
    assert weatheranalysis.find_cities_with_most_hotels() == {("FR", "Paris"): None}


def test_weatheranalysis_get_most_dfs(weatheranalysis):
    weatheranalysis.most_hotels = {("FR", "Paris"): None}
    weatheranalysis.hotels_df = pd.DataFrame(
        {"Country": ["FR", "FR", "FR"], "City": ["Paris", "Marseille", "Paris"]}
    )
    weatheranalysis.get_most_dfs()
    assert len(weatheranalysis.most_hotels[("FR", "Paris")]) == 2


def test_weatheranalysis_get_city_centers(weatheranalysis):
    test_data = {
        ("FR", "Paris"): pd.DataFrame({"Latitude": [2, 4], "Longitude": [3, 5]})
    }
    expected = {("FR", "Paris"): (3, 4)}
    weatheranalysis.most_hotels = test_data
    assert weatheranalysis.get_city_centers() == expected


def test_weatheranalysis_max_temp_city():
    test_data = [
        (("US", "Houston"), [(1, 0, 0), (2, 0, 30.5)]),
        (("GB", "London"), [(1, 0, 0), (2, 0, 0)]),
    ]
    result = WeatherAnalysis.max_temp_city(test_data)
    expected = "Houston", 2
    assert result == expected


def test_weatheranalysis_min_temp_city():
    test_data = [
        (("US", "Houston"), [(1, 0, 0), (2, 0, 0)]),
        (("GB", "London"), [(1, 0, 0), (2, -30, 0)]),
    ]
    result = WeatherAnalysis.min_temp_city(test_data)
    expected = "London", 2
    assert result == expected


def test_weatheranalysis_delta_max_temp():
    test_data = [
        (("US", "Houston"), [(1, 0, 10), (2, 0, 5)]),
        (("GB", "London"), [(1, 0, 6), (2, 0, 10)]),
    ]
    result = WeatherAnalysis.delta_max_temp(test_data)
    expected = "Houston"
    assert result == expected


def test_weatheranalysis_delta_temp():
    test_data = [
        (("US", "Houston"), [(1, 8, 10), (2, 15, 20)]),
        (("GB", "London"), [(1, -10.5, 10), (2, 17, 19)]),
    ]
    result = WeatherAnalysis.delta_temp(test_data)
    expected = "London", 1
    assert result == expected


def test_weatheranalysis_create_json_wth_analysis(weatheranalysis):
    Path(weatheranalysis.output_path).mkdir(parents=True, exist_ok=True)
    weatheranalysis.city_weather = {("US", "Houston"): [(1, 8, 10), (2, 15, 20)]}
    weatheranalysis.create_json_with_analysis()
    with open(weatheranalysis.output_path / "analysis.json", mode="r") as fl:
        result = json.load(fl)
    expected = {
        "Maximum Temperature": {
            "City": "Houston",
            "Date": "01.01.1970",
        },
        "Minimum Temperature": {
            "City": "Houston",
            "Date": "01.01.1970",
        },
        "Maximum delta of maximum temperatures": {
            "City": "Houston",
        },
        "Maximum delta of minimum and maximum temperatures": {
            "City": "Houston",
            "Date": "01.01.1970",
        },
    }
    assert result == expected


def test_weatheranalysis_create_charts(weatheranalysis):
    weatheranalysis.most_hotels = {("US", "Houston"): None}
    weatheranalysis.city_weather = {("US", "Houston"): [(1, 8, 10), (90000, 10, 12)]}
    with patch("WeatherAnalysis.plt") as mocked_plt:
        mocked_fig = Mock()
        mocked_plt.figure.return_value = mocked_fig
        weatheranalysis.create_charts()
        mocked_plt.plot.has_calls(
            (["01.01", "02.01"], [8, 10]), (["01.01", "02.01"], [10, 12])
        )
        mocked_fig.savefig.assert_called_once_with(
            weatheranalysis.output_path / "US" / "Houston" / "chart.png"
        )


def test_weatheranalysis_create_charts(weatheranalysis):
    Path(weatheranalysis.output_path / "FR" / "Paris").mkdir(
        parents=True, exist_ok=True
    )
    df_1 = pd.DataFrame(
        {
            "Latitude": (2, 4),
            "Longitude": (3, 5),
            "Country": ("FR", "FR"),
            "City": ("Paris", "Paris"),
            "Name": ("Hotel1", "Hotel2"),
        }
    )
    addresses = ["a1", "a2"]
    weatheranalysis.most_hotels = {("FR", "Paris"): df_1}
    with patch("WeatherAnalysis.PickPoint") as mocked:
        mocked_response = Mock()
        mocked_response.results = addresses
        mocked.return_value = mocked_response

        weatheranalysis.hotels_to_csv()

        df_1["Address"] = addresses
        expected = df_1[["Name", "Address", "Latitude", "Longitude"]]
        result = pd.read_csv(
            weatheranalysis.output_path / "FR" / "Paris" / "Hotels.csv"
        )
        assert result.equals(expected)


@patch("WeatherAnalysis.WeatherAnalysis.hotels_to_csv")
@patch("WeatherAnalysis.WeatherAnalysis.create_json_wth_analysis")
@patch("WeatherAnalysis.WeatherAnalysis.create_charts")
@patch("WeatherAnalysis.OpenWeather")
@patch("WeatherAnalysis.WeatherAnalysis.get_city_centers")
@patch("WeatherAnalysis.WeatherAnalysis.get_most_dfs")
@patch("WeatherAnalysis.WeatherAnalysis.find_cities_with_most_hotels")
@patch("WeatherAnalysis.FileHandler")
def test_weatheranalysis_run(
    FileHandler,
    find_cities_with_most_hotels,
    get_most_dfs,
    get_city_centers,
    OpenWeather,
    create_charts,
    create_json_wth_analysis,
    hotels_to_csv,
    weatheranalysis,
):
    weatheranalysis.run()

    FileHandler.assert_called_once()
    find_cities_with_most_hotels.assert_called_once()
    get_most_dfs.assert_called_once()
    get_city_centers.assert_called_once()
    OpenWeather.assert_called_once()
    create_charts.assert_called_once()
    create_json_wth_analysis.assert_called_once()
    hotels_to_csv.assert_called_once()
"""