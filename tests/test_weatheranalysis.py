import json
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from WeatherAnalysis import WeatherAnalysis


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
