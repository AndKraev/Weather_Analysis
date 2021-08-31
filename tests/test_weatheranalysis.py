from collections import Counter, defaultdict
from copy import deepcopy
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from WeatherAnalysis import City, TempData, WeatherAnalysis


@pytest.fixture
def analyser(tmp_path):
    tmp = tmp_path
    return WeatherAnalysis(tmp)


@pytest.fixture
def hotels():
    return pd.DataFrame(
        {
            "Country": ["FR", "FR", "FR"],
            "City": ["Paris", "Marseille", "Paris"],
            "Latitude": [2, 4, 8],
            "Longitude": [4, 6, 10],
        }
    )


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
    analyser._count_hotels_in_cities(pd.DataFrame(hotels))
    expected = defaultdict(Counter, {"FR": Counter({"Paris": 2, "Marseille": 1})})
    assert analyser._hotels_counter == expected


def test_weatheranalysis_build_cities_with_most_hotels(analyser, hotels):
    analyser._hotels_counter = defaultdict(
        Counter, {"FR": Counter({"Paris": 2, "Marseille": 1})}
    )
    analyser._build_cities_with_most_hotels(hotels)
    expected_city = analyser._cities[0]
    assert len(analyser._cities) == 1
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
        analyser._cities = [city_1]
        analyser._fetch_city_weather()
        assert analyser._cities[0].weather == 2


def test_weatheranalysis_create_output_folders(analyser, city):
    with patch("WeatherAnalysis.Path") as mocked_path:
        response = Mock()
        mocked_path.return_value = response
        analyser._cities = [city, city]
        analyser._create_output_folders()
        mocked_path.assert_called_with(
            analyser.output_folder / city.country / city.name
        )
        assert response.mkdir.call_count == 2


def test_weatheranalysis_find_max_temp(analyser):
    city1 = City(
        name="Houston",
        weather=[(1, 0, 0), (2, 0, 30.5)],
        country=None,
        hotels=None,
        longitude=1,
        latitude=1,
    )
    city2 = City(
        name="London",
        weather=[(1, 0, 0), (2, 0, 0)],
        country=None,
        hotels=None,
        longitude=1,
        latitude=1,
    )
    analyser._cities = [city1, city2]
    temp = analyser._find_max_temp()
    assert temp.city == "Houston"
    assert temp.date == 2


def test_weatheranalysis_find_min_temp(analyser):
    city1 = City(
        name="Houston",
        weather=[(1, 0, 0), (2, 0, 0)],
        country=None,
        hotels=None,
        longitude=1,
        latitude=1,
    )
    city2 = City(
        name="London",
        weather=[(1, 0, 0), (2, -30, 0)],
        country=None,
        hotels=None,
        longitude=1,
        latitude=1,
    )
    analyser._cities = [city1, city2]
    temp = analyser._find_min_temp()
    assert temp.city == "London"
    assert temp.date == 2


def test_weatheranalysis_find_delta_max_temp(analyser):
    city1 = City(
        name="Houston",
        weather=[(1, 0, 10), (2, 0, 5)],
        country=None,
        hotels=None,
        longitude=1,
        latitude=1,
    )
    city2 = City(
        name="London",
        weather=[(1, 0, 6), (2, 0, 10)],
        country=None,
        hotels=None,
        longitude=1,
        latitude=1,
    )
    analyser._cities = [city1, city2]
    temp = analyser._find_delta_max_temp()
    assert temp.city == "Houston"


def test_weatheranalysis_find_delta_max_min_temp(analyser):
    city1 = City(
        name="Houston",
        weather=[(1, 8, 10), (2, 15, 20)],
        country=None,
        hotels=None,
        longitude=1,
        latitude=1,
    )
    city2 = City(
        name="London",
        weather=[(1, -10.5, 10), (2, 17, 19)],
        country=None,
        hotels=None,
        longitude=1,
        latitude=1,
    )
    analyser._cities = [city1, city2]
    temp = analyser._find_delta_max_min_temp()
    assert temp.city == "London"
    assert temp.date == 1


@patch("WeatherAnalysis.json")
@patch("WeatherAnalysis.open")
@patch("WeatherAnalysis.WeatherAnalysis.find_delta_max_min_temp")
@patch("WeatherAnalysis.WeatherAnalysis.find_delta_max_temp")
@patch("WeatherAnalysis.WeatherAnalysis.find_min_temp")
@patch("WeatherAnalysis.WeatherAnalysis.find_max_temp")
def test_weatheranalysis_find_cities_and_dates_with_top_temp_values(
    max_temp,
    min_temp,
    delta_max_temp,
    delta_max_min_temp,
    mocked_open,
    mocked_json,
    analyser,
):
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

    analyser._find_cities_and_dates_with_top_temp_values()
    mocked_json.dump.assert_called_once_with(
        data, analyser.input_folder, ensure_ascii=False, indent=4
    )


def test_weatheranalysis_create_temp_charts(analyser):
    city = City(
        name="Houston",
        weather=[(1, 8, 10), (90000, 10, 12)],
        country="US",
        hotels=None,
        longitude=1,
        latitude=1,
    )
    analyser._cities = [city]
    with patch("WeatherAnalysis.plt") as mocked_plt:
        mocked_fig = Mock()
        mocked_plt.figure.return_value = mocked_fig

        analyser._create_temp_charts()

        mocked_plt.plot.has_calls(
            (["01.01", "02.01"], [8, 10]), (["01.01", "02.01"], [10, 12])
        )
        mocked_fig.savefig.assert_called_once_with(
            analyser.output_folder / "US" / "Houston" / "chart.png"
        )


@patch("WeatherAnalysis.PickPoint")
def test_weatheranalysis_create_csv_files(mocked_pickpoint, analyser):
    df = pd.DataFrame(
        {
            "Latitude": (2, 4),
            "Longitude": (3, 5),
            "Country": ("FR", "FR"),
            "City": ("Paris", "Paris"),
            "Name": ("Hotel1", "Hotel2"),
        }
    )
    addresses = ["a1", "a2"]
    city = City(
        name="Paris",
        weather=[(1, 8, 10), (90000, 10, 12)],
        country="FR",
        hotels=df,
        longitude=1,
        latitude=1,
    )
    analyser._cities = [city]
    mocked_pickpoint.results = addresses
    mocked_pickpoint.return_value = mocked_pickpoint
    path = analyser.output_folder / city.country / city.name
    path.mkdir(parents=True)
    df["Address"] = addresses
    expected = df[["Name", "Address", "Latitude", "Longitude"]]

    analyser.create_csv_files()
    result = pd.read_csv(path / "Hotels.csv")
    assert result.equals(expected)


def test_weatheranalysis_create_output_folders(analyser):
    city = City(
        name="Paris",
        weather=[(1, 8, 10), (90000, 10, 12)],
        country="FR",
        hotels=None,
        longitude=1,
        latitude=1,
    )
    analyser._cities = [city]
    expected_path = analyser.output_folder / city.country / city.name
    analyser._create_output_folders()
    assert expected_path.exists()


@patch("WeatherAnalysis.WeatherAnalysis.create_csv_files")
@patch("WeatherAnalysis.WeatherAnalysis.create_temp_charts")
@patch("WeatherAnalysis.WeatherAnalysis.find_cities_and_dates_with_top_temp_values")
@patch("WeatherAnalysis.WeatherAnalysis.fetch_city_weather")
@patch("WeatherAnalysis.WeatherAnalysis.create_output_folders")
@patch("WeatherAnalysis.WeatherAnalysis.build_cities_with_most_hotels")
@patch("WeatherAnalysis.WeatherAnalysis.count_hotels_in_cities")
@patch("WeatherAnalysis.FileHandler")
def test_weatheranalysis_run(
    filehandler,
    count_hotels_in_cities,
    build_cities_with_most_hotels,
    create_output_folders,
    fetch_city_weather,
    find_cities_and_dates_with_top_temp_values,
    create_temp_charts,
    create_csv_files,
    analyser,
):
    filehandler.return_value = filehandler
    filehandler.hotels_df = "test"

    analyser.run()

    filehandler.assert_called_once_with(analyser.input_folder, analyser.output_folder)
    count_hotels_in_cities.assert_called_once_with("test")
    build_cities_with_most_hotels.assert_called_once_with("test")
    create_output_folders.assert_called_once()
    fetch_city_weather.assert_called_once()
    find_cities_and_dates_with_top_temp_values.assert_called_once()
    create_temp_charts.assert_called_once()
    create_csv_files.assert_called_once()
