import json
from unittest import mock

from Services import OpenWeather
import Setup


@mock.patch("Services.AsyncGetAPI")
@mock.patch("time.time")
def test_openweather_url_lists(AsyncGetAPI, time):
    lat, lon = 12.21, 21.12
    OpenWeather.sort_results = mock.Mock()
    api = Setup.openweather_api
    time.return_value = mock.Mock(return_value=0)
    expected_urls = [f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}"
                     f"&lon={lon}&exclude=hourly,minutely,alerts&units=metric&"
                     f"appid={api}"]

    expected_urls.extend([f"http://api.openweathermap.org/data/2.5/onecall/"
                          f"timemachine?lat={lat}&lon={lon}&dt={-86400 * num + 1}"
                          f"&units=metric&appid={api}" for num in range(1, 6)])

    assert OpenWeather(
        {"city": (lat, lon)}, threads=100
    ).create_api_ulr_list() == expected_urls


@mock.patch("Services.AsyncGetAPI")
def test_openweather_sort_results(AsyncGetAPI):
    lat, lon = 12.21, 21.12
    with open("openweather_responses.json", mode="r") as fl:
        responses = json.load(fl)
    urls = list(range(6))

    with mock.patch(Services.Openweather.res)
    OpenWeather.urls_list = urls
    OpenWeather.results = []

    OpenWeather({"city": (lat, lon)}, threads=100)

