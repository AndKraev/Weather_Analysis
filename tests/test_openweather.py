import json
from pathlib import Path
from unittest.mock import Mock, patch

import Services
import Setup
from Services import OpenWeather


@patch("time.time")
@patch("Services.OpenWeather.run")
def test_openweather_url_lists(run, time):
    lat, lon = 12.21, 21.12
    # OpenWeather.sort_results = Mock()
    api = Setup.openweather_api
    time.return_value = 0  # Mock(return_value=0)
    expected_urls = [
        f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}"
        f"&lon={lon}&exclude=hourly,minutely,alerts&units=metric&"
        f"appid={api}"
    ]

    expected_urls.extend(
        [
            f"http://api.openweathermap.org/data/2.5/onecall/"
            f"timemachine?lat={lat}&lon={lon}&dt={-86400 * num}"
            f"&units=metric&appid={api}"
            for num in range(1, 6)
        ]
    )

    assert (
        OpenWeather({"city": (lat, lon)}, threads=100).create_api_ulr_list()
        == expected_urls
    )


@patch("Services.OpenWeather.run")
def test_openweather_sort_results(run):
    lat, lon = 12.21, 21.12
    with open(Path("tests/openweather_responses.json"), mode="r") as fl:
        responses = json.load(fl)
    urls = [f"urls_{n}" for n in range(1, 7)]
    results = {url: response for url, response in zip(urls, responses)}
    ow = OpenWeather({"city": (lat, lon)}, threads=100)
    ow.urls_list = urls
    expected = {
        "city": [
            (1630157206, 22.09, 31.41),
            (1630157206, 22.09, 31.41),
            (1630157206, 22.09, 31.41),
            (1630157206, 22.09, 31.41),
            (1630157206, 22.09, 31.41),
            (1630234800, 21.45, 30.84),
            (1630321200, 22.34, 32.62),
            (1630407600, 22.07, 33.04),
            (1630494000, 22.4, 27.07),
            (1630580400, 21.41, 31.32),
            (1630666800, 20.4, 32.47),
        ]
    }
    assert ow.sort_results(results) == expected
