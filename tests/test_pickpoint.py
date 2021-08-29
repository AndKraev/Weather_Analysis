from unittest import mock

from Services import PickPoint


@mock.patch("Services.AsyncGetAPI")
def test_pickpoint_url_lists(AsyncGetAPI):
    lat, lon = 12.21, 21.12
    expected = [f'https://api.pickpoint.io/v1/reverse/?key=1x4kgLEfAM7w-6ubGhse&'
                f'lat={lat}&lon={lon}&accept-language=en-US']
    assert PickPoint([(lat, lon)], threads=100).create_api_ulr_list() == expected


@mock.patch("Services.AsyncGetAPI")
def test_pickpoint_sort_results(AsyncGetAPI):
    pp = PickPoint([(1, 2)], threads=100)
    pp.urls_list = ["1", "2"]
    result = pp.sort_results(
        {"2": {"display_name": "data2"}, "1": {"display_name": "data1"}}
    )
    expected = ["data1", "data2"]
    assert result == expected

