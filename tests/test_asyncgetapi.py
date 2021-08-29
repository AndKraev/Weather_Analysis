from Services import AsyncGetAPI


class MockResponse:
    def __init__(self, url, status):
        self.url = url
        self.status = status

    async def json(self):
        return "Text of " + self.url

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


def mocked_response(*args, **kwargs):
    return MockResponse(args[1], 200)


def test_asyncgetapi_result(monkeypatch):
    monkeypatch.setattr("aiohttp.ClientSession.get", mocked_response)
    urls = ["url1", "url2", "url3"]
    expected = {'url1': 'Text of url1', 'url2': 'Text of url2', 'url3': 'Text of url3'}
    assert AsyncGetAPI(urls, 100).results == expected
