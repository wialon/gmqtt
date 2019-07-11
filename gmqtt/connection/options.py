import urllib.parse


class ConnectionOption:
    __slots__ = ('_raw_url', '_parsed_url', '_ssl')

    def __init__(self, url, ssl):
        self._raw_url = url
        self._parsed_url = urllib.parse.urlparse(url)
        self._ssl = ssl

    @property
    def url(self):
        return self._raw_url

    @property
    def scheme(self):
        return self._parsed_url.scheme

    @property
    def host(self):
        return self._parsed_url.hostname

    @property
    def port(self):
        return self._parsed_url.port

    @property
    def ssl(self):
        return self._ssl
