import requests

from page import Page
from resource import ResourceError

URL = "http://api.us.socrata.com/api/catalog/"
VERSION = "v1"
FILTER = "only=datasets"

DEFAULT_OFFSET = 0
DEFAULT_LIMIT = 100


# Entry point to the whole thing.
def createCatalog(offset=DEFAULT_OFFSET):
  return Catalog(offset=offset)


class Catalog:


  def __init__(self, offset=DEFAULT_OFFSET):
    self._offset = offset
    self._limit = DEFAULT_LIMIT


  def __iter__(self):
    return self


  def __getitem__(self, i):
    itemsPerPage = self._limit
    startOffset = itemsPerPage * i
    data = self._fetchData(startOffset)
    page = Page(data)
    return page


  def next(self):
    data = self._fetchData(self._offset)
    page = Page(data)
    self._offset += self._limit
    return page


  def getTotalSodaResourceCount(self):
    data = self._fetchData(0, 1, filter=False)
    return data["resultSetSize"]


  def _fetchData(self, offset, limit=100, filter=True):
    url = URL + VERSION + "?offset=" + str(offset) + "&limit=" + str(limit)
    if filter:
      url += "&" + FILTER

    try:
      response = requests.get(url, timeout=5)
      if response.status_code is not 200:
        raise ResourceError("HTTP request error: " + response.text)
    except requests.exceptions.ConnectionError as e:
      print e
      raise ResourceError("HTTP Connection error on " + url + ".")
    except requests.exceptions.Timeout:
      raise ResourceError("HTTP Connection timeout on " + url + ".")

    return response.json()
