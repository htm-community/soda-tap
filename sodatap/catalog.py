import requests

from page import Page

URL = "http://api.us.socrata.com/api/catalog/"
VERSION = "v1"
FILTER = "only=datasets"

DEFAULT_OFFSET = 0
DEFAULT_LIMIT = 100


# Entry point to the whole thing.
def createCatalog(offset=DEFAULT_OFFSET):
  return Catalog(offset=offset)


def fetch(url):
  try:
    r = requests.get(url)
    if r.status_code != 200:
      print r.json()
      dataOut = []
    else:
      # print r.request.url
      dataOut = r.json()
  except requests.exceptions.ConnectionError as e:
    print url + "  " + str(e)
    dataOut = []
  return dataOut


def fetchCatalogData(offset, limit=100, filter=True):
  url = URL + VERSION + "?offset=" + str(offset) + "&limit=" + str(limit)
  if filter:
    url += "&" + FILTER
  return fetch(url)


class Catalog:


  def __init__(self, offset=DEFAULT_OFFSET):
    self._offset = offset
    self._limit = DEFAULT_LIMIT

  
  def __iter__(self):
    return self


  def __getitem__(self, i):
    itemsPerPage = self._limit
    startOffset = itemsPerPage * i
    data = fetchCatalogData(startOffset)
    page = Page(data)
    return page


  def next(self):
    data = fetchCatalogData(self._offset)
    page = Page(data)
    self._offset += self._limit
    return page

  
  def getTotalSodaResourceCount(self):
    data = fetchCatalogData(0, 1, filter=False)
    return data["resultSetSize"]
