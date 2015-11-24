import requests

URL = "http://api.us.socrata.com/api/catalog/"
VERSION = "v1"
FILTER = "only=datasets"

DEFAULT_OFFSET = 0
DEFAULT_LIMIT = 100


# Entry point to the whole thing.
def createCatalog():
  return Catalog()



class Catalog:

  
  def __init__(self):
    self._offset = DEFAULT_OFFSET
    self._limit = DEFAULT_LIMIT

  
  def __iter__(self):
    return self


  def __getitem__(self, i):
    itemsPerPage = self._limit
    startOffset = itemsPerPage * i
    response = requests.get(
      URL + VERSION
      + "?" + FILTER
      + "&offset=" + str(startOffset)
    )
    print response.request.url
    data = response.json()
    page = Page(data)
    return page


  def next(self):
    self._offset += self._limit
    # print "fetching more... [offset %s]" % self._offset
    response = requests.get(
      URL + VERSION
      + "?" + FILTER
      + "&offset=" + str(self._offset)
    )
    print response.request.url
    data = response.json()
    page = Page(data)
    return page


class Page:
  
  def __init__(self, data):
    self._data = data
    self._results = data["results"]
    self._resultSetSize = data["resultSetSize"]
    self._counter = 0

  
  def __iter__(self):
    return self


  def __getitem__(self, i):
    return Resource(self._results[i])

  
  def __len__(self):
    return len(self._results)


  def next(self):
    try:
      resource = Resource(self._results[self._counter])
      self._counter += 1
    except IndexError:
      self._counter = 0
      raise StopIteration
    return resource


class Resource:
  
  def __init__(self, json):
    self._json = json
    self._temporalFieldNames = []

  def getLink(self):
    return self._json["link"]
  
  def getPermalink(self):
    return self._json["permalink"]
  
  def getResource(self):
    return self._json["resource"]

  def getClassification(self):
    return self._json["classification"]

  def getMetadata(self):
    return self._json["metadata"]
  
  def getName(self):
    return self.getResource()["name"]
    
  def getId(self):
    return self.getResource()["id"]
    
  def getJsonUrl(self):
    return "https://" + self.getMetadata()["domain"] \
           + "/resource/" + self.getResource()["id"] + ".json"
  

  def fetchData(self, limit=5000, order=None):
    url = self.getJsonUrl() + "?$limit=" + str(limit)
    if order is not None:
      url += "&$order=" + order
    try:
      response = requests.get(url)
    except requests.exceptions.ConnectionError:
      return []
    return response.json()
    
  
  def json(self):
    return self._json
  
  def __str__(self):
    try:
      return str(self.getLink())
    except UnicodeEncodeError:
      return "Cannot convert unicode to ASCII"