from resource import Resource

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


