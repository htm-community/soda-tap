import datetime
import requests

DATE_FORMATS = [
  "%Y-%m-%dT%H:%M:%S.%f",
  "%Y-%m-%dT%H:%M:%S",
  "%Y-%m-%d",
  "%m/%d/%y"
]
TOO_OLD_DAYS = 180
TOO_SLOW_INTERVAL_DAYS = 7


class ResourceError(Exception):
    pass


class Resource:
  
  def __init__(self, json):
    self._json = json
    self._temporalFieldNames = []
    self._samplePoint = None
    self._fieldMapping = None
    self._temporalFieldNames = None
    self._temporalIndex = None
    self._meanTimeDelta = None

  def _getSampleDataPoint(self):
    if self._samplePoint is not None:
      return self._samplePoint
    # Taking the 2nd record in case the 1st one is dirty (it happens).
    try:
      return self.fetchData(limit=2)[1]
    except IndexError:
      raise ResourceError("No data!")
    except KeyError as e:
      raise ResourceError("Error fetching first data point: " + str(e))


  def _isNonNumericalNumberString(self, key):
    blacklist = [
      "zip", "address", "incidentid", "offers_", "recordid", "rowid",
      "number", "code", "year", "month", "meter_id", "bldgid",
      "parcel_no", "case", "_no", "uniquekey", "district",
      "_id", "_key", "checknum", "_group", "crimeid", "facility",
      "phone", "licensenum", "_status", "fileno", "cnty_cd", "day",
      "extra_multiplier", "nc_pin", "facid"
    ]
    for word in blacklist:
      if word in key:
        return True
    return False


  def _stringToDate(self, str):
    for formatString in DATE_FORMATS:
      try:
        return datetime.datetime.strptime(str, formatString)
      except ValueError:
        pass
    raise ValueError(
      "Date string " + str + " does not conform to expected date formats: " 
      + ", ".join(DATE_FORMATS)
    )

  
  def _getDataType(self, key, value):
    if self._isNonNumericalNumberString(key):
      dataType = "str"
    elif isinstance(value, dict):
      dictKeys = value.keys()
      if ("type" in dictKeys and value["type"] == "Point") \
      or ("latitude" in dictKeys and "longitude" in dictKeys):
        dataType = "location"
      else:
        dataType = "dict"
    elif isinstance(value, list):
      dataType = "list"
    else:
      try:
        int(value)
        dataType = "int"
      except ValueError:
        try:
          float(value)
          dataType = "float"
        except ValueError:
          try:
            self._stringToDate(value)
            dataType = "date"
          except ValueError:
            dataType = "str"
    return dataType


  def _extractFieldInfo(self):
    self._fieldMapping = {}
    for key, val in self._getSampleDataPoint().iteritems():
      self._fieldMapping[key] = self._getDataType(key, val)


  def _calculateMeanTimeDelta(self, data=None):
    if self._meanTimeDelta is not None:
      return self._meanTimeDelta
    if data is None:
      data = self.fetchData(limit=100)
    temporalIndex = self.getTemporalIndex()
    deltas = []
    lastDate = None
    for point in data:
      d = self._stringToDate(point[temporalIndex])
      if lastDate is None:
        lastDate = d
        continue
      diff = d - lastDate
      deltas.append(diff)
      lastDate = d
    meanTimeDelta = sum(deltas, datetime.timedelta(0)) / len(deltas)
    if meanTimeDelta.days > TOO_SLOW_INTERVAL_DAYS:
      raise ResourceError(
        "Time delta between points is too high: " + str(meanTimeDelta)
      )
    self._meanTimeDelta = meanTimeDelta
    return self._meanTimeDelta


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


  def getDomain(self):
    return self.getMetadata()["domain"]


  def getName(self):
    return self.getResource()["name"]


  def getId(self):
    return self.getResource()["id"]


  def getJsonUrl(self):
    return "https://" + self.getMetadata()["domain"] \
           + "/resource/" + self.getResource()["id"] + ".json"


  def getMeanTimeDelta(self):
    return self._meanTimeDelta


  def getLocationField(self):
    for k, v in self.getFieldMapping().iteritems():
      if v == "location":
        return k
    return None


  def getStreamType(self):
    if "location" in self.getFieldTypes():
      return "geospatial"
    else:
      return "spatial"


  def getFieldMapping(self):
    if self._fieldMapping is None:
      self._extractFieldInfo()
    return self._fieldMapping


  def getFieldNames(self):
    return self.getFieldMapping().keys()


  def getFieldTypes(self):
    return self.getFieldMapping().values()


  def getTemporalFields(self):
    if self._temporalFieldNames is not None:
      return self._temporalFieldNames
    temporalFieldNames = []
    point = self._getSampleDataPoint()
    # find the temporal field names
    for key, value in point.iteritems():
      if isinstance(value, basestring):
        try:
          self._stringToDate(value)
          # If coercing into a date worked, then it is a date.
          temporalFieldNames.append(key)
        except ValueError:
          # Ignore errors from attempted date coercion.
          pass
    self._temporalFieldNames = temporalFieldNames
    return self._temporalFieldNames 


  def getTemporalIndex(self):
    if self._temporalIndex is not None:
      return self._temporalIndex
    temporalFieldNames = self.getTemporalFields()
    if len(temporalFieldNames) == 0:
      raise ResourceError(
        "Resource " + self.getId() + " has no temporal fields."
      )
    name = temporalFieldNames[0]
    for n in temporalFieldNames:
      lowName = n.lower()
      if "created" in lowName or \
         "open" in lowName or \
         "received" in lowName or \
         "effective" in lowName or \
         "publication" in lowName or \
         "asof" in lowName or \
         "start" in lowName:
        name = n
    self._temporalIndex = name
    return self._temporalIndex


  # # WIP
  # def getFieldRanking(self, data):
  #   counts = {}
  #   for point in data:
  #     for k, v in point.iteritems():
  #       if v is None or v == "":
  #         if k not in counts.keys():
  #           counts[k] = 1
  #         else:
  #           counts[k] += 1
  #   # print counts


  def validate(self):
    name = self.getName()
    data = self.fetchData(limit=100)
    allTypes = self.getFieldTypes()
    temporalIndex = self.getTemporalIndex()
    
    # State lottery is pretty useless from what I have seen.
    if "lottery" in name.lower() or "lotto" in name.lower():
      raise ResourceError("Lottery streams suck.")
    
    # Not temporal if there are less than 100 data points.
    if len(data) < 100:
      raise ResourceError("Not enough data to analyze.")
    
    # Not temporal if there are no ints or floats or locations involved.
    if "int" not in allTypes \
        and "float" not in allTypes \
        and "location" not in allTypes:
      raise ResourceError("No scalars or locations found.")
    
    # If any points are missing a temporal field, not temporal.
    for point in data:
      if temporalIndex not in point.keys():
        raise ResourceError("Some points are missing temporal field values.")
    
    # If the first and last points have the same date, not temporal.
    firstDate = data[0][temporalIndex]
    lastDate = data[len(data) - 1][temporalIndex]
    if firstDate == lastDate:
      raise ResourceError("No temporal movement over data.")
    
    # If latest data is old, not temporal.
    today = datetime.datetime.today()
    try:
      lastDate = self._stringToDate(lastDate)
    except ValueError as e:
      raise ResourceError(
        "Last known data point has wrong date format: " + str(e)
      )
    sixMonthsAgo = today - datetime.timedelta(days=TOO_OLD_DAYS)
    if lastDate < sixMonthsAgo:
      raise ResourceError("Data is over " + str(TOO_OLD_DAYS) + " days old.")
    
    # If data is way in the future, that ain't right.
    if lastDate > (today + datetime.timedelta(days=7)):
      raise ResourceError("Data is in the future!")
  
    # If the average distance between points is too large, not temporal.
    return self._calculateMeanTimeDelta(data)


  def fetchData(self, limit=5000):
    url = self.getJsonUrl() + "?$limit=" + str(limit)
    order = None
    
    # Order by temporalIndex if we have one
    if self._temporalIndex is not None:
      order = "&$order=" + self.getTemporalIndex() + " DESC"
    
    if order is not None:
      url += order

    try:
      response = requests.get(url, timeout=5)
      if response.status_code is not 200:
        raise ResourceError("HTTP request error: " + response.text)
    except requests.exceptions.ConnectionError:
      raise ResourceError("HTTP Connection error on " + url + ".")
    except requests.exceptions.Timeout:
      raise ResourceError("H  TTP Connection timeout on " + url + ".")
    
    data = response.json()
    
    # If the order by temporal index was applied, the data is DESC, so reverse.
    if order is not None:
      data = list(reversed(data))
    return data


  def json(self):
    return self._json
  
  def __str__(self):
    try:
      return str(self.getLink())
    except UnicodeEncodeError:
      return "Cannot convert unicode to ASCII"
