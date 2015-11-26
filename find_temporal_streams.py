import datetime
import json
import os
import sys
import urlparse

import redis
from termcolor import colored

from sodatap import createCatalog

DATE_FORMATS = [
  "%Y-%m-%dT%H:%M:%S.%f",
  "%Y-%m-%dT%H:%M:%S",
  "%Y-%m-%d",
  "%m/%d/%y"
]
REDIS_URL = os.environ["REDIS_URL"]
REDIS_DB = 0
POOL = None

TOO_OLD_DAYS = 180
TOO_SLOW_INTERVAL_DAYS = 7


def stringToDate(str):
  for formatString in DATE_FORMATS:
    try:
      return datetime.datetime.strptime(str, formatString)
    except ValueError:
      pass
  raise ValueError(
    "Date string " + str + " does not conform to expected date formats: " 
    + ", ".join(DATE_FORMATS)
  )


def getTemporalFields(point):
  temporalFieldNames = []
  # find the temporal field names
  for key, value in point.iteritems():
    if isinstance(value, basestring):
      try:
        stringToDate(value)
        # If coercing into a date worked, then it is a date.
        temporalFieldNames.append(key)
      except ValueError:
        # Ignore errors from attempted date coercion.
        pass
  return temporalFieldNames


def getPrimaryTemporalField(temporalFieldNames):
  if len(temporalFieldNames) == 0:
    return None
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
  return name


def isNonNumericalNumberString(key):
  blacklist = [
    "zip", "address", "latitude", "longitude", "incidentid",
    "number", "code", "year", "month", "meter_id", "bldgid",
    "parcel_no", "case", "_no", "uniquekey", "district",
    "_id", "_key", "checknum", "_group", "crimeid", "facility",
    "phone", "licensenum", "_status", "fileno", "cnty_cd", "day",
    "extra_multiplier", "nc_pin"
  ]
  for word in blacklist:
    if word in key:
      return True
  return False


def getDataType(key, value):
  if isNonNumericalNumberString(key):
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
          stringToDate(value)
          dataType = "date"
        except ValueError:
          dataType = "str"
  return dataType


def extractFieldTypesFrom(dataPoint):
  fieldMeta = {}
  for key, val in dataPoint.iteritems():
    fieldMeta[key] = getDataType(key, val)
  return fieldMeta


def storeResource(redisClient, resource, field, fieldTypes, meanTimeDelta):
  id = resource.getId()
  type = "scalar"
  fieldNames = fieldTypes.values()
  if "location" in fieldNames \
          or ("latitude" in fieldNames 
              and "longitude" in fieldNames):
    type = "geospatial"
  redisClient.set(id, json.dumps({
    "type": type,
    "temporalField": field,
    "jsonUrl": resource.getJsonUrl(),
    "fieldTypes": fieldTypes,
    "meanTimeDelta": str(meanTimeDelta),
    "catalogEntry": resource.json()
  }))
  redisClient.sadd(type, id)


# WIP
def getFieldRanking(data):
  counts = {}
  for point in data:
    for k, v in point.iteritems():
      if v is None or v == "":
        if k not in counts.keys():
          counts[k] = 1
        else:
          counts[k] += 1
  # print counts


def calculateMeanTemporalDistance(data, temporalField):
  deltas = []
  lastDate = None
  for point in data:
    d = stringToDate(point[temporalField])
    if lastDate is None:
      lastDate = d
      continue
    # if lastDate == d:
    #   raise ValueError("Multiple data points with same timestamp.")
    diff = d - lastDate
    deltas.append(diff)
    lastDate = d
  meanTimeDelta = sum(deltas, datetime.timedelta(0)) / len(deltas)
  if meanTimeDelta.days > TOO_SLOW_INTERVAL_DAYS:
    raise ValueError("Time delta between points is too high: " + str(meanTimeDelta))
  return meanTimeDelta


def validateTemporal(name, temporalField, data, fieldTypes):
  # State lottery is pretty useless from what I have seen.
  if "lottery" in name.lower() or "lotto" in name.lower():
    raise ValueError("Lottery stream.")
  
  # Not temporal if there are less than 100 data points.
  if len(data) < 100:
    raise ValueError("Not enough data to analyze.")
  
  # Not temporal if there are no ints or floats or locations involved.
  allTypes = fieldTypes.values()
  if "int" not in allTypes \
      and "float" not in allTypes \
      and "location" not in allTypes:
    raise ValueError("No scalars or locations found.")
  
  # If any points are missing a temporal field, not temporal.
  for point in data:
    if temporalField not in point.keys():
      raise ValueError("Some points are missing temporal field values.")
  
  # If the first and last points have the same date, not temporal.
  firstDate = data[0][temporalField]
  lastDate = data[len(data) - 1][temporalField]
  if firstDate == lastDate:
    raise ValueError("No temporal movement over data.")
  
  # If latest data is old, not temporal.
  today = datetime.datetime.today()
  lastDate = stringToDate(lastDate)
  sixMonthsAgo = today - datetime.timedelta(days=TOO_OLD_DAYS)
  if lastDate < sixMonthsAgo:
    raise ValueError("Data is over a " + str(TOO_OLD_DAYS) + " days old.")
  
  # If data is way in the future, that ain't right.
  if lastDate > (today + datetime.timedelta(days=7)):
    raise ValueError("Data is in the future!")

  # If the average distance between points is too large, not temporal.
  return calculateMeanTemporalDistance(data, temporalField)


def processResource(redisClient, resource, stored):
  id = resource.getId()
  name = resource.getName()
  link = resource.getPermalink()
  domain = resource.getMetadata()["domain"]
  try:
    if id in stored:
      raise ValueError("Already stored.")
    # Need to get one data point to calculate the primary temporal field.
    try:
      dataPoint = resource.fetchData(limit=100)[1]
    except IndexError:
      raise ValueError("No data!")
    except KeyError as e:
      raise ValueError("Error fetching first data point: " + str(e))
    temporalFieldNames = getTemporalFields(dataPoint)
    primaryTemporalField = getPrimaryTemporalField(temporalFieldNames)
    # Not temporal if there's no temporal field identified.
    if primaryTemporalField is None or primaryTemporalField == "":
      raise ValueError("No temporal field found.")
    fieldTypes = extractFieldTypesFrom(dataPoint)
    # Need to get the rest of the data ordered by the temopral field for
    # further analysis.
    try:
      data = list(reversed(resource.fetchData(
        limit=100, order=primaryTemporalField + " DESC"
      )))
    except TypeError as e:
      raise ValueError("Error fetching sample data: " + str(e))
    # If this is a not temporal stream, the function below will raise a
    # ValueError
    meanTemporalDistance = validateTemporal(
      name, primaryTemporalField, data, fieldTypes
    )
    # TODO: rank fields?
    # fieldRanking = getFieldRanking(data)
    storeResource(
      redisClient, resource, primaryTemporalField,
      fieldTypes, meanTemporalDistance
    )
    print colored(
      "  Stored %s (%s %s) by %s" % (name, id, domain, primaryTemporalField),
      "green"
    )

  except ValueError as e:
    print colored(
      "  %s [%s] | " % (name, link),
      "yellow"
    ) + " " + colored(str(e), "magenta")


def run(offset=0):
  redisUrl = urlparse.urlparse(REDIS_URL)
  redisClient = redis.Redis(
    host=redisUrl.hostname, port=redisUrl.port, 
    db=REDIS_DB, password=redisUrl.password
  )
  stored = redisClient.keys("*")
  count = offset
  catalog = createCatalog(offset=offset)

  for page in catalog:
    for resource in page:
      
      processResource(redisClient, resource, stored)
      
      count += 1
      if count % 10 == 0:
        keyCount = len(redisClient.keys("*"))
        # Adjust count for the two other keys used in Redis.
        amtStored = count
        if count > 0:
          amtStored -= 2
        percStored = float(keyCount) / float(amtStored)
        print colored(
          "Inspected %i streams, stored %i temporal streams (%f)."
          % (amtStored, keyCount, percStored),
          "cyan"
        )



if __name__ == "__main__":
  offset = 0
  if len(sys.argv) > 1:
    offset = int(sys.argv[1])
  run(offset)