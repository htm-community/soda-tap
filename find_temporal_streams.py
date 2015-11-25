import datetime
import json

import redis
from termcolor import colored

from sodatap import createCatalog

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def isDateString(str):
  try:
    datetime.datetime.strptime(str, DATE_FORMAT)
    return True
  except ValueError:
    return False


def getTemporalFields(point):
  temporalFieldNames = []
  # find the temporal field names
  for key, value in point.iteritems():
    if isinstance(value, basestring) and isDateString(value):
      temporalFieldNames.append(key)
  return temporalFieldNames


def getPrimaryTemporalField(temporalFieldNames):
  if len(temporalFieldNames) == 0:
    return None
  name = temporalFieldNames[0]
  for n in temporalFieldNames:
    if "created" in n.lower() or \
       "open" in n.lower() or \
       "received" in n.lower():
      name = n
  return name


def isNonNumericalNumberString(key):
  blacklist = [
    "zip", "address", "latitude", "longitude", "incidentid",
    "number", "code", "year", "month", "meter_id", "bldgid",
    "parcel_no", "case", "_no", "uniquekey", "district",
    "_id", "_key", "checknum", "_group", "crimeid", "facility",
    "phone", "licensenum", "_status", "fileno"
  ]
  for word in blacklist:
    if word in key:
      return True
  return False


def getDataType(key, value):
  if isNonNumericalNumberString(key):
    dataType = "str"
  elif isinstance(value, dict):
    if "type" in value.keys() and value["type"] == "Point":
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
        if isDateString(value):
          dataType = "date"
        else:
          dataType = "str"
  return dataType


def extractFieldTypesFrom(dataPoint):
  fieldMeta = {}
  for key, val in dataPoint.iteritems():
    fieldMeta[key] = getDataType(key, val)
  return fieldMeta


def storeResource(redisClient, resource, field, fieldTypes):
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
    "catalogEntry": resource.json()
  }))


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



def validateTemporal(temporalField, data, fieldTypes):
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
  # TODO: Check to see if the latest data was too far in the past.


def run():
  redisClient = redis.StrictRedis(host="localhost", port=6379, db=0)
  stored = redisClient.keys("*")
  count = 0
  catalog = createCatalog()

  for page in catalog:
    for resource in page:
      id = resource.getId()
      name = resource.getName()
      domain = resource.getMetadata()["domain"]
      
      try:
        if id in stored:
          raise ValueError("Already stored.")
        # Need to get one data point to calculate the primary temporal field.
        try:
          dataPoint = resource.fetchData(limit=100)[0]
        except IndexError:
          raise ValueError("No data!")
        except KeyError:
          raise ValueError("Error fetching first data point: " + str(e));
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
        validateTemporal(primaryTemporalField, data, fieldTypes)
        fieldRanking = getFieldRanking(data)
        storeResource(redisClient, resource, primaryTemporalField, fieldTypes)
        print colored(
          "Stored %s (%s %s) by %s" % (name, id, domain, primaryTemporalField), 
          "green"
        )

      except ValueError as e:
        print colored(
          "\t%s (%s %s) is not temporal: %s" % (name, id, domain, e), 
          "yellow"
        )
      
      finally:
        count += 1
        if count % 10 == 0:
          keyCount = len(redisClient.keys("*"))
          percStored = float(keyCount) / float(count)
          print colored(
            "Processed %i streams...\nStored %i streams as temporal (%f)." 
              % (count, keyCount, percStored), 
            "cyan"
          )
  

if __name__ == "__main__":
  run()