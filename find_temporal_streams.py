import datetime
import json

import redis

from sodatap import createCatalog

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def isDateString(str):
  try:
    datetime.datetime.strptime(str, DATE_FORMAT)
    return True
  except ValueError:
    return False


def isTemporal(resource):
  temporalFieldNames = []
  point = resource.fetchData(limit=1)[0]
  for key, value in point.iteritems():
    if isinstance(value, basestring) and isDateString(value):
      temporalFieldNames.append(key)

  if len(temporalFieldNames) > 0:
    return True, temporalFieldNames, point
  return False, None, point


def getMainTemporalFieldName(temporalFieldNames):
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
    "_id", "_key", "checknum"
  ]
  for word in blacklist:
    if word in key:
      return True
  return False


def getDataType(key, value):
  if isNonNumericalNumberString(key):
    dataType = "str"
  elif isinstance(value, dict):
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


def extractFieldMetaFrom(dataPoint):
  fieldMeta = {}
  for key, val in dataPoint.iteritems():
    fieldMeta[key] = getDataType(key, val)
  return fieldMeta


r = redis.StrictRedis(host="localhost", port=6379, db=0)
stored = r.keys("*")
count = 0

catalog = createCatalog()
for page in catalog:
  for resource in page:
    id = resource.getId()
    try:
      if id in stored:
        print "\t%s already stored, skipping temporal check" % id
        continue
      temporal, temporalFieldNames, dataPoint = isTemporal(resource)
      if temporal:
        field = getMainTemporalFieldName(temporalFieldNames)
        print "\tStoring %s ==> %s" % (id, field)
        r.set(id, json.dumps({
          "temporalField": field,
          "jsonUrl": resource.getJsonUrl(),
          "fieldMeta": extractFieldMetaFrom(dataPoint),
          "catalogEntry": resource.json()
        }))
      else:
        print "\tSkipping %s" % resource.getName()
    finally:
      count += 1
      if count % 10 == 0:
        print "Processed %i streams..." % count
        print "Stored %i streams as temporal." % len(r.keys("*"))