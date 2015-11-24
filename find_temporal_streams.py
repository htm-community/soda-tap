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
  point = resource.getFirstDataPoint()
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


def getDataType(key, value):
  if isinstance(value, dict):
    return "dict"
  if isinstance(value, list):
    return "list"
  try:
    int(value)
    return "int"
  except ValueError:
    try:
      float(value)
      return "float"
    except ValueError:
      if isDateString(value):
        return "date"
      return "str"


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