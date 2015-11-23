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
    print "\t== %s is temporal ==" % resource.getName() 
    print getMainTemporalFieldName(temporalFieldNames)
    return True, temporalFieldNames
  return False, None


def getMainTemporalFieldName(temporalFieldNames):
  if len(temporalFieldNames) == 0:
    return None
  name = temporalFieldNames[0]
  for n in temporalFieldNames:
    if "created" in n.lower() or \
       "open" in n.lower() or \
       "received" in n.lower:
      name = n
  return name



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
      temporal, temporalFieldNames = isTemporal(resource)
      if temporal:
        field = getMainTemporalFieldName(temporalFieldNames)
        print "\tStoring %s ==> %s" % (id, field)
        r.set(id, json.dumps({
          "temporalField": field,
          "jsonUrl": resource.getJsonUrl(),
          "catalogEntry": resource.json()
        }))
      else:
        print "\tSkipping %s" % resource.getName()
    finally:
      count += 1
      if count % 10 == 0:
        print "Processed %i streams..." % count
        print "Stored %i streams as temporal." % len(r.keys("*"))