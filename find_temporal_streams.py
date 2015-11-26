import datetime
import json
import os
import sys
import urlparse

import redis
from termcolor import colored

from sodatap import createCatalog, ResourceError

REDIS_URL = os.environ["REDIS_URL"]
REDIS_DB = 0
POOL = None



def storeResource(redisClient, resource, field, 
                  fieldMapping, meanTimeDelta, type):
  id = resource.getId()
  redisClient.set(id, json.dumps({
    "type": type,
    "temporalField": field,
    "jsonUrl": resource.getJsonUrl(),
    "fieldTypes": fieldMapping,
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


def processResource(redisClient, resource, stored):
  id = resource.getId()
  name = resource.getName()
  link = resource.getPermalink()
  domain = resource.getMetadata()["domain"]
  try:
    if id in stored:
      raise ResourceError("Already stored.")
    # Need to get one data point to calculate the primary temporal field.
    primaryTemporalField = resource.getTemporalIndex()

    fieldMapping = resource.getFieldMapping()
    fieldNames = resource.getFieldNames()

    # If this is a not temporal stream, the function below will raise a
    # ResourceError
    resource.validate()
    
    # TODO: rank fields?
    # fieldRanking = getFieldRanking(data)

    # Identify stream type (scalar or geospatial).
    streamType = "scalar"
    
    if "location" in fieldNames \
    or ("latitude" in fieldNames and "longitude" in fieldNames):
      streamType = "geospatial"

    storeResource(
      redisClient, resource, primaryTemporalField,
      fieldMapping, resource.getMeanTimeDelta(), streamType
    )
    print colored(
      "  Stored %s stream \"%s\" (%s %s) by %s" 
        % (streamType, name, id, domain, primaryTemporalField),
      "green"
    )

  except ResourceError as e:
    print colored(
      "  %s | %s | " % (name, link),
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