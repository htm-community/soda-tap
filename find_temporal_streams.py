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



def storeResource(redisClient, resource):
  id = resource.getId()
  meanTimeDelta = resource.getMeanTimeDelta()
  payload = {
    "type": resource.getStreamType(),
    "temporalField": resource.getTemporalIndex(),
    "jsonUrl": resource.getJsonUrl(),
    "fieldTypes": resource.getFieldMapping(),
    "meanTimeDelta": str(meanTimeDelta),
    "catalogEntry": resource.json()
  }
  if resource.hasMultipleSeries():
    payload["seriesId"] = resource.getSeriesIdentifier()
    # payload["seriesNames"] = resource.getSeriesNames()
  redisKey = resource.getStreamType() + ":" + id
  redisClient.set(redisKey, json.dumps(payload))
  print colored(
    "  Stored %s stream \"%s\" (%s %s) by %s" 
      % (resource.getStreamType(), resource.getName(), resource.getId(), 
         resource.getDomain(), resource.getTemporalIndex()),
    "green"
  )


def processResource(redisClient, resource):
  try:
    # If this is a not temporal stream, the function below will raise a
    # ResourceError
    resource.validate()
    storeResource(redisClient, resource)
  except ResourceError as e:
    print colored(
      "  %s | %s | " % (resource.getName(), resource.getPermalink()),
      "yellow"
    ) + " " + colored(str(e), "magenta")


def run(offset=0):
  redisUrl = urlparse.urlparse(REDIS_URL)
  redisClient = redis.Redis(
    host=redisUrl.hostname, port=redisUrl.port, 
    db=REDIS_DB, password=redisUrl.password
  )
  count = offset
  catalog = createCatalog(offset=offset)

  for page in catalog:
    for resource in page:
      processResource(redisClient, resource)
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