import os

import json
import urlparse

import web
import redis

from sodatap import createCatalog

ITEMS_PER_PAGE = 10
GOOGLE_MAPS_API_KEY = os.environ["GOOGLE_MAPS_API_KEY"]
REDIS_URL = os.environ["REDIS_URL"]
REDIS_DB = 0
POOL = None

urls = (
  "/", "index",
  "/catalog", "catalog",
  "/catalog/(.+)", "catalog",
  "/resource/(.+)", "resource",
)
app = web.application(urls, globals())
render = web.template.render('templates/')
redisUrl = urlparse.urlparse(REDIS_URL)
cat = createCatalog()

# def createConnectionPool():
#   redisUrl = urlparse.urlparse(REDIS_URL)
#   print redisUrl.hostname
#   print redisUrl.port
#   return redis.ConnectionPool(
#     host=redisUrl.hostname, port=redisUrl.port, 
#     db=REDIS_DB, password=redisUrl.password
#   )


def chunks(l, n):
  """Yield successive n-sized chunks from l."""
  for i in xrange(0, len(l), n):
    yield l[i:i + n]


#################
# HTTP handlers #
#################


class index:
  def GET(self):
    r = redis.Redis(
      host=redisUrl.hostname, port=redisUrl.port, 
      db=REDIS_DB, password=redisUrl.password
    )
    totalSodaResources = cat.getTotalSodaResourceCount()
    totalTemporalResources = len(r.keys("*")) - 2
    if totalTemporalResources < 0:
      totalTemporalResources = 0
    return render.layout(
      render.index(totalSodaResources, totalTemporalResources), 
      GOOGLE_MAPS_API_KEY
    )


class catalog:
  def GET(self, page=0):
    # r = redis.Redis(connection_pool=POOL)
    r = redis.Redis(
      host=redisUrl.hostname, port=redisUrl.port, 
      db=REDIS_DB, password=redisUrl.password
    )
    query = web.input()
    streamType = "*"
    if "type" in query:
      streamType = query["type"]
    storedKeys = sorted(r.keys(streamType + ":*"))

    chunked = list(chunks(storedKeys, ITEMS_PER_PAGE))
    try:
      pageIds = chunked[int(page)]
    except IndexError:
      return web.notfound("Sorry, the page you were looking for was not found.")
    page = [json.loads(r.get(id)) for id in pageIds]
    return render.layout(render.catalog(
      page, render.resource, render.dict, render.list
    ), GOOGLE_MAPS_API_KEY)


class resource:
  def GET(self, id=None):
    # r = redis.Redis(connection_pool=POOL)
    r = redis.Redis(
      host=redisUrl.hostname, port=redisUrl.port, 
      db=REDIS_DB, password=redisUrl.password
    )
    key = r.keys("*:" + id)
    if key is None:
      return web.notfound("The resource " + id + " was not found.")
    data = r.get(id)
    resource = json.loads(data)
    return render.layout(render.resource(
      resource, render.dict, render.list
    ), GOOGLE_MAPS_API_KEY)


##############
# Start here #
##############

if __name__ == "__main__":
  # TODO: put connection pool back (there were issues on Heroku with it).
  # POOL = createConnectionPool()
  app.run()
