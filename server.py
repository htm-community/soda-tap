import os

import json

import web
import redis

ITEMS_PER_PAGE = 10
GOOGLE_MAPS_API_KEY = os.environ["GOOGLE_MAPS_API_KEY"]

urls = (
  "/", "index",
  "/catalog", "catalog",
  "/catalog/(.+)", "catalog",
  "/resource/(.+)", "resource",
)
app = web.application(urls, globals())
render = web.template.render('templates/')
r = redis.StrictRedis(host="localhost", port=6379, db=0)


def chunks(l, n):
  """Yield successive n-sized chunks from l."""
  for i in xrange(0, len(l), n):
    yield l[i:i + n]

#####
# HTTP handlers
#####

class index:
  def GET(self):
    return render.layout(render.index(), GOOGLE_MAPS_API_KEY)

class catalog:
  def GET(self, page=0):
    query = web.input()
    if "type" in query:
      stored = sorted(r.smembers(query["type"]))
    else:
      stored = sorted(r.keys("*"))
    chunked = list(chunks(stored, ITEMS_PER_PAGE))
    try:
      pageIds = chunked[int(page)]
    except IndexError:
      return web.notfound("Sorry, the page you were looking for was not found.")
    page = [json.loads(r.get(id)) for id in pageIds]
    return render.layout(render.catalog(
      page, render.dict, render.list
    ), GOOGLE_MAPS_API_KEY)

class resource:
  def GET(self, id=None):
    key = r.keys("*:" + id)
    if key is None:
      return web.notfound("The resource " + id + " was not found.")
    data = r.get(id)
    resource = json.loads(data)
    return render.layout(render.resource(
      resource, render.dict, render.list
    ), GOOGLE_MAPS_API_KEY)

if __name__ == "__main__":
  app.run()
