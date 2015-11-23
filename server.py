import web
from sodatap import createCatalog

urls = (
  "/", "index",
  "/catalog", "catalog",
  "/catalog/(.+)", "catalog"
)
app = web.application(urls, globals())
render = web.template.render('templates')

class index:
  def GET(self):
    return "Nothing to see here (maybe /catalog)."


class catalog:
  def GET(self, page=0):
    catalog = createCatalog()
    page = catalog[int(page)]
    print len(page)
    return render.catalog(page, render.dict, render.list)


if __name__ == "__main__":
  app.run()
