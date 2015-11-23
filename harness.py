#!/usr/bin/env python

from sodatap import createCatalog

catalog = createCatalog()
print type(catalog)
page = catalog[0]
for resource in page:
  print resource

# for page in catalog:
#   for resource in page:
#     print resource
