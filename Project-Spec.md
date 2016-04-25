# Specification

## Discovery Process

This nightly process should use the [Socrata Catalog](http://labs.socrata.com/docs/search.html) to query every [`dataset`](http://labs.socrata.com/docs/search.html#-limiting-results-to-specific-resource-types) across all Socrata installations.

Ideally this would be detached from any other web processed described below, but would update the same data store used by those web services, making any newly discovered streams immediately available to users upon discovery.

The discovery process would sample every single Socrata dataset, checking for a temporal component across a sample period of time or number of events. SODA does not expose a way to inspect a dataset for temporal attributes, to data sampling must be used.

You can see the current validation logic for dataset processing in [`Resource.validate()`](https://github.com/rhyolight/soda-tap/blob/master/sodatap/resource.py#L348-L392).

### Performance

The discover process should be multi-threaded and run entirely within a few hours. It will make an awful lot of HTTP calls, and therefore might require an API key from Socrata for authentication.

## Web Index

The website should allow unauthenticated users to peruse all the temporal datasets that have currently been discovered.

### Pages

#### `/home`

Landing page, explanation of usage.

#### `/domains`

Lists all the [domains](http://labs.socrata.com/docs/search.html#-listing-domains) and links each to `/streams?domain=XXX` for a complete list of each domain's streams.

#### `/streams`

A paginated simple list of streams by name, includes minimal details about streams. Compact. Should handle query parameters similarly to how the [SODA Catalog handles them](http://labs.socrata.com/docs/search.html#-complete-search-api) and simply pass them through to the REST call.

#### `/stream`

A detailed view of one data stream. If geospatial in nature, should display the last X data using Google Maps API. If scalar in nature, should plot the last X data points in charts. See River View for details.

## RESTful API

The REST API for this project simply needs to expose the temporal SODA streams and some minimal metadata about each one that has been discovered. There is only one URL in the interface.

#### `/temporal_streams`

Returns a JSON object representing all temporal streams found in the discovery process.