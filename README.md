# SODA Tap

> This project is a work in progress. Please see the [project spec](Project-Spec.md) for details on continuing development.

## Current State

In its current state, this codebase is able to index all [Socrata](https://www.socrata.com/products/open-data/) data resources and identify _temporal_ data streams. Once identified and indexed within a Redis database, the web server component allows users to browse all the temporal data resources and view there latest information live, as data to feed the charts and maps are pulled direclty from the SODA API.

## Requirements

### Redis

You must provide a `REDIS_URL` to store details about the discovered temporal streams.

## Usage

Actual data is not stored in this project, only metadata about temporal streams found by the discover process.

## Discover Temporal Data

This is the discovery process. It should be run periodically to update the metadata about temporal streams in Socrata.

    python discover_temporal_data.py

This will run for a very long time. The idea is to run this every night or every few days to identify and update any new temporal data in the Socrata Platform. 

## Start the server

    python server.py

Currently, this is a very minimal website that allows paging through all the temporal Socrata data streams found in the discover process.

## The `sodatap` library

This will eventually be pulled out into another repo and be installable via pip, but not yet.
