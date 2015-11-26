## > Work in progress.

# SODA Tap

This codebase is able to index all Socrata data resources and identify _temporal_ data streams. Once identified and indexed within a Redis database, the web server component allows users to browse all the temporal data resources and view there latest information live, as data to feed the charts and maps are pulled direclty from the SODA API.

## Redis

You must provide a `REDIS_URL` to store details about the discovered temporal streams.

## Find the Temporal Data

    python find_temporal_data.py
    
This will run for a very long time. The idea is to run this every night or every few days to identify and update any new temporal data in the Socrata Platform. 

## Start the server

    python server.py
    
## The `sodatap` library

This will eventually be pulled out into another repo and be installable via pip, but not yet. 