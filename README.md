Data Quality
============

Preliminary DQ / Data Quality prototype

## Individual Solr Cores / Collections:
* EmptyFieldStats
* TermStats
* TermCodepointStats

## Differences Between Cores / Collections:
* DiffIds
* DiffSchema
* DiffEmptyFieldStats

## Running

Since these are quick/early dev prototypes, some have constants that set which core/collection to go after.

Others use Apache Commons CLI library, aka "Apache GetOpts".  In those cases the arguments are typically:

Set the full URL:
* -u | --url http://.....

Or just set portions of it:
* -h | --host localhost
* -p | --port 8983 or 8888, etc
* -c | --collection demo_shard1_replica1

For example:

```java com.lucidworks.dq.data.EmptyFieldStats --host localhost --collection demo_shard1_replica1```
