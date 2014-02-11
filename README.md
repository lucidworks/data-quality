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

For most classes with a main, running with no arguments will give command line syntax.  Note that "-h" is short for "--host"; running with no arguments gives syntax help.

Since these are quick/early dev prototypes, a few may have constants that set which core/collection to go after.

Others use Apache Commons CLI library, aka "Apache GetOpts".

General syntax is below, but some utilities take additional options.

### Single Core Syntax

Set the full URL:
* -u | --url http://.....

Or just set portions of it:
* -h | --host localhost
* -p | --port 8983 or 8888, etc
* -c | --collection demo_shard1_replica1

For example:

```java com.lucidworks.dq.data.EmptyFieldStats --host localhost --collection demo_shard1_replica1```

### Dual Core / Diff Syntax

You're comparing A to B.
* Lowercase single letters refer to A
* Uppercase single letters refer to B
* Long options have the suffix "_a" or "_b" added.

For example, to compare IDs of 2 cores, the following commands are equivalent:

```java com.lucidworks.dq.diff.DiffIds -h localhost -p 8983 -H localhost -P 8984```

```java com.lucidworks.dq.diff.DiffIds --host_a localhost --port_a 8983 --host_b localhost --port_b 8984```

DiffSchema can also read from XML files or automatically provide a Solr default schema.