Data Quality
============

Preliminary DQ / Data Quality experiments and prototype.

## Individual Solr Cores / Collections:
* empty_fields - com.lucidworks.dq.data.EmptyFieldStats - generally which fields are populated, including percentages
* term_stats - com.lucidworks.dq.data.TermStats - token length, and terms > 3 standard deviations from that
* data_checker - com.lucidworks.dq.data.DateChecker - report on date fields, fit to idealized exponential growth curve
* code_points - com.lucidworks.dq.data.TermCodepointStats - look for potentially corrupted tokens by looking for strings that span the most Unicode classes

## Differences Between Cores / Collections:
* diff_ids - com.lucidworks.dq.diff.DiffIds - documents that are only in A or B
* diff_schema - com.lucidworks.dq.diff.DiffSchema - compares fields, types, dynamic field patterns, etc.
* diff_empty_fields - com.lucidworks.dq.diff.DiffEmptyFieldStats - compare population of collections

## Sample Reports
See ```src/main/resources/sample-reports/```

## Building

This project assumes Java 7 (aka Java 1.7)

If you were given a pre-built .jar file, skip to the section **Running**

To checkout and build the project you'll also need git and maven.  Issue the command:

```mvn package```

It will create a convenient **SELF CONTAINED** jar file at ```target/data-quality-java-1.0-SNAPSHOT.jar```

Henceforth we'll refer to this as just **data-quality.jar**, but substitie the full path and name of the file you created.

## Running

### data-quality.jar

In the following examples we refer to **data-quality.jar**, but the actual file you have might be called something like ```data-quality-java-1.0-SNAPSHOT.jar```; use that full name wherever we say data-quality.jar.  Also, if the jar file isn't in your current directory, you should include the full file path.

The jar is self contained, including all other project dependency libraries including SolrJ, and is a little over 30 megabytes in size.

### Two Ways to Run

The jar was built to be used with the ```java -jar``` convention.  Since this requires that only one class be declared as primary, we include a CmdLineLauncher class that routes to the other classes.  It's also possible to use the jar in your classpath and call specific java classes directly, provided you know the full package and class name.

*Developer Note:
The mapping between command name and full class name is in com.lucidworks.dq.util.CmdLineLauncher.java in the static CLASSES field; the "commands" are really just class aliases.*

Example: See what classes and commands are available:

```java -jar data-quality.jar```

Example output:
```
Pass a command name on the command line to see help for that class:
        empty_fields: Look for fields that aren't fully populated.
          term_stats: Look at indexed tokens and lengths in each field.
         code_points: Look for potentially corrupted tokens.  Assumption is corrupted data is more random and will therefore tend to span more Unicode classes.
        data_checker: Look at the dates stored the collection.
   diff_empty_fields: Compare fields that aren't fully populated between two cores/collections.
            diff_ids: Compare IDs between two cores/collections.
         diff_schema: Compare schemas between two cores/collections.
```

Example: Show the syntax for a specific command, for example ```empty_fields```:

```java -jar data-quality.jar empty_fields```

Modified Example: Show the same thing using more traditional Java syntax:

```java -Cupertino data-quality.jar com.lucidworks.dq.data.EmptyFieldStats```

Example output, using either java syntax:
```
usage: EmptyFieldStats -u http://localhost:8983 [-c <arg>] [-f <arg>] [-h
       <arg>] [-i] [-p <arg>] [-s] [-u <arg>]
 -c,--collection <arg>   Collection/Core for Solr, Eg: collection1
 -f,--fields <arg>       Fields to analyze, Eg: fields=name,category,
                         default is all fields
 -h,--host <arg>         IP address for Solr, default=localhost
 -i,--ids                Include IDs of docs with empty fields. WARNING:
                         may create large report
 -p,--port <arg>         Port for Solr, default=8983
 -s,--stored-fields      Also check stats of Stored fields. WARNING: may
                         take lots of time and memory for large
                         collections
 -u,--url <arg>          URL for Solr, OR set host, port and possibly
                         collection
```

### Script Wrapper

If you'll be doing this frequently you might wish to write a shell script wrapper.


**data-quality.sh** *(Linux, Mac, etc.)*
```
#!/bin/bash
JAR="/full/path/data-quality-java-1.0-SNAPSHOT.jar"
java -jar "$JAR" $*
```

On Unix, don't forget to ```chmod +x data-quality.sh```

Run with ```data-quality.sh empty_fields```

**data-quality.cmd** *(Windows)*
```
@echo off
set JAR=c:\full\path\data-quality-java-1.0-SNAPSHOT.jar
java -jar "%JAR%" %*
```

Run with ```data-quality empty_fields```


### Arguments

General rules:

* Give the command_name first, if using ```java -jar``` syntax.
* For most classes with a main, running with no arguments will give command line syntax.
* Do not use "-h" for help; "-h" is short for "--host" not "--help"; running with no arguments gives syntax help.

### Single Core Arguments

Set the full URL:
* -u | --url http://.....

Or just set portions of it:
* -h | --host localhost
* -p | --port 8983 or 8888, etc
* -c | --collection demo_shard1_replica1

For example, to get information about partially populated fields:

```java -jar data-quality.jar empty_fields --host localhost --collection demo_shard1_replica1```

### Class-Specific Arguments

A few arguments are specific to only 1 or 2 commands, either because they don't make sense elsewhere or because they're experimental.  If an option becomes popular, it could be added to other commands.

Example: For partially populated fields, include all the **actual IDs** of docs with missing values.

```java -jar data-quality.jar empty_fields --ids --host localhost --collection demo_shard1_replica1```

The ```--ids``` only exists in this one report at the moment, and can generate a very long report!

### Dual Core / Diff Syntax Arguments

The idea is that you're comparing A to B:
* Lowercase single letters refer to A
* Uppercase single letters refer to B
* Long options have the suffix "_a" or "_b" added.

For example, to compare IDs of 2 cores, the following commands are equivalent:

```java -jar data-quality.jar diff_ids -h localhost -p 8983 -H localhost -P 8984```

```java -jar data-quality.jar diff_ids --host_a localhost --port_a 8983 --host_b localhost --port_b 8984```

DiffSchema can also read from XML files or automatically provide a Solr default schema.

## Developers: Bonus Utilities, SolrJ wrappers, etc!

All under ```src/main/java/com/lucidworks/dq/util/```

* General:
  * Mostly static methods, for easy/safe reuse
* SolrUtils - SolrJ Wrappers!
  * Example code showing how to use SolrJ for more than just searching
  * get all values from a field
  * Indexed vs. Stored fields
  * wrapper around **/terms**
  * wrapper around **/admin/luke**
  * wrapper around **/schema/...**
  * wrapper around **/clustering**; requires ```-Dsolr.clustering.enabled=true``` on Java command line
  * Grabbing Facet values
  * Using Solr Stats
  * Traversing SolrJ ```NamedList``` and ```SimpleOrderedMap``` collection data types
* SetUtils:
  * inAOnly, inBOnly
  * Union, Intersection
  * choice of destructive (slightly faster) or non-destructive (safer! probably what you want)
  * Stable maps... usually preserves insertion order
  * head, tail, reverse
  * sortMapByValues
* DateUtils:
  * to / from various formats
  * workaround to SolrJ's habit of sometimes returning dates as strings
  * corrects for timezone issues
* StatsUtils:
  * sum, min, max
  * average, standardDeviation
  * Note: Solr can also do this, which is often faster
  * LeastSquares line fit and Exponential curve fitting
* LLR:
  * more advanced statistics
  * start at Log Likelihood Ration / G2
  * may have +/- sign issue

## TODO:

* Blog posts w/ code snippets
* Consider adding ```--ids``` to more classes
* Refactor to be more consistent about when data is actually fetched, when tabulations are actually performed, etc.  Ideally allow for an empty constructor, then setters, then a "run now" mode.
* Then refactor command line wrapper
* util.SolrUtils is getting pretty large...
* Maybe use logging instead of println... although that drags in library and config issues, warning messages, etc.  Dealing with slf4j warnings if you're a bit new to command line java is a hassle.
* Maybe use a real reporting framework... but worried about overhead...
* Unit tests: would need mock/static solr cores
* LLR: verify sign, package with report, command line args, etc
* Curve fitting: alternative to Least Squares
* Maybe include .sh and .cmd scripts, but also need overall .zip packaging
* Not very friendly for non-command-line
* Ajax/HTML5 wrapper might be nice

