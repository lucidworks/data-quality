Data Quality
============

Preliminary DQ / Data Quality experiments and related utilities.

# Commands and Class Names

## Individual Solr Cores / Collections:
* ```empty_fields``` - generally which fields are populated, including percentages _(com.lucidworks.dq.data.EmptyFieldStats)_
* ```term_stats``` - token length, and terms > 3 standard deviations from average _(com.lucidworks.dq.data.TermStats)_
* ```date_checker``` - report on date fields, fit to idealized exponential growth curve _(com.lucidworks.dq.data.DateChecker)_
* ```code_points``` - look for potentially corrupted tokens by looking for strings that span the most Unicode classes _(com.lucidworks.dq.data.TermCodepointStats)_

## Differences Between Cores / Collections:
* ```diff_ids``` - documents that are only in A or B _(com.lucidworks.dq.diff.DiffIds)_
* ```diff_schema``` - compares fields, types, dynamic field patterns, etc. _(com.lucidworks.dq.diff.DiffSchema)_
* ```diff_empty_fields``` - compare population of collections _(com.lucidworks.dq.diff.DiffEmptyFieldStats)_

## Collection Diagnostics and Maintenance:
* ```doc_count``` - Count of active documents in a collection and send to standard out / stdout _(com.lucidworks.dq.data.DocCount)_
* ```dump_ids``` - Dump all the IDs from a collection to standard out / stdout _(com.lucidworks.dq.data.DumpIds)_
* ```delete_by_ids``` - Delete documents by their ID, either passed on the command line, or from a file, or from standard in / stdin _(com.lucidworks.dq.data.DeleteByIds)_
* ```solr_to_solr``` - Copy records from one Solr collection or core to another, can control which fields and records _(com.lucidworks.dq.data.SolrToSolr)_


## Sample Reports
See ```src/main/resources/sample-reports/```

# Download Prebuilt Binary

Fully runnable jar is available here:

* https://github.com/LucidWorks/data-quality/releases/tag/0.3
* Click the **green button** with ```data-quality-java-1.0-SNAPSHOT.jar``` and the download will start

# Building From Source

This project assumes Java 7 (aka Java 1.7)

If you were given a pre-built .jar file, skip to the section **Running**

To checkout and build the project you'll also need git and maven.  Issue the command:

```
git clone git@github.com:LucidWorks/data-quality.git
cd data-quality
mvn package
```

It will create a convenient **SELF CONTAINED** jar file at ```target/data-quality-java-1.0-SNAPSHOT.jar```

Henceforth we'll refer to this as just **data-quality.jar**, but substitute the full path and name of the file you created.

## Build Errors

Error:
```
[ERROR] COMPILATION ERROR :
[INFO] -------------------------------------------------------------
[ERROR] Failure executing javac, but could not parse the error:
javac: invalid target release: 1.7
```

Fix: Try setting JAVA_HOME to your Java 1.7 instance.

For example on Mac OS X:

```export JAVA_HOME=`javahome -v 1.7````

Or update ```~/.mavenrc```

Or define separate Java variables for Java 6 and 7 and then call for Java 7 in the pom.xml

Example Mac OS X ```~/.mavenrc```
```
export JAVA_HOME_6=`javahome -v 1.6`
export JAVA_HOME_7=`javahome -v 1.7`
```

Additions to ```pom.xml``` to specifically call for Java 7:
```
<project ...>
  ...
  <build>
    <plugins>
      ...
	  <plugin>
        <artifactId>maven-surefire-plugin</artifactId>
        <configuration>
            <!-- Set in ~/.mavenrc -->
            <!-- export JAVA_HOME_7=`javahome -v 1.7` -->
            <jvm>${env.JAVA_HOME_7}/bin/java</jvm>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

I might add this to a future version of this project's pom.xml

# Running

## data-quality.jar

In the following examples we refer to **data-quality.jar**, but the actual file you have might be called something like ```data-quality-java-1.0-SNAPSHOT.jar```; use that full name wherever we say data-quality.jar.  Also, if the jar file isn't in your current directory, you should include the full file path.

The jar is self contained, including all other project dependency libraries including SolrJ, and is a little over 30 megabytes in size.

## Two Ways to Run

The jar was built to be used with the ```java -jar``` convention.  Since this requires that only one class be declared as primary, we include a CmdLineLauncher class that routes to the other classes.  It's also possible to use the jar in your classpath and call specific java classes directly, provided you know the full package and class name.

*Developer Note:
The mapping between command name and full class name is in com.lucidworks.dq.util.CmdLineLauncher.java in the static CLASSES field; the "commands" are really just class aliases.*

Example: See what classes and commands are available:

```java -jar data-quality.jar```

Example output (and list of valid commands):
```
Pass a command name on the command line to see help for that class:
        empty_fields: Look for fields that aren't fully populated.
          term_stats: Look at indexed tokens and lengths in each field.
         code_points: Look for potentially corrupted tokens.  Assumption is corrupted data is more random and will therefore tend to span more Unicode classes.
        data_checker: Look at the dates stored the collection.
   diff_empty_fields: Compare fields that aren't fully populated between two cores/collections.
            diff_ids: Compare IDs between two cores/collections.
         diff_schema: Compare schemas between two cores/collections.
           doc_count: Count of active documents in a collection to standard out / stdout.
            dump_ids: Dump all the IDs from a collection to standard out / stdout.
       delete_by_ids: Delete documents by their ID, either passed on the command line, or from a file, or from standard in / stdin.
        solr_to_solr: Copy records from one Solr collection or core to another.
```

Example: Show the syntax for a specific command, for example ```empty_fields```:

```java -jar data-quality.jar empty_fields```

Modified Example: Show the same thing using more traditional Java syntax:

```java -jar data-quality.jar com.lucidworks.dq.data.EmptyFieldStats```

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
 -s,--stored_fields      Also check stats of Stored fields. WARNING: may
                         take lots of time and memory for large
                         collections
 -u,--url <arg>          URL for Solr, OR set host, port and possibly
                         collection
```

## Script Wrapper

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


## Arguments

General rules:

* Give the command_name first, if using ```java -jar``` syntax.
* For most classes with a main, running with no arguments will give command line syntax.
* Do not use "-h" for help; "-h" is short for "--host" not "--help"; running with no arguments gives syntax help.

## Single Core Arguments

Set the full URL:
* -u | --url http://.....

Or just set portions of it:
* -h | --host localhost
* -p | --port 8983 or 8888, etc
* -c | --collection demo_shard1_replica1

For example, to get information about partially populated fields:

```java -jar data-quality.jar empty_fields --host localhost --collection demo_shard1_replica1```

## Dual Core / Diff Syntax Arguments

The idea is that you're referring to two Solr instances, **Solr instances A and B**:
* **Lowercase** single letters refer to **Solr instance A**
* **Uppercase** single letters refer to **Solr instance B**
* Long options have the suffix "_a" or "_b" added.

For example, to compare IDs of 2 cores, the following commands are equivalent:

```java -jar data-quality.jar diff_ids -h localhost -p 8983 -H localhost -P 8984```

```java -jar data-quality.jar diff_ids --host_a localhost --port_a 8983 --host_b localhost --port_b 8984```

DiffSchema can also read from XML files or automatically provide a Solr default schema.

## Class-Specific Arguments

A few arguments are specific to only 1 or 2 commands, either because they don't make sense elsewhere or because they're experimental.  If an option becomes popular, it could be added to other commands.

Example: For partially populated fields, include all the **actual IDs** of docs with missing values.

```java -jar data-quality.jar empty_fields --ids --host localhost --collection demo_shard1_replica1```

The ```--ids``` only exists in this one report at the moment, and can generate a very long report!

# Collection Maintenance Examples

## Example: Dump IDs to a File

All three of these examples do the same thing but using different command line syntax:

```java -jar data-quality.jar dump_ids -u http://localhost:8983/solr/collection1 > collection1_ids.txt```

```java -jar data-quality.jar dump_ids -h localhost -c collection1 > collection1_ids.txt```

```java -jar data-quality.jar dump_ids --host localhost --collection collection1 > collection1_ids.txt```

## Example: Compare IDs

Find IDs that are in an the old collection but are not in the new collection:

```java -jar data-quality.jar diff_ids --url_a http://localhost:8983/solr/old_collection --url_b http://localhost:8983/solr/new_collection --mode a_only --output_file old_records.txt```

Note: You can also compare a live collection to a file containing document IDs.  This is useful if you cannot access both solr instances at the same time; you can run on one system, then ftp or scp the file, and compare it to a second system.

## Example: Delete Documents by ID

Read IDs from a file and remove records from the collection, and just specify the host and collection, and use the short form of the options:

```java -jar data-quality.jar delete_by_ids -h localhost -c collection1 -f bad_ids.txt```

## Example: Copy Records

Copy data from an old Solr instance to a new instance:

```java -jar data-quality.jar solr_to_solr --url_a http://old_server:8983/solr/old_collection --url_b http://new_server:8983/solr/new_collection --exclude_fields timestamp,text_en --xml```

Remember, to see all the options available from **solr_to_solr** just run it without any options:

```java -jar data-quality.jar solr_to_solr```

Example output:
```
Copy records from one Solr collection or core to another.
usage: SolrToSolr --url_a http://localhost:8983/collection1 --url_b
       http://localhost:8983/collection2 [-b] [-c <arg>] [-C <arg>] [-f <arg>]
       [-F <arg>] [-H <arg>] [-h <arg>] [-i] [-l] [-P <arg>] [-p <arg>] [-q
       <arg>] [-U <arg>] [-u <arg>] [-x]

Useful for tasks such as copying data to/from Solr clusters, migrating between
Solr versions, schema debugging, or synchronizing Solr instances. Can ONLY
COPY Stored Fields, though this is the default for many fields in Solr. In
syntax messages below, SolrA=source and SolrB=destination. Will use Solr
"Cursor Marks", AKA "Deep Paging", if available which is in Solr version 4.7+,
see https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results and
SOLR-5463
Options:
 -b,--batch_size           Batch size, 1=doc-by-doc. 0=all-at-once but be
                           careful memory-wise and 0 also disables deep paging
                           cursors. Default=1000
 -c,--collection_a <arg>   Collection/Core for SolrA, Eg: collection1
 -C,--collection_b <arg>   Collection/Core for SolrB, Eg: collection2
 -f,--include_fields <arg> Fields to copy, Eg: include_fields=id,name,category
                           Make sure to include the id! By default all stored
                           fields are included except system fields like
                           _version_ and _root_. You can also use simple
                           globbing patterns like billing_* but make sure to
                           use quotes on the command line to protect them from
                           the operating system. Field name and pattern
                           matching IS case sensitive unless you set
                           ignore_case. Patterns do NOT match system fields
                           either, so if really need a field like _version_
                           then add the full name to include_fields not using
                           a wildcard. Solr field names should not contain
                           commas, spaces or wildcard pattern characters. Does
                           not use quite the same rules as dynamicField
                           pattern matching, different implementation. See
                           also exclude_fields
 -F,--exclude_fields <arg> Fields to NOT copy over, Eg:
                           exclude_fields=timestamp,text_en Useful for
                           skipping fields that will be re-populated by
                           copyField in SolrB schema.xml. System fields like
                           _version_ are already skipped by default. Use
                           literal field names or simple globbing patterns
                           like text_*; remember to use quotes on the command
                           line to protect wildcard characters from the
                           operating system. Excludes override includes when
                           comparing literal field names or when comparing
                           patterns, except that literal fields always take
                           precedence over patterns. If a literal field name
                           appears in both include and exclude, it will not be
                           included. If a field matches both include and
                           exclude patterns, it will not be included. However,
                           if a field appears as a literal include but also
                           happens to match an exclude pattern, then the
                           literal reference will win and it WILL be included.
                           See also include_fields
 -H,--host_b <arg>         IP address for SolrB, destination of records,
                           default=localhost
 -h,--host_a <arg>         IP address for SolrA, source of records,
                           default=localhost
 -i,--ignore_case          Ignore UPPER and lowercase differences when
                           matching field names and patterns, AKA case
                           insensitive; the original form of the fieldname
                           will still be output to the destination collection
                           unless output_lowercase_names is used
 -l,--lowercase_names      Change fieldnames to lowercase before submitting to
                           destination collection; does NOT affect field name
                           matching. Note: May create multi-valued fields from
                           previously single-valued fields, Eg: Type=food,
                           type=fruit -> type=[food, fruit]; if you see an
                           error about "multiple values encountered for non
                           multiValued field type" this setting can be changed
                           in SolrB's schema.xml file. There is no
                           output_uppercase since that would complicate the id
                           field. See also ignore_case
 -P,--port_b <arg>         Port for SolrB, default=8983
 -p,--port_a <arg>         Port for SolrA, default=8983
 -q,--query <arg>          Query to select which records will be copied; by
                           default all records are copied.
 -U,--url_b <arg>          URL SolrB, destination of records, OR set host_b
                           (and possibly port_b / collection_b)
 -u,--url_a <arg>          URL for SolrA, source of records, OR set host_a
                           (and possibly port_a / collection_a)
 -x,--xml                  Use XML transport (XMLResponseParser) instead of
                           default javabin; useful when working with older
                           versions of Solr, though slightly slower. Helps fix
                           errors "RuntimeException: Invalid version or the
                           data in not in 'javabin' format",
                           "org.apache.solr.common.util.JavaBinCodec.unmarshal
                           ", or similar errors.
```

# Developers: Bonus Utilities, SolrJ wrappers, etc!

All under ```src/main/java/com/lucidworks/dq/util/```

* General:
  * Mostly static methods, for easy/safe reuse
* SolrUtils - SolrJ Wrappers!
  * Example code showing how to use **SolrJ** for more than just searching!
  * Source code **comments** show some **equivalent HTTP URL syntax**
  * Get all values from a field
  * Indexed vs. Stored fields
  * Wrapper around **/terms**
  * Wrapper around **/admin/luke**
  * Wrapper around **/schema/...**
  * Wrapper around **/clustering**; requires ```-Dsolr.clustering.enabled=true``` on Solr's Java command line
  * Grabbing **Facet values**
  * Using Solr **Stats**
  * Traversing SolrJ ```NamedList``` and ```SimpleOrderedMap``` collection data types
  * Whether your Solr instance supports new **"Cursor Marks"**, AKA "Deep Paging", see https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results and SOLR-5463
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
* StringUtils:
  * Glob Patterns and Regex Utils
* StatsUtils:
  * sum, min, max
  * average, standardDeviation
  * Note: Solr can also do this, which is often faster
  * LeastSquares line fit and Exponential curve fitting
* LLR / Log-Likelihood Ratio:
  * more advanced statistics
  * start at Log Likelihood Ration / G2
  * may have +/- sign issue

# TODO:

* Blog posts w/ code snippets
* Pre-Built downloadable .jar
* Add Java 7 specific parameters to pom.xml ?
* Consider adding ```--ids``` to more classes
* Refactor to be more consistent about when data is actually fetched, when tabulations are actually performed, etc.  Ideally allow for an empty constructor, then setters, then a "run now" mode.
* Call for "is clustering enabled"
* Call to enable dynamic schemas, field guessing, etc.
* Maybe wrappers for simple collection maint, aliases, etc
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
* Fix indenting to be consistently just 2 spaces
* Javadoc

