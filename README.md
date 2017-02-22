# streamsx.cassandra

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
#Table of Contents

- [Description](#description)
- [Cassandra Sink](#cassandra-sink)
  - [Recent Changes](#recent-changes)
  - [Supported Versions](#supported-versions)
  - [Data Types](#data-types)
    - [Additional documentation](#additional-documentation)
- [Installation](#installation)
  - [Using the Distribution](#using-the-distribution)
  - [Building From Source](#building-from-source)
    - [Updating To New Version](#updating-to-new-version)
    - [Installing Toolkit From Scratch](#installing-toolkit-from-scratch)
    - [Running Unit Tests](#running-unit-tests)
- [Configuration and Setup for Sample Project](#configuration-and-setup-for-sample-project)
  - [Setting Up Cassandra on OSX](#setting-up-cassandra-on-osx)
  - [Setting Up Configuration Objects](#setting-up-configuration-objects)
- [Usage](#usage)
  - [Sample SPL](#sample-spl)
  - [Connection and Null Value Configuration](#connection-and-null-value-configuration)
  - [Future Work](#future-work)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Description

This is a toolkit containing operators that connect IBM Infosphere Streams to Apache Cassandra.

Currently this toolkit contains only one operator, a sink for writing Streams tuples to a Cassandra table.

Future operators may include

- A source operator for reading from Cassandra  
- An analytic operator for writing to Cassandra and returning a status, such as a message ID for a message that can now be deleted  

# Cassandra Sink

**IMPORTANT NOTE: tuple field names in Streams need to match the field names in your Cassandra table _EXACTLY_, including case** 

Due to a wealth of bug fixes and stability changes, it is **strongly** recommended that all users upgrade to version 1.3.0 from previous versions.

## Recent Changes
* Support for Streams 4.2.0
* Using Streams Configuration Objects instead of ZK

## Supported Versions
**Streams Versions:** 
* Stable: 4.0.0, 4.1.0 (streamsx.cassandra 1.3.x)
* Stable: 4.2.0 (streamsx.cassandra 2.0+) 

**Cassandra Versions:** 
* Stable: 2.0, 2.1 (these versions of C\* use CQL 3.1)  

## Data Types

SPL Type            | Support Status | CQL 3.1 Type | 
--------            | -------------- | -----------  |
boolean             | Supported      | boolean |
enum	            | Not supported  | |
int8	            | Supported      | int\* |
int16	            | Supported      | int\* |
int32	            | Supported      | int |
int64	            | Supported      | bigint |
uint8	            | Supported      | int\* |
uint16	            | Supported      | int\* |
uint32	            | Supported      | int |
uint64	            | Supported      | bigint |
float32	            | Supported      | float |
float64	            | Supported      | double |
decimal32	        | Supported      | decimal |
decimal64	        | Supported      | decimal |
decimal128	        | Supported      | decimal |
complex32	        | Not supported  | |
complex64	        | Not supported  | |
timestamp	        | Not supported  | \*\*\* |
rstring	            | Supported      | varchar |
ustring	            | Supported      | varchar |
blob     	        | Not supported  | |
xml	                | Experimental   | varchar\*\*|
list\<T\>	            | Supported      | list<CQL equivalent> |
bounded list type	| Supported      | list\<CQL equivalent\> |
set\<T\>          	| Supported      | set\<CQL equivalent\>  |
bounded set type	| Supported      | set\<CQL equivalent\>  |
map\<K,V\>        	| Supported      | map\<CQL K equivalent, CQL V equivalent\> |
bounded map type	| Supported      | map\<CQL K equivalent, CQL V equivalent\> |
tuple\<T name, ...\>  | Not supported  | |

\* CQL 3.3 has support for bytes and shorts, this functionality will be utilized in the future.

\*\* XML support is not fully tested. There is no native XML type in C\* so XML is brought in as a String.

\*\*\* Consider using a unix timestamp as a uint64.

### Additional documentation
[Java equivalents for SPL types](http://www.ibm.com/support/knowledgecenter/SSCRJU_4.1.1/com.ibm.streams.dev.doc/doc/workingwithspltypes.html)
[CQL 3.1 data type reference](https://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html)

# Installation

## Using the Distribution
1. Download the tar to your VM
2. Untar the tar
3. Add the extracted tar as a toolkit location in Streams Studio. For more information, see [these instructions](https://github.com/TheWeatherCompany/analytics-streams-docs/blob/master/adding-a-toolkit.md).

## Building From Source

All build instructions here are tailored towards the following setup:

- Host machine running OSX  
- Cassandra running locally on host machine  
- Streams QSE VM running on VirtualBox or similar

If you're using Windows or Linux as your host and find that these instructions don't apply, you can try running Cassandra locally on your VM and changing
the seed address to `localhost`.

### Updating To New Version

If you have already gone through the install from scratch instructions below and just need the new version,
`cd` to the toolkit folder on your vm and run the following commands:

```
git pull
sbt ctk toolkit
```

Refresh the toolkit location in Streams Studio, and you should be good to go!

### Installing Toolkit From Scratch

Your "virtual machine" in this context is the Streams QSE VM. These instructions were written for Streams 4.1.0 and 4.1.1, they have not been tested on 4.2.2.

1. Install SBT on your virtual machine. See instructions for RedHat here: <http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html>
2. Clone this repo somewhere convenient on the filesystem of your virtual machine. It doesn't need to be in your Eclipse workspace
3. Setup Cassandra on your host machine and your ZooKeeper configuration on your virtual machine. See the Usage section below for detailed instructions.
3. Ensure that the cfg.json file uploaded to ZK has the right IP for your local Cassandra. See the Usage section below for more on configuring ZK
    ```
    {
        ...
        seeds: "10.0.2.2"
        ...
    }
    ```
    This is your gateway IP. It's possible you may have a different one, you can check `netstat -rn` to see yours if `10.0.2.2` doesn't work.
4. In the top level of the repo, run `sbt toolkit`. You may need to create `impl/lib/` in the repo for it to run properly.
5. Create a new Streams project. Add the location of the repo as a toolkit location.
4. Start Cassandra on OSX running on localhost and listening on port 9042. I think those options are both defaults so you shouldn't need to change them.
6. Setup a keyspace and a table in Cassandra. I wrote a little bash script and a cql file to make it easy on myself, you can see them in the scripts folder in this repo.
The cql there matches what I did for my test project.
7. Write a test project. Here's a gist showing the project I made. <https://gist.github.com/ecurtin/2f0baf2d238dddbc461d3594ec3988e1> and here's some output from Cassandra.
    ```
    cqlsh:testkeyspace> select * from testtable;
    
     count | greeting       | nint | testlist  | testmap                      | testset
    -------+----------------+------+-----------+------------------------------+-----------
        19 | Hello Streams! | null | [1, 2, 3] | {7: True, 8: False, 9: True} | {4, 5, 6}
         2 | Hello Streams! | null | [1, 2, 3] | {7: True, 8: False, 9: True} | {4, 5, 6}
        24 | Hello Streams! | null | [1, 2, 3] | {7: True, 8: False, 9: True} | {4, 5, 6}
         3 | Hello Streams! | null | [1, 2, 3] | {7: True, 8: False, 9: True} | {4, 5, 6}
        35 | Hello Streams! | null | [1, 2, 3] | {7: True, 8: False, 9: True} | {4, 5, 6}
        30 | Hello Streams! | null | [1, 2, 3] | {7: True, 8: False, 9: True} | {4, 5, 6}
        16 | Hello Streams! | null | [1, 2, 3] | {7: True, 8: False, 9: True} | {4, 5, 6}
        ... etc etc
    ```

### Running Unit Tests

Unit tests do not run automatically in the build. They must be run on the VM because of dependencies on Streams libraries.

If you want to run the tests, first get Cassandra running on your host machine. 
See the next section, Setting Up Cassandra on OSX for more info.
You will need to set up very basic authentication and authorization.

Before starting Cassandra, in your `cassandra.yaml` change your authentication and authorization settings to the following:

```
authenticator: PasswordAuthenticator
authorizer: org.apache.cassandra.auth.CassandraAuthorizer
```

The default superuser login with these settings, unless deliberately changed, is username `cassandra`, password `cassandra`. 

The Cassandra seeds used in the tests are `127.0.0.1` and `10.0.2.2`

Now in your VM, run `sbt test`.
    
# Configuration and Setup for Sample Project

## Setting Up Cassandra on OSX

Assuming that your host machine is OSX!

**NOTE** Some modifications to `cassandra-cfg.json` and `setupCassandra.sh` will be necessary if you changed your `cassandra.yaml` to use password authentication. 
This tutorial will assume that you are using the standard AllowAll authentication, and that if you changed your settings you probably know enough about what you're doing to figure out the rest :)

On OSX, I store my cassandra files in `/opt` but yours may be elsewhere.

First, start Cassandra if it's not already started.
```
/opt/cassandra-cassandra-2.1.11/bin/cassandra
```

You can see if cassandra is running by
```
ps -ef | grep cassandra
```
and looking for the big long java process.

To run the sample SPL Gist, I've provided a CQL script and accompanying bash script to run it. Navigate to this repo folder on OSX and run the setup Cassandra script.
```
$ cd /wherever/this/folder/is/on/your/HOST/machine/streamsx.cassandra/scripts
$ ./setupCassandra
Setting up Cassandra
Done setting up Cassandra
$
```

## Setting Up Configuration Objects

Simply run 
```bash
./scripts/stConnectionConfig.sh
```
This script is also a good template for creating your own configuration objects.

*NOTE* Configuration objects are only accessible when running in distributed mode because they are tied to the domain.

# Usage

## Sample SPL

See `scripts/CassandraTestSPL.txt` for the hello world example. (`streamtool` tries to compile any `.spl` file it finds, so to prevent
the example file from getting mixed up in the toolkit I had to make the extension `.txt`)

## Connection and Null Value Configuration

If fields do not have a null value configured, they are assumed to always be valid.

Empty collections (maps, lists, sets) will automatically be written as nulls, no need to configure that.

Null values are now configured using configuration objects. See the `scripts/stConnectionConfig.sh` for an example of connection and null value configuration.

## Future Work

Here's what's next for streamsx.cassandra:

- Support for Cassandra 3.x  
- Consistent Region support  
- Cassandra read operator  
