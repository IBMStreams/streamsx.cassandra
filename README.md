# streamsx.cassandra

---

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
# Table of Contents

- [Latest Release Here](#latest-release-here)
- [Description](#description)
- [Cassandra Sink](#cassandra-sink)
  - [Recent Changes](#recent-changes)
  - [Supported Versions](#supported-versions)
  - [Data Types](#data-types)
    - [Additional documentation](#additional-documentation)
- [Installation](#installation)
  - [Using the Distribution](#using-the-distribution)
- [Sample Project](#sample-project)
  - [Running the Sample Project](#running-the-sample-project)
  - [Connection and Null Value Configuration](#connection-and-null-value-configuration)
  - [Setting Up Cassandra on OSX](#setting-up-cassandra-on-osx)
  - [Setting Up Configuration Objects](#setting-up-configuration-objects)
  - [NOT RECOMMENDED: Using JSON instead of Configuration Objects](#not-recommended-using-json-instead-of-configuration-objects)
- [Compiling and Installing Toolkit From Scratch](#compiling-and-installing-toolkit-from-scratch)
  - [You Don't Have To Compile The Toolkit Just To Use It!!](#you-dont-have-to-compile-the-toolkit-just-to-use-it)
  - [If You're Positive That You Need To Compile It](#if-youre-positive-that-you-need-to-compile-it)
  - [Running Unit Tests](#running-unit-tests)
- [Future Work](#future-work)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Latest Release [Here](https://github.com/IBMStreams/streamsx.cassandra/releases)

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
1. Download the tarball of the latest release to your VM <https://github.com/IBMStreams/streamsx.cassandra/releases/tag/2.0.2-RELEASE>
2. Untar the tarball
3. Add the extracted tar as a toolkit location in Streams Studio
And that's it!

# Sample Project

Here's the sample application, also available in the `scripts` folder in this repo. Notice that `nInt`, short for "nullInt" is going to be
written as null because its value matches the configured "null value" in the null value configuration object created by `scripts/stConnectionConfig.sh`
```
namespace com.weather.test;

composite CassandraTest {

	graph

		// The names of the fields in Greeting need to match the fieldnames in the Cassandra table _exactly_
		stream<rstring greeting, uint64 count, list<int32> testList, set<int32> testSet, map<int32, boolean> testMap, int32 nInt> Greeting = Beacon() {
			param
				iterations: 1000000u; //generate 1000000 tuples
				period : 0.5; //generate a tuple every 0.5 seconds
			output
				Greeting:
					greeting =  "Hello Streams!",
					count = IterationCount() + 1ul,
					testList = [1,2,3],
					testSet = {4, 5, 6},
					testMap = {7: true, 8 : false, 9: true},
					nInt = -2147483647;
		}


		() as CoolStuff = com.weather.streamsx.cassandra::CassandraSink(Greeting) {
			param
				connectionCfgObject : "testCassandraConfiguration";
				nullMapCfgObject : "testCassandraNullValues";
		}
}
```

## Running the Sample Project

1. Install the toolkit either by downloading a release or by cloning the repo and compiling from scratch.
2. Create a new Streams project in Streams Studio and copy/paste the sample SPL code above.
3. Setup a local instance of Cassandra either on your host machine or on your Streams QSE VM. See instructions below for more.
3. Run `scripts/setupCassandra.sh` to create the keyspace and table in your local Cassandra instance needed for the sample application.
4. Check to see if the connection info in `scripts/stConnectionConfig.sh` matches your local Cassandra instance. 
For instance, you may need to modify `seeds` depending on you setup Cassandra in the previous step.
5. Run `scripts/stConnectionConfig.sh` to create your Configuration Objects.
6. Build and launch your Streams application **_*in Distributed Mode*_**. Configuration objects are tied to the Streams Domain, and if you run your HelloWorld application in Standalone
mode it will not have access to the configuration objects and will throw you a bunch of errors.
7. Query your local Cassandra instance to see the results:
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

## Connection and Null Value Configuration

If fields do not have a null value configured, they are assumed to always be valid.

Empty collections (maps, lists, sets) will automatically be written as nulls, no need to configure that.

Null values are now configured using configuration objects. See the `scripts/stConnectionConfig.sh` for an example of connection and null value configuration.


## Setting Up Cassandra on OSX

Assuming that your host machine is OSX! You can also adapt these instructions to use a Cassandra installation on your Streams QSE VM.

**NOTE** Some modifications to your Cassandra connection configuration object and `setupCassandra.sh` will be necessary if you changed your `cassandra.yaml` to use password authentication. 
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

Navigate to this repo folder on OSX and run the setup Cassandra script to create the keyspace and table for the sample application.
```
$ cd /wherever/this/folder/is/on/your/HOST/machine/streamsx.cassandra/scripts
$ ./setupCassandra
Setting up Cassandra
Done setting up Cassandra
$
```

## Setting Up Configuration Objects

On your Streams VM, simply run 
```bash
./scripts/stConnectionConfig.sh
```
This script is also a good template for creating your own configuration objects.

*NOTE* Configuration objects are only accessible when running in distributed mode because they are tied to the domain.

More about configuration objects here: https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html

## NOT RECOMMENDED: Using JSON instead of Configuration Objects
You can also pass your connection info and null values to the operator as JSON strings using `jsonAppConfig` and `jsonNullMap`. This is not 
recommended as it is easy to, for instance, 
accidentally check your Cassandra password into version control, however it is an option if configuration objects are not a viable solution for your setup.

If you go this route, I would recommend exploring ways to write out your configuration as a JSON file and read it in through SPL at compile time. This would
prevent you from doing what I'm doing in the same below and putting your configuration for Cassandra directly in your SPL code.

In this (kinda unrealistic) sample code, I'm using a configuration object for the Cassandra connection info but I'm passing in my null values as a JSON string.
```
namespace com.weather.test;

composite CassandraTest {

    // PLEASE NOTE: It is HIGHLY recommended to use Configuration Objects instead of passing in config as JSON like I'm doing here.
    
	param
    expression<rstring> $nullValueJSON:
		'{
           "greeting" : "",
           "count" : 0,
           "nInt" : -2147483647
         }';

	graph

		// The names of the fields in Greeting need to match the fieldnames in the Cassandra table _exactly_
		stream<rstring greeting, uint64 count, list<int32> testList, set<int32> testSet, map<int32, boolean> testMap, int32 nInt> Greeting = Beacon() {
			param
				iterations: 1000000u; //generate 1000000 tuples
				period : 0.5; //generate a tuple every 0.5 seconds
			output
				Greeting:
					greeting =  "Hello Streams!",
					count = IterationCount() + 1ul,
					testList = [1,2,3],
					testSet = {4, 5, 6},
					testMap = {7: true, 8 : false, 9: true},
					nInt = -2147483647;
		}


		() as CoolStuff = com.weather.streamsx.cassandra::CassandraSink(Greeting) {
			param
				connectionCfgObject : "testCassandraConfiguration";
				jsonNullMap : $nullValueJSON;
		}
}
```

# Compiling and Installing Toolkit From Scratch

## You Don't Have To Compile The Toolkit Just To Use It!!

These compilation instructions are meant for those interested in developing and maintaining the toolkit.
If you're just interested in _using_ streamsx.cassandra, 
please grab the latest tarball from the releases page: https://github.com/IBMStreams/streamsx.cassandra/releases 
and follow the installation instructions [up above](#using-the-distribution), and welcome! We're glad you're 
here!

## If You're Positive That You Need To Compile It

All build instructions here are tailored towards the following setup:

- Host machine running OSX  
- Cassandra running locally on host machine  
- Streams QSE VM running on VirtualBox or similar

If you're using Windows or Linux as your host and find that these instructions don't apply, you can try running Cassandra locally on your VM and changing
the seed address to `localhost`.

Your "virtual machine" in this context is the Streams QSE VM. These instructions were written for Streams 4.1.0 and 4.1.1, they have not been tested on 4.2.2.

1. Install SBT on your virtual machine. See instructions for RedHat here: <http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html>
2. Clone this repo somewhere convenient on the filesystem of your virtual machine. It doesn't need to be in your Eclipse workspace
4. In the top level of the repo, run `sbt toolkit`. You may need to create `impl/lib/` in the repo for it to run properly.
4. To remove the toolkit files, run `sbt ctk`

## Running Unit Tests

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

# Future Work

Here's some potential future directions for streamsx.cassandra. Contributors welcome!

- Support for Cassandra 3.x  
- Consistent Region support  
- Cassandra read operator  
