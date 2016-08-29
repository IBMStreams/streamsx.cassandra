# CassandraSink

**NOTE: tuple field names need to match the field names in your Cassandra table EXACTLY.** 

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
#Table of Contents

- [Version 1.3.0](#version-12)
  - [Coming in Future Versions](#coming-in-future-versions)
    - [Future Functionality](#future-functionality)
    - [Documentation To-Dos](#documentation-to-dos)
- [Installation](#installation)
  - [Using the Distribution](#using-the-distribution)
  - [Building From Source](#building-from-source)
    - [Updating To New Version](#updating-to-new-version)
    - [Installing Toolkit From Scratch](#installing-toolkit-from-scratch)
- [Configuration and Setup](#configuration-and-setup)
  - [Setting Up Cassandra on OSX](#setting-up-cassandra-on-osx)
  - [Setting Up ZooKeeper on Your Virtual Machine](#setting-up-zookeeper-on-your-virtual-machine)
- [Usage](#usage)
  - [Sample SPL Gists](#sample-spl-gists)
  - [Null Value Configuration](#null-value-configuration)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Cassandra Sink Version 1.3.0

## Supports
**Streams Version:** 4.0.0+
**Cassandra Version:** 2.0, 2.1 (these releases use CQL 3.1)

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
timestamp	        | Not supported  | |
rstring	            | Supported      | varchar |
ustring	            | Supported      | varchar |
blob     	        | Not supported  | |
xml	                | Experimental   | varchar |
list<T>	            | Supported      | list<CQL equivalent> |
bounded list type	| Supported      | list<CQL equivalent> |
set<T>          	| Supported      | set<CQL equivalent>  |
bounded set type	| Supported      | set<CQL equivalent>  |
map<K,V>        	| Supported      | map<CQL K equivalent, CQL V equivalent> |
bounded map type	| Supported      | map<CQL K equivalent, CQL V equivalent> |
tuple<T name, ...>  | Not supported  | |

\* CQL 3.3 has support for bytes and shorts, however it is not supported by this operator at this time.

### Additional documentation
[Java equivalents for SPL types](http://www.ibm.com/support/knowledgecenter/SSCRJU_4.1.1/com.ibm.streams.dev.doc/doc/workingwithspltypes.html)

# Installation

## Using the Distribution
1. Download the tar to your VM
2. Untar the tar
3. Add the extracted tar as a toolkit location in Streams Studio. For more information, see [these instructions](https://github.com/TheWeatherCompany/analytics-streams-docs/blob/master/adding-a-toolkit.md).

## Building From Source

### Updating To New Version

If you have already gone through the install from scratch instructions below and just need the new version,
`cd` to the toolkit folder on your vm and run the following commands:

```
git pull
sbt ctk toolkit
```

Refresh the toolkit location in Streams Studio, and you should be good to go!

### Installing Toolkit From Scratch

You will need access to the Artifactory Analytics-Virtual repo to fetch dependencies on analytics-zooklient and streamsx.util

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
    
# Configuration and Setup

## Setting Up Cassandra on OSX

Assuming that your host machine is OSX!

I store my cassandra files in `/opt` but yours may be elsewhere.

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

## Setting Up ZooKeeper on Your Virtual Machine

ZooKeeper is already running as part of streams on your virtual machine, so you shouldn't need to start it up.

First, see if the correct variable is set for for your ZK connect string

```
$ echo $STREAMS_ZKCONNECT
localhost:21810
```

If nothing prints, or if anything other than `localhost:21810` prints, add the following line to the file `~/.bashrc`

```
export STREAMS_ZKCONNECT=localhost:21810
```

and then in the terminal run the following to see the change made

```
$ source ~/.bashrc
$ echo $STREAMS_ZKCONNECT
```

now navigate to the folder for this repo and into the scripts file. There is another bash script which uses zookeepercli to store the cfg.json file, also in that folder, in your local ZK install.

```
$ cd /wherever/this/folder/is/on/your/VIRTUAL/machine/streamsx.cassandra/scripts
$ ./setupLocalZK.sh
```

The `setupLocalZK.sh` script will first delete the znode in zookeeper if it exists and will then store the contents of cfg.json. The znode name is set to hello_world_info, change as you like.

You can see the contents of the znode by running

```
$ zookeepercli --servers localhost:21810 -c get /streamsx.cassandra/cassandra_config
$ zookeepercli --servers localhost:21810 -c get /streamsx.cassandra/null_values 

```

# Usage

## Sample SPL Gists

This gist shows a sample SPL file using the new ZooKeeper based configuration as well as samples of the configuration: <https://gist.github.com/ecurtin/2f0baf2d238dddbc461d3594ec3988e1> 

## Null Value Configuration

In previous versions, you had to build a map of all your fields to booleans and pass it in with every tuple. No more!!

There are now two ZooKeeper configuration files. 
One of them describes the connection info for Cassandra, the other describes the values for each field that will be seen as "null",
meaning that the value will not be present in the prepared statement for Cassandra.

If fields do not have a null value configured, they are assumed to always be valid.

Empty collections (maps, lists, sets) will automatically be written as nulls, no need to configure that.

See [the gist](https://gist.github.com/ecurtin/2f0baf2d238dddbc461d3594ec3988e1) for examples of null value configuration for the sample application.
