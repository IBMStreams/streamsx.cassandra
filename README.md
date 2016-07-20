# CassandraSink

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
#Table of Contents

- [Version 0.3: ZooKeeper Based Config and Caching for Prepared Statements](#version-03-zookeeper-based-config-and-caching-for-prepared-statements)
  - [Coming in Future Versions](#coming-in-future-versions)
    - [Future Functionality](#future-functionality)
    - [Documentation To-Dos](#documentation-to-dos)
- [Setup](#setup)
  - [Using the Distribution](#using-the-distribution)
  - [Building From Source](#building-from-source)
    - [Updating To New Version](#updating-to-new-version)
    - [Installing Toolkit From Scratch](#installing-toolkit-from-scratch)
- [Usage](#usage)
  - [Setting Up Cassandra on OSX](#setting-up-cassandra-on-osx)
  - [Setting Up ZooKeeper on Your Virtual Machine](#setting-up-zookeeper-on-your-virtual-machine)
  - [Sample SPL Gists](#sample-spl-gists)
  - [The Null Map Parameter](#the-null-map-parameter)
- [Things That May Come Up In Testing](#things-that-may-come-up-in-testing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Version 0.3: ZooKeeper Based Config and Caching for Prepared Statements

This version adds the ability/requirement of configuring the connection to Cassandra
via JSON stored in ZooKeeper

NOTE: tuple field names need to match the field names in your Cassandra table EXACTLY. 

## Coming in Future Versions

### Future Functionality
- refactoring the nullMap situation in a big way
- improving caching performance

### Documentation To-Dos
- create a table showing SPL types and their corresponding Cassandra types
- include a section about Artifactory


# Setup

## Using the Distribution
1. Download the tar to your VM
2. Untar the tar
3. Add the extracted tar as a toolkit location in Streams Studio

That's it!

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
7. Write a test project. Here's a gist showing the project I made. <https://gist.github.com/ecurtin/a7594dafc418eb55863fff99ef5fb301> and here's some output from Cassandra.
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
    
# Usage

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
$ zookeepercli --servers localhost:21810 -c get /streamsx.cassandra/hello_world_info 
```

## Sample SPL Gists

Here's a new sample SPL file showing the new parameters for this version: <https://gist.github.com/ecurtin/a7594dafc418eb55863fff99ef5fb301>
    
## The Null Map Parameter

**THIS IS GETTING MAJORLY REFACTORED IN THE NEXT VERSION**

When your tuple gets to the CassandraSink operator, it needs to have a item which is a map<rstring, boolean> that maps the names of all the other fields
in your tuple to a boolean indicating whether or not the values are valid (not null).


This is probably not proper SPL syntax, but just as a quick example
```
tuple< rstring name, int age, rstring email>

<name = "Emily", age = 26, email = "this is not a valid email address", myMap = { "name" : true, "age" : true, "email" : false }>
```

And now in my CassandraSink operator: 

```
() as MySink = com.weather.streamsx.cassandra::CassandraSink {
    ...
    param
        ...
        nullMap: "myMap";
        ...
}

```

# Things That May Come Up In Testing
- RString vs. String: I had some issues with this writing the base code. 
I think rstrings are getting writing to Cassandra correctly, but it wouldn't surprise me if there's issues.
- Numeric types not mapping properly. It wouldn't surprise me if this is an issue.

