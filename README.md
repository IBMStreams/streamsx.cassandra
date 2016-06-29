# CassandraSink

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Version 0.2: Still Really Alpha But Now With Collections!!](#version-02-still-really-alpha-but-now-with-collections)
  - [Coming in Future Versions](#coming-in-future-versions)
- [Setup](#setup)
  - [Updating To New Version](#updating-to-new-version)
  - [Installing Toolkit From Scratch](#installing-toolkit-from-scratch)
- [Usage](#usage)
  - [Sample SPL Gists](#sample-spl-gists)
  - [The Null Map Parameter](#the-null-map-parameter)
    - [Things That May Come Up In Testing](#things-that-may-come-up-in-testing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Version 0.2: Still Really Alpha But Now With Collections!!

This version is capable of writing primitive fields **and collections** to Cassandra.
That includes strings and numeric fields (int, long, etc) as well as lists, sets, and maps.

This version theoretically supports writing nulls though I haven't tested that at all.

NOTE: tuple field names need to match the field names in your Cassandra table EXACTLY. 

## Coming in Future Versions

- A caching layer for the prepared statements to greatly improve performance
- A better way of doing configuration

# Setup

## Updating To New Version

If you have already gone through the install from scratch instructions below and just need the new version,
`cd` to the toolkit folder on your vm and run the following commands:

```
git pull
sbt ctk
sbt toolkit
```

Refresh the toolkit location in Streams Studio, and you should be good to go!

## Installing Toolkit From Scratch

1. Install SBT on your virtual machine. See instructions for RedHat here: <http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html>
2. Clone this repo somewhere convenient on the filesystem of your virtual machine. It doesn't need to be in your Eclipse workspace
3. Ensure that src/main/resources/etc/streams.conf has the right IP for your local Cassandra.
    ```
    streams.cassandra-analytics {
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
7. Write a test project. Here's a gist showing the project I made. <https://gist.github.com/ecurtin/9ad5302f30298d28405f24c6b499c42f>
Note that this is a bad example because with "Hello Streams" as the key, this entry is getting overwritten in Cassandra each time. 
Because I stopped when the counter was 29, my entire table was just: 
    ```
    cqlsh:testkeyspace> select * from testtable;
    
     greeting      | count
    ---------------+-------
     Hello Steams! |    29
    
    (1 rows)
    ```
    
# Usage

## Sample SPL Gists

Here's a sample SPL file showing the new collections capability: <https://gist.github.com/ecurtin/4c01e61e8deff3b62a997f2d534672ba>
    
## The Null Map Parameter

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

I have NOT tested out trying to write nulls to Cassandra through this. The gist I shared up there is the full extent of my testing.

### Things That May Come Up In Testing
- RString vs. String: I had some issues with this writing the base code. 
I think rstrings are getting writing to Cassandra correctly, but it wouldn't surprise me if there's issues.
- Numeric types not mapping properly. It wouldn't surprise me if this is an issue.

