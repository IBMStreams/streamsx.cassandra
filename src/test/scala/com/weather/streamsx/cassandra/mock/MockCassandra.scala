package com.weather.streamsx.cassandra

// I have yet to find a library for embedded Cassandra for unit testing that doesn't SUCK.
// I'm running cassandra locally while I run tests, which is not ideal, but it's better than these buggy Cassandra libraries.

object MockCassandra {
  val ip = "127.0.0.1,10.0.2.2"
  val port = 9042
}


//todo: this is a lame place to note todos

//todo: talk to senthil about shutting down the operator due to authentication issues on the keysapce and table that don't come up until runtime