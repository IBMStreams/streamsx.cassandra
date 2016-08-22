package com.weather.streamsx.cassandra
import scala.collection.JavaConverters._

class GreetingTest extends PipelineTest(
  "testk",
  "testt",
  """
     |create table IF NOT EXISTS testk.testt (
     |  greeting varchar,
     |  count bigint,
     |  testList list<int>,
     |  testSet set<int>,
     |  testMap map<int, boolean>,
     |  nInt int,
     |  PRIMARY KEY (count)
     |) with caching = 'none';
  """.stripMargin
){



  "Tuples" should "get written to Cassandra" in {
//    var t = generator.newEmptyTuple()

    val structureMap = Map( "greeting" -> "rstring",
                            "count" -> "uint64",
                            "testList" -> "list<int32>",
                            "testSet" -> "set<int32>",
                            "testMap" -> "map<int32, boolean>",
                            "nullInt" -> "int32"
                          )

    val tuple, valuesMap = genAndSubmitTuple(structureMap)

    val rows = session.execute(s"select * from $keyspace.$table").all.asScala.toSeq
    rows should have size 1
  }

}

/*
I like Ryan a lot and today is his birthday
Hello world I donh'rt reall ty likr this keybooard because the keys are not in the riught place and this for me is vberyu frustrating and also my hands are not centered very well so it's even more frustrating and I have no idea how I would feelk enteriung numbers siuce I usualluy use only one hand for that.nl
 */