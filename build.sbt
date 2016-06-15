name := "CassandraSink"

organization := "com.weather.streamsx"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

val awsJavaSdkVersion      = "1.9.34"
val cassandraDriverVersion = "2.1.9"
val cfgVersion             = "1.4.0-RELEASE"
val circeVersion           = "0.3.0"
val commonsCodecVersion    = "1.10"
//val geotoolsVersion        = "14.3"
val guavaVersion           = "19.0"
val jsrVersion             = "3.0.1"
val jtsVersion             = "1.10"
val junitVersion           = "4.10"
val log4jVersion           = "2.2"
val scalacheckVersion      = "1.11.4"
val scalatestVersion       = "2.2.4"
val scalazVersion          = "7.1.2"
val slf4jVersion           = "1.7.12"
val sprayJsonVersion       = "1.3.1"
val ssglibVersion          = "3.1.0-RELEASE"
val streamsUtilVersion     = "0.1.0-RELEASE"

val ssgDevHost          = "jenkinsutil.dev.sun.weather.com"
val ssgMvnPath          = "/pub/mvn/ssg"
val ssgMvnPathSnapshots = "/pub/mvn/ssg-snapshots"

resolvers ++= Seq(
  "TWC SSG Repo" at s"https://$ssgDevHost$ssgMvnPath",
  "TWC Snapshot" at s"https://$ssgDevHost$ssgMvnPathSnapshots"
  //  ,
  //  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools/"
)

libraryDependencies ++= Seq(
  "com.amazonaws"                % "aws-java-sdk-s3"       % awsJavaSdkVersion,
  "com.amazonaws"                % "aws-java-sdk-sqs"      % awsJavaSdkVersion,
  "com.datastax.cassandra"       % "cassandra-driver-core" % cassandraDriverVersion
    classifier "shaded"
    excludeAll(
    ExclusionRule(organization = "io.netty"),
    ExclusionRule(organization = "com.google.guava")
    ),
  "com.google.guava"             % "guava"                 % guavaVersion,
  "com.google.code.findbugs"     % "jsr305"                % jsrVersion              % "compile",
  "com.weather"                 %% "cfg"                   % cfgVersion,
  "com.weather.ssg"             %% "ssglib"                % ssglibVersion,
  "io.circe"                    %% "circe-core"            % circeVersion,
  "io.circe"                    %% "circe-generic"         % circeVersion,
  "io.circe"                    %% "circe-jawn"            % circeVersion,
  "org.apache.logging.log4j"     % "log4j-api"             % log4jVersion,
  //  "org.geotools"                 % "gt-shapefile"          % geotoolsVersion,
  "org.scalaz"                  %% "scalaz-core"           % scalazVersion,
  "org.slf4j"                    % "slf4j-api"             % slf4jVersion,
  "junit"                        % "junit"                 % junitVersion            % "test",
  "org.slf4j"                    % "slf4j-simple"          % slf4jVersion            % "test",
  "org.scalacheck"              %% "scalacheck"            % scalacheckVersion       % "test",
  "org.scalatest"               %% "scalatest"             % scalatestVersion        % "test"
)

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings