name := "CassandraSink"

organization := "com.weather.streamsx"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

target in assembly := file("impl/java/bin")
assemblyJarName in assembly := "com.weather.streamsx.jar"

val junitVersion           = "4.10"
val log4jVersion           = "2.2"
val scalacheckVersion      = "1.11.4"
val scalatestVersion       = "2.2.4"
val slf4jVersion           = "1.7.12"

val ssgDevHost          = "jenkinsutil.dev.sun.weather.com"
val ssgMvnPath          = "/pub/mvn/ssg"
val ssgMvnPathSnapshots = "/pub/mvn/ssg-snapshots"

resolvers ++= Seq(
  "TWC SSG Repo" at s"https://$ssgDevHost$ssgMvnPath",
  "TWC Snapshot" at s"https://$ssgDevHost$ssgMvnPathSnapshots"
)

libraryDependencies ++= Seq(
  "org.apache.logging.log4j"     % "log4j-api"             % log4jVersion,
  "org.slf4j"                    % "slf4j-api"             % slf4jVersion,
  "junit"                        % "junit"                 % junitVersion            % "test",
  "org.scalacheck"              %% "scalacheck"            % scalacheckVersion       % "test",
  "org.scalatest"               %% "scalatest"             % scalatestVersion        % "test",
  "org.slf4j"                    % "slf4j-simple"          % slf4jVersion            % "test"
)

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings