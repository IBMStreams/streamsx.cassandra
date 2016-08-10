//import scala.language.postfixOps // <- making IntelliJ hush about the ! bash command postfix

name := "streamsx.cassandra"
organization := "com.weather"
version := "1.2-RELEASE"
scalaVersion := "2.11.8"
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
compileOrder in Compile := CompileOrder.ScalaThenJava


val cassandraDriverVersion = "2.1.9"
val circeVersion           = "0.3.0"
val junitVersion           = "4.10"
val log4jVersion           = "2.2"
val scalacheckVersion      = "1.11.4"
val scalatestVersion       = "2.2.4"
val scalazVersion          = "7.1.2"
val slf4jVersion           = "1.7.12"
val streamsOperatorVersion = "4.1.0.0"
val streamsxZKVersion      = "0.3-SNAPSHOT"
val streamsxUtilVersion    = "0.2.5-RELEASE"


resolvers ++= Seq(
  "Artifactory" at "https://repo.artifacts.weather.com/analytics-virtual"
)

libraryDependencies ++= Seq(
  "com.datastax.cassandra"       % "cassandra-driver-core" % cassandraDriverVersion
    classifier "shaded"
    excludeAll(
    ExclusionRule(organization = "io.netty"),
    ExclusionRule(organization = "com.google.guava")
    ),
  "com.ibm"                      % "streams.operator"      % streamsOperatorVersion  % "provided",
  "com.weather"                 %% "streamsx-util"         % streamsxUtilVersion,
  "com.weather"                 %% "analytics-zooklient"   % streamsxZKVersion,
  "io.circe"                    %% "circe-core"            % circeVersion,
  "io.circe"                    %% "circe-generic"         % circeVersion,
  "io.circe"                    %% "circe-jawn"            % circeVersion,
  "org.scalaz"                  %% "scalaz-core"           % scalazVersion,
  "org.apache.logging.log4j"     % "log4j-api"             % log4jVersion,
  "org.slf4j"                    % "slf4j-api"             % slf4jVersion,
  "junit"                        % "junit"                 % junitVersion            % "test",
  "org.scalacheck"              %% "scalacheck"            % scalacheckVersion       % "test",
  "org.scalatest"               %% "scalatest"             % scalatestVersion        % "test",
  "org.slf4j"                    % "slf4j-simple"          % slf4jVersion            % "test",
  "org.cassandraunit"            % "cassandra-unit"        % "2.2.2.1"               % "test",
  "org.apache.curator" % "curator-test" % "2.11.0"
)

val jarFn = "streamsx.cassandra.jar"
val libDir = "impl/lib"

cleanFiles <+= baseDirectory { base => base / "com.weather.streamsx.sqs" }

def rmToolkit(u: Unit): Unit = "spl-make-toolkit -c -i ." !

val ctk = TaskKey[Unit]("ctk", "Cleans the SPL toolkit")
ctk <<= clean map rmToolkit

def mkToolkit(jar: sbt.File): Unit = "spl-make-toolkit -i ." ! match {
  case 0 => s"cp -p ${jar.getPath} $libDir/$jarFn" !
  case _ => sys.error(s"not copying $jarFn bc toolkit creation failed")
}

val dist = TaskKey[Unit]("dist", "Makes a distribution for the toolkit")
dist := {
  val dir = baseDirectory.value.getName
  val parent = baseDirectory.value.getParent
  val excludes = Seq(
    "build.sbt",
    "data",
    "lib/com.ibm.streams.operator.jar",
    "output",
    "project",
    "src",
    "target",
    ".apt_generated",
    ".classpath",
    ".git",
    ".gitignore",
    ".project",
    ".settings",
    ".toolkitList"
  ).map(d => s"--exclude=$d").mkString(" ")
  s"tar -zcf $parent/${name.value}_${version.value}.tgz -C $parent $dir $excludes" !
}

val toolkit = TaskKey[Unit]("toolkit", "Makes the SPL toolkit")
toolkit <<= assembly map mkToolkit
dist <<= dist.dependsOn(toolkit)
