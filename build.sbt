import scala.language.postfixOps // <- making IntelliJ hush about the ! bash command postfix

name := "streamsx.cassandra"
organization := "com.weather"
version := "2.0.2-RELEASE"
scalaVersion := "2.11.8"
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
compileOrder in Compile := CompileOrder.ScalaThenJava

val circeVersion           = "0.3.0"
val ibmStreamsVersion      = "4.2.0.0"
val jodaTimeVersion        = "2.9.4"
val cassandraDriverVersion = "2.1.10.2"
val junitVersion           = "4.10"
val log4jVersion           = "2.2"
val scalacheckVersion      = "1.11.4"
val scalatestVersion       = "2.2.4"
val scalazVersion          = "7.1.2"
val slf4jVersion           = "1.7.12"
val curatorVersion         = "2.4.1"
val zooKlientVersion       = "0.3.1-RELEASE"
val streamsxUtilVersion    = "0.2.5-RELEASE"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.datastax.cassandra"       % "cassandra-driver-core" % cassandraDriverVersion
    classifier "shaded"
    excludeAll(
    ExclusionRule(organization = "io.netty"),
    ExclusionRule(organization = "com.google.guava")
    ),
  "org.apache.curator"           % "curator-framework"     % curatorVersion,
  "org.scalaz"                  %% "scalaz-core"           % scalazVersion,
  "org.apache.logging.log4j"    % "log4j-api"              % log4jVersion,
  "joda-time"                   % "joda-time"              % jodaTimeVersion,
  "com.google.code.gson"        % "gson"                   % "2.6.2",
  "junit"                       % "junit"                  % junitVersion            % "test",
  "org.scalacheck"              %% "scalacheck"            % scalacheckVersion       % "test",
  "org.scalatest"               %% "scalatest"             % scalatestVersion        % "test",
  "org.slf4j"                   % "slf4j-simple"           % slf4jVersion            % "test"
)

val jarFn = "streamsx.cassandra.jar"
val libDir = "impl/lib"

cleanFiles <+= baseDirectory { base => base / "com.weather.streamsx.cassandra" }

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
    "lib/com.ibm.*",
    "lib/streams.*",
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

test in assembly := {}

val toolkit = TaskKey[Unit]("toolkit", "Makes the SPL toolkit")
toolkit <<= assembly map mkToolkit
dist <<= dist.dependsOn(toolkit)

unmanagedJars in Compile += file(s"/opt/ibm/InfoSphere_Streams/$ibmStreamsVersion/lib/com.ibm.streams.operator.jar")
