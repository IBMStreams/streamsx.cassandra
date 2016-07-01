//import scala.language.postfixOps // <- making IntelliJ hush about the ! bash command postfix

name := "streamsx.cassandra"
organization := "com.weather"
version := "0.2-SNAPSHOT"
scalaVersion := "2.11.8"
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
//compileOrder in Compile := CompileOrder.ScalaThenJava


//val awsJavaSdkVersion      = "1.9.34"
val cassandraDriverVersion = "2.1.9"
val cfgVersion             = "1.4.0-RELEASE"
val circeVersion           = "0.3.0"
val commonsCodecVersion    = "1.10"
//val geotoolsVersion        = "14.3"
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
val streamsxUtilVersion    = "0.2.3-RELEASE"


val ssgDevHost          = "jenkinsutil.dev.sun.weather.com"
val ssgMvnPath          = "/pub/mvn/ssg"
val ssgMvnPathSnapshots = "/pub/mvn/ssg-snapshots"

resolvers ++= Seq(
  "Artifactory" at "https://repo.artifacts.weather.com/analytics-virtual",
  "TWC SSG Repo" at s"https://$ssgDevHost$ssgMvnPath",
  "TWC Snapshot" at s"https://$ssgDevHost$ssgMvnPathSnapshots",
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
//  // these are just to make IntelliJ happy
//  "org.scala-lang.modules"       % "scala-xml_2.11"        % "1.0.4",
//  "org.scala-lang.modules"       % "scala-parser-combinators_2.11" % "1.0.4",

  // these are actual dependencies
  "com.datastax.cassandra"       % "cassandra-driver-core" % cassandraDriverVersion
    classifier "shaded"
    excludeAll(
    ExclusionRule(organization = "io.netty"),
    ExclusionRule(organization = "com.google.guava")
    ),
  "com.weather"                 %% "streamsx-util"         % streamsxUtilVersion,
  "com.ibm"                      % "streams.operator"      % "4.1.0.0"               % "provided",
  "org.apache.logging.log4j"     % "log4j-api"             % log4jVersion,
  "org.slf4j"                    % "slf4j-api"             % slf4jVersion,
  "junit"                        % "junit"                 % junitVersion            % "test",
  "org.slf4j"                    % "slf4j-simple"          % slf4jVersion            % "test",
  "org.scalacheck"              %% "scalacheck"            % scalacheckVersion       % "test",
  "org.scalatest"               %% "scalatest"             % scalatestVersion        % "test",
   "com.weather"                 %% "cfg"                   % cfgVersion
  //  "com.weather.ssg"             %% "ssglib"                % ssglibVersion,
  //  "org.scalaz"                  %% "scalaz-core"           % scalazVersion,
  //  "com.amazonaws"                % "aws-java-sdk-s3"       % awsJavaSdkVersion,
  //  "com.amazonaws"                % "aws-java-sdk-sqs"      % awsJavaSdkVersion,
)

parallelExecution in Test := false

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