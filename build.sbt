name := "spark-sfdc"

version := "1.0.0"

organization := "com.orgnisation"

scalaVersion := "2.12.1"



crossScalaVersions := Seq("2.10.5", "2.11.7")

val sparkVersion = "3.0.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion)

val sparkComponents = Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-csv" % "1.1",
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "com.lihaoyi" %% "requests" % "0.6.9" ,
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value,
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value ,
  "org.apache.hadoop" % "hadoop-client" % "2.2.0",
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile"
)


libraryDependencies += "org.scalatest" % "scalatest_2.12" % "3.2.14" % "test"
libraryDependencies += "org.scalatest" % "scalatest-flatspec_2.12" % "3.2.14" % "test"