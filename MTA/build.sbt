name:= "MTA"

version := "0.1"

scalaVersion := "2.11.10"

resolvers += "Unidata maven repository" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
//https://mvnrepository.com/artifact/edu.ucar/netcdf
//https://mvnrepository.com/artifact/org.apache.spark/spark-core
//https://mvnrepository.com/artifact/org.slf4j/slf4j-api
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-actor
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-stream

libraryDependencies ++= Seq(
  "edu.ucar" % "cdm" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "edu.ucar" % "grib" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "edu.ucar" % "netcdf4" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "colt" % "colt" % "1.2.0",
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "log4j" % "log4j" % "1.2.17",
  "org.ejml" % "ejml-simple" % "0.32",
  "com.typesafe.akka" %% "akka-actor" % "2.4.4",
  "com.typesafe.akka" %% "akka-stream" % "2.4.4"
  //"org.openscience.cdk" % "cdk-bundle" % "2.1.1"
)
