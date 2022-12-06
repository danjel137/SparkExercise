ThisBuild / version := "0sbt .1.0-SNAPSHOT"


ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkSql_DataFrames_DataSet"
  )
//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies +="org.apache.spark" %% "spark-core" % "3.0.0"

libraryDependencies +="org.apache.spark" %% "spark-sql" % "3.0.0"

libraryDependencies +="org.apache.spark" %% "spark-mllib" % "3.0.0"

libraryDependencies +="org.apache.spark" %% "spark-streaming" % "3.0.0"
