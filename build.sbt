name := "hbaseClient"

version := "0.1"

scalaVersion := "2.12.8"

scalacOptions ++= Seq("-Ypartial-unification")
libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-effect" % "1.3.1",
  "co.fs2" %% "fs2-core" % "1.0.4",
  "org.apache.hbase" % "hbase-client" % "2.2.0",
  "org.apache.hbase" % "hbase-common" % "2.2.0",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
)