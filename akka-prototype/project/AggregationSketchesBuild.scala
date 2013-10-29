import sbt._
import sbt.Keys._

object AggregationSketchesBuild extends Build {

  lazy val aggregationSketches = Project(
    id = "aggregation-sketches",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "Aggregation Sketches",
      organization := "edu.berkeley.cs.amplab",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.3",
      resolvers ++= Seq(
        "sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
        "sonatype-releases"  at "http://oss.sonatype.org/content/repositories/releases",
        "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
        "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/"
      ),
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "1.9.2" % "test",
        "com.twitter" % "algebird-core_2.9.3" % "0.3.0"
      )
    )
  )
}
