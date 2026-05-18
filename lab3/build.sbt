
ThisBuild / scalaVersion := "3.3.7"

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-Xmax-inlines", "64")
)

val flinkVersion = "2.2.1"

lazy val common = (project in file("common"))
  .settings(
    commonSettings,
    name := "common",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % "0.14.7",
      "io.circe" %% "circe-parser" % "0.14.7",
      "io.circe" %% "circe-generic" % "0.14.7"
    )
  )

lazy val producer = (project in file("producer"))
  .dependsOn(common)
  .settings(
    commonSettings,
    name := "producer",
    assembly / mainClass := Some("Producer"),
    assembly / assemblyJarName := "producer.jar",
    libraryDependencies ++= Seq(
      "com.github.tototoshi" %% "scala-csv" % "2.0.0",
      "org.apache.kafka" % "kafka-clients" % "4.2.0",
      "com.github.tototoshi" %% "scala-csv" % "2.0.0",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
      case _                                        => MergeStrategy.first
    }
  )

lazy val consumer = (project in file("consumer"))
  .dependsOn(common)
  .settings(
    commonSettings,
    name := "consumer",
    assembly / mainClass := Some("Consumer"),
    assembly / assemblyJarName := "consumer.jar",
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-clients" % flinkVersion,
      "org.apache.flink" % "flink-connector-kafka" % "4.0.1-2.0",
      "org.apache.flink" % "flink-connector-jdbc" % "3.3.0-1.20",
//      "org.flinkextended" %% "flink-scala-api-2" % "2.2.2",
      "org.postgresql" % "postgresql" % "42.7.1"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
      case _                                        => MergeStrategy.first
    }
  )

lazy val root = (project in file("."))
  .aggregate(common, producer, consumer)
  .settings(
    name := "lab3"
  )