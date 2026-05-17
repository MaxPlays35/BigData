ThisBuild / scalaVersion := "3.8.3"

lazy val root = (project in file("."))
  .settings(
    name := "lab2"
  )

val sparkVersion = "4.0.0"
val clickhouse_jdbc_version = "0.9.4"


libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion % "provided").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % sparkVersion % "provided").cross(CrossVersion.for3Use2_13),
  ("org.postgresql" % "postgresql" % "42.7.11"),
  ("com.clickhouse" % "clickhouse-jdbc" % clickhouse_jdbc_version classifier "all"),
  ("com.clickhouse.spark" %% "clickhouse-spark-runtime-4.0" % "0.10.0").cross(CrossVersion.for3Use2_13)
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
  case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
  case _                                        => MergeStrategy.first
}