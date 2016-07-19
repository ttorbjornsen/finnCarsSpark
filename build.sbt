name := "finnCarsSpark"

version := "1.0"

scalaVersion := "2.11.8" //need 2.11 because limit of 23 columns in case class with 2.10
val sparkVersion = "1.4.1"


resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

//%% henter automatisk rett scalaversjon av biblioteket
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion exclude("org.apache.spark", "spark-streaming_2.10"),
  "com.typesafe.play" %% "play-json" % "2.4.3" % "compile" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.jsoup" % "jsoup" % "1.7.3" % "compile",
  "org.scalatest" %% "scalatest" % "2.2.6" % "compile"
).map(_.excludeAll(ExclusionRule(organization = "org.mortbay.jetty")))

parallelExecution in Test := false



assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
