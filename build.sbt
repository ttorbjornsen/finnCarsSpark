name := "finnCarsSpark"

version := "1.0"

scalaVersion := "2.10.5"
val sparkVersion = "1.4.1"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" //%% henter automatisk rett scalaversjon av biblioteket
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion % "provided"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.2.1" % "provided"
libraryDependencies += "org.jsoup" % "jsoup" % "1.7.3"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "provided"

