name := "finnCarsSpark"

version := "1.0"

scalaVersion := "2.10.5"
val sparkVersion = "1.4.1"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

//%% henter automatisk rett scalaversjon av biblioteket
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "compile"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "compile"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion % "compile"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "compile"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "compile"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion exclude("org.apache.spark", "spark-streaming_2.10")
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.2.1" % "compile"
libraryDependencies += "org.jsoup" % "jsoup" % "1.7.3" % "compile"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "compile"


javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

