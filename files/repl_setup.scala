/**
  * Created by torbjorn.torbjornsen on 12.07.2016.
  */

import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext} //allows registering temptables
import play.api.libs.json._
import scala.io.Source
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

System.setProperty("hadoop.home.dir", "C:\\Users\\torbjorn.torbjornsen\\Hadoop\\")
val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","192.168.56.56")
val sc = new SparkContext(conf)
sc.setLogLevel("WARN")

val csc = new CassandraSQLContext(sc)
csc.setKeyspace("finncars")
val _hc = new HiveContext(sc)
import _hc.implicits._ //allows registering temptables

val jsonDoc = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinn.json")

