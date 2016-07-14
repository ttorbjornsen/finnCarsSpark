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
import play.api.libs.json.Reads._ // Custom validation helpers
import play.api.libs.functional.syntax._ // Combinator syntax


System.setProperty("hadoop.home.dir", "C:\\Users\\torbjorn.torbjornsen\\Hadoop\\")
val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","192.168.56.56")
val sc = new SparkContext(conf)
sc.setLogLevel("WARN")

val _csc = new CassandraSQLContext(sc)
_csc.setKeyspace("finncars")
val _hc = new HiveContext(sc)
import _hc.implicits._ //allows registering temptables

val jsonDoc = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinn.json") //remember to update from Kafka with latest retrieval


val dfAcqCarHeader = _hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarHeader.json").toDF()
dfAcqCarHeader.write.
  format("org.apache.spark.sql.cassandra").
  options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
  mode(SaveMode.Append).
  save()

val dfAcqCarDetails = _hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarDetails.json").toDF()
dfAcqCarDetails.write.
  format("org.apache.spark.sql.cassandra").
  options(Map("table" -> "acq_car_details", "keyspace" -> "finncars")).
  mode(SaveMode.Append).
  save()

dfAcqCarHeader.registerTempTable("acq_car_header")
dfAcqCarDetails.registerTempTable("acq_car_details")

val dfCarHeader = _csc.read.
  format("org.apache.spark.sql.cassandra").
  options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
  load().
  select("title", "url", "location", "year", "km", "price", "load_time", "load_date").
  filter($"url" === "http://m.finn.no/car/used/ad.html?finnkode=78866263").
  limit(1)

val acqCarHeader = dfCarHeader.map(row => AcqCarHeader(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), (row(6).asInstanceOf[java.util.Date]).getTime(), row.getString(7))).collect.toList(0)
val dao = new DAO(_hc, _csc)


