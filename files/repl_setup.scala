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


//TEMP
val propCar = PropCar("http://m.finn.no/car/used/ad.html?finnkode=78866263","Volkswagen Passat","Kyrksæterøra","1999","12345","145000",Map("Salgsform" -> "Bruktbil til salgs", "Girkasse" -> "Automat", "Antall seter" -> "5"),Set("Aluminiumsfelger", "Automatisk klimaanlegg"),"Fin bil. NEDSATT PRIS",false,1451604800000L,"01.01.2016")
val propCarRDD = sc.parallelize(Seq(propCar))
propCarRDD.saveToCassandra("finncars", "prop_car_daily")
val acqCarHeaderUrl = acqCarHeader.url
val acqCarHeaderLoadTime = acqCarHeader.load_time

val prevAcqCarDetails = _csc.sparkContext.cassandraTable[AcqCarDetails]("finncars", "acq_car_details").
  where("url = ?", acqCarHeaderUrl).
  where("load_time <= ?", new java.util.Date(acqCarHeaderLoadTime)).
  filter(row => row.deleted == false).collect


      val acqCarDetails = prevAcqCarDetails.maxBy(_.load_time)
      val propertiesMap:Map[String,String] = {
        val jsValueMap = Json.parse(acqCarDetails.properties)
        jsValueMap.as[Map[String, String]].filterKeys {
          "".contains(_) == false
        }
      }

val propertiesMap:Map[String,String] = {
  val jsValueMap = Json.parse(acqCarDetails.properties)
  jsValueMap.as[scala.collection.immutable.ListMap[String, String]].filterKeys {
    "".contains(_) == false
  }
}




propertiesMap.getClass()
val propertiesMap:Map[String,String] = {
  val jsValueMap = Json.toJson(acqCarDetails.properties)
  jsValueMap.as[Map[String, String]].filterKeys {
    "".contains(_) == false
  }
}



val propertiesMap = Json.toJson(acqCarDetails.properties.replace("\"", "")) //write from JSON String to JsValue ok. Use Formats to be able to get from JsValue to Map[String, String]?
implicit val propertiesMap = acqCarDetails.properties.replace("\"", "").format[CarProperties] //write from JSON String to JsValue ok. Use Formats to be able to get from JsValue to Map[String, String]?

val propertiesMap:Map[String,String] = Map("Key" -> "Value", "Key2" -> "Value2")
val equipmentList:Set[String] = Set("Test")
val propCar = PropCar(url=acqCarHeader.url, location=acqCarHeader.location, title=acqCarHeader.title, year=acqCarHeader.year, km=acqCarHeader.km, price=acqCarHeader.price, properties=propertiesMap, equipment=equipmentList, information=acqCarDetails.information, deleted=acqCarDetails.deleted, load_time=acqCarHeader.load_time, load_date=acqCarHeader.load_date)
val propCarRDD = sc.parallelize(Seq(propCar)).map(identity)
propCarRDD.saveToCassandra("finncars", "prop_car_daily")

case class CarProperties(m:scala.collection.immutable.Map[String, String])
implicit val CarPropertiesReader = Json.reads[CarProperties] //use Play-JSON macro
implicit val CarPropertiesWriter = Json.writes[CarProperties] //use Play-JSON macro
Json.prettyPrint(Json.toJson(CarProperties(Map("key1" -> "value1", "key2" -> "value2"))))
val propertiesMap = Json.toJson(CarProperties(Map("key1" -> "value1", "key2" -> "value2")))

scala.util.parsing.json.JSONObject(acqCarDetails.properties)
val propertiesMap = Utility.getMapFromJsonMapTEMP(acqCarDetails.properties)
val propertiesMap = Utility.getMapFromJsonMap(acqCarDetails.properties)