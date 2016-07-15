import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.mkuthan.spark.SparkSqlSpec
import org.scalatest.{BeforeAndAfter, FunSpec, FunSuite, Matchers}
import play.api.libs.json._
import scala.io.Source

import scala.collection.JavaConversions._
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive._
import scala.collection.mutable.ArrayBuffer
import java.sql.{Date, Timestamp}
import scala.collection.JavaConversions._

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.sql.types._



/**
  * Created by torbjorn.torbjornsen on 06.07.2016.
  */
class Tests extends FunSpec with Matchers with SparkSqlSpec{

  private var dao:DAO = _
  private var randomAcqCarHeader:AcqCarHeader = _

  override def beforeAll():Unit={
    super.beforeAll()
    val _csc = csc
    val _hc = hc
    import _hc.implicits._

    //Utility.setupCassandraTestKeyspace() //create keyspace test_finncars


//
//    val dfAcqCarHeader = _hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarHeader.json").toDF()
//    dfAcqCarHeader.write.
//      format("org.apache.spark.sql.cassandra").
//      options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
//      mode(SaveMode.Append).
//      save()
//
//    val dfAcqCarDetails = _hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarDetails.json").toDF()
//    dfAcqCarDetails.write.
//      format("org.apache.spark.sql.cassandra").
//      options(Map("table" -> "acq_car_details", "keyspace" -> "finncars")).
//      mode(SaveMode.Append).
//      save()

//    dfAcqCarHeader.registerTempTable("acq_car_header")
//    dfAcqCarDetails.registerTempTable("acq_car_details")

    val dfRandomCarHeader = _csc.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
      load().
      select("title", "url", "location", "year", "km", "price", "load_time", "load_date").
      limit(1)

    //REPL : USE val
    randomAcqCarHeader = dfRandomCarHeader.map(row => AcqCarHeader(row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), (row(6).asInstanceOf[java.util.Date]).getTime(), row.getString(7))).collect.toList(0)
    //REPL : USE val
    dao = new DAO(_hc, _csc)
   }

  describe("application") {
    it("should be able to extract and correctly parse details page") {
      val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //temp
      val carDetails: Map[String, JsValue] = Utility.scrapeCarDetails(url)

      carDetails("properties").as[Map[String, String]] should contain key("Årsmodell")
      carDetails("equipment").as[List[String]] should contain ("Vinterhjul")
      carDetails("information").as[String] should include ("Xenonpakke")
      carDetails("deleted").as[Boolean] should equal(false)

    }

    it("can handle deleted detail car pages from finn") {
      val url = "http://m.finn.no/car/used/ad.html?finnkode=76755775"
      val carDetails = Utility.scrapeCarDetails(url)

      carDetails("properties").as[Map[String, String]] should contain key("NULL")
      carDetails("equipment").as[List[String]] should contain ("NULL")
      carDetails("information").as[String] should include ("NULL")
      carDetails("deleted").as[Boolean] should equal(true)
    }

     it("can parse and subset json car properties into scala map") {
      val jsonPropertiesMap = "{\"Salgsform\":\"Bruktbil til salgs\",\"Girkasse\":\"Automat\",\"Antall seter\":\"5\"}"
      val parsedPropertiesMap = Utility.getMapFromJsonMap(jsonPropertiesMap, Seq("Salgsform")) //exclude keys and remove json structure
      parsedPropertiesMap.size should equal(2)
      parsedPropertiesMap should contain key("Antall seter")
      parsedPropertiesMap should contain value("5")
      parsedPropertiesMap should contain key("Girkasse")
      parsedPropertiesMap should contain value("Automat")
    }

    it("can parse and subset json car equipment to scala list") {
      val jsonEquipmentArray = "[\"Aluminiumsfelger\",\"Automatisk klimaanlegg\",\"Skinnseter\"]"
      val parsedEquipmentList = Utility.getSetFromJsonArray(jsonEquipmentArray, Seq("Aluminiumsfelger"))
      parsedEquipmentList.size should equal(2)
      parsedEquipmentList should contain ("Automatisk klimaanlegg")
      parsedEquipmentList should contain ("Skinnseter")
    }

    ignore("can merge AcqCarHeader object with AcqCarDetails object") {
      val propCar:PropCar = dao.createPropCar(randomAcqCarHeader)
      propCar.information should equal ("Fin bil. NEDSATT PRIS")
    }

    it("can retrieve the last price from before it was marked as sold") {
      //val urlSoldCar = "http://m.finn.no/car/used/ad.html?finnkode=79068104"
      //val acqCarHeaderSoldCar = Utility.createAcqCarHeaderObjectFromUrl(urlSoldCar)
      //val lastPrice = dao.getLastPrice(acqCarHeaderSoldCar)
    }
  }


  describe("JSON to Cassandra") {
    //subject of the test

    it("can convert JSON hdr file to list of AcqCarHeaders") {
      val sourceJson = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinn.json")
      val jsonCarHdr: JsValue = Json.parse(sourceJson.mkString)
      val numOfCars = jsonCarHdr.\\("group")(0).as[JsArray].value.size
      val acqCarHeaderList = Range(0, numOfCars).map(i =>
        Utility.createAcqCarHeaderObject(i, jsonCarHdr)).toList
      acqCarHeaderList.length should equal (numOfCars)
    }

    it("can convert JSON hdr file to list of AcqCarDetails") {
      val sourceJson = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinnLimited.json")
      val jsonCarHdr: JsValue = Json.parse(sourceJson.mkString)
      val numOfCars = jsonCarHdr.\\("group")(0).as[JsArray].value.size
      val acqCarDetailList = Range(0, numOfCars).map(i =>
        Utility.createAcqCarDetailsObject(i, jsonCarHdr)).toList
      acqCarDetailList.length should equal (numOfCars)
    }



  }





}
