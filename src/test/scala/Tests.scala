import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.mkuthan.spark.SparkSqlSpec
import org.scalatest.{BeforeAndAfter, FunSpec, FunSuite, Matchers}
import play.api.libs.json._
//import com.holdenkarau.spark.testing.SharedSparkContext
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


import org.apache.spark.sql._
import org.apache.spark.sql.types._



/**
  * Created by torbjorn.torbjornsen on 06.07.2016.
  */
class Tests extends FunSpec with Matchers with SparkSqlSpec{

  private var dao:DAO = _

  override def beforeAll():Unit={
    super.beforeAll()
    val _csc = csc
    val _sqlc = sqlc
    import _sqlc.implicits._

    _sqlc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarHeader.json").toDF().registerTempTable("acq_car_header")
    _sqlc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarDetails.json").toDF().registerTempTable("acq_car_details")

//    hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarHeader.json").toDF().registerTempTable("acq_car_header")
//    hc.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarDetails.json").toDF().registerTempTable("acq_car_details")

    dao = new DAO(sqlc, csc)
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

    it("can get latest details record which have not been deleted") {
      val headerUrl = "http://m.finn.no/car/used/ad.html?finnkode=78866263"
      val loadTime = 3
      val df = dao.getLatestDetails(headerUrl, loadTime)
      val array = df.select("information").collect
      array(0).toString should equal("[Fin bil]")
    }

    it("can parse and subset json car properties into scala map") {
      val jsonPropertiesMap = "{\"Salgsform\":\"Bruktbil til salgs\",\"Girkasse\":\"Automat\",\"Antall seter\":\"5\"}"
      val parsedPropertiesMap = Utility.getMapSubsetFromJsonMap(jsonPropertiesMap, Seq("Antall seter", "Girkasse")) //subset and remove json structure
      parsedPropertiesMap.size should equal(2)
      parsedPropertiesMap should contain key("Antall seter")
      parsedPropertiesMap should contain value("5")
      parsedPropertiesMap should contain key("Girkasse")
      parsedPropertiesMap should contain value("Automat")
    }

    it("can parse and subset json car equipment to scala list") {
      val jsonEquipmentArray = "[\"Aluminiumsfelger\",\"Automatisk klimaanlegg\",\"Skinnseter\"]"
      val parsedEquipmentList = Utility.getListSubsetFromJsonArray(jsonEquipmentArray, Seq("Automatisk klimaanlegg", "Skinnseter"))
      parsedEquipmentList.size should equal(2)
      parsedEquipmentList should contain ("Automatisk klimaanlegg")
      parsedEquipmentList should contain ("Skinnseter")
    }




        //dfAcqCarHeader.count should equal(5)

//      val dfAcqCarDetails = sqlContext.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarDetails.json")
//      dfAcqCarDetails.show
//
//      dfAcqCarDetails.registerTempTable("acq_car_details")
//      dfAcqCarHeader.registerTempTable("acq_car_header")
//
//      val dfCarHeaderAndDetails = sqlContext.sql("select h.url, d.url from acq_car_details d, acq_car_header h where d.load_date = h.load_date")
//      val dfCarHeaderAndDetails = sqlContext.sql("select h.url, d.url from acq_car_details d, acq_car_header h")
//      val dfCarHeaderAndDetails = sqlContext.sql("select h.url from acq_car_header AS h UNION select d.url from acq_car_header AS d")
//      dfCarHeaderAndDetails.collect
//
//      val propCarDF = Utility.mergeCarHeaderAndDetails(acqCarHeaderDF, acqCarDetailsDF)
//      propCarDF.show
//
//      propCarDF.count should equal(3)


//
//      //      val csc = new CassandraSQLContext(sc)
////      csc.setKeyspace("finncars")
//      val dfAcqCarDetails = csc.read.
//        format("org.apache.spark.sql.cassandra").
//        options(Map("table" -> "acq_car_details", "keyspace" -> "finncars")).
//        load().
//        select("url", "properties", "equipment", "information", "deleted", "load_time", "load_date").
//        limit(3).
//        toDF
//
//      dfAcqCarDetails.select("deleted").show
////
//      val detailUrls = dfAcqCarDetails.select("url").collect().map(row => (row(0).toString)).toList
//      val headerUrls = detailUrls.map(url => sc.cassandraTable("finncars", "acq_car_header").
//        select("title", "url", "location", "year", "km", "price", "load_time", "load_date").
//        where("url = ?", url)
//      )
//
//      val dfAcqCarHeader = sc.union(headerUrls).map(f => new AcqCarHeader(f.getString("title"), f.getString("url"), f.getString("location"), f.getString("year"), f.getString("km"), f.getString("price"), f.getLong("load_time"), f.getString("load_date"))).toDF
//      dfAcqCarHeader.join()
//
//      headerUrls.take(10).foreach(println)
//
//
//
//      case class AcqCarHeader(title:String, url:String, location:String, year: String, km: String, price: String, load_time:Long, load_date:String)
//      case class AcqCarDetails(url:String, properties:String, equipment:String, information:String, deleted:Boolean, load_time:Long, load_date:String)
//      PropCar(url:String, title:String, location:String, year: String, km: String, price: String, properties:String, equipment:String, information:String, deleted:Boolean, load_time:Long, load_date:String)
//    }




  }


  describe("Cassandra CRUD testing") {
    ignore("can create, update and delete record in acquisition layer") {
      val testAcqCarHeader = ("UnitTest","http://test.url", "testLocation", "testYear", "testKM", "testPrice", 1L, "01.01.2016")
      val testAcqCarDetails = ("http://test.url","{testPropertyKey:testPropertyValue}", "{testEquipment}", "{testInformation}", 1L, "01.01.2016")
    }
  }



  describe("Create Acq-detail object from JSON") {
    ignore ("can create acq-detail object from JSON"){
      val sourceJson = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinn.json")
      val jsonCarHdr: JsValue = Json.parse(sourceJson.mkString)
      val acqCarDetailsObject = Utility.createAcqCarDetailsObject(1, jsonCarHdr)
      //http://m.finn.no/car/used/ad.html?finnkode=78540425
      acqCarDetailsObject.properties should include ("Stasjonsvogn")
      acqCarDetailsObject.equipment should include ("Sentrallås")
      acqCarDetailsObject.information should include ("lettstartet varebil")
    }

  }


  describe("JSON to Cassandra") {
    //subject of the test

    ignore("can convert JSON hdr file to list of AcqCarHeaders") {
      val sourceJson = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinn.json")
      val jsonCarHdr: JsValue = Json.parse(sourceJson.mkString)
      val numOfCars = jsonCarHdr.\\("group")(0).as[JsArray].value.size
      val acqCarHeaderList = Range(0, numOfCars).map(i =>
        Utility.createAcqCarHeaderObject(i, jsonCarHdr)).toList
      acqCarHeaderList.length should equal (numOfCars)
    }

    ignore("can convert JSON hdr file to list of AcqCarDetails") {
      val sourceJson = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\carsFinnLimited.json")
      val jsonCarHdr: JsValue = Json.parse(sourceJson.mkString)
      val numOfCars = jsonCarHdr.\\("group")(0).as[JsArray].value.size
      val acqCarDetailList = Range(0, numOfCars).map(i =>
        Utility.createAcqCarDetailsObject(i, jsonCarHdr)).toList
      acqCarDetailList.length should equal (numOfCars)
    }



  }





}
