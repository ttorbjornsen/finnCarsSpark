import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSpec, FunSuite, Matchers}
import play.api.libs.json._
import com.holdenkarau.spark.testing.SharedSparkContext
import scala.io.Source


/**
  * Created by torbjorn.torbjornsen on 06.07.2016.
  */
class Tests extends FunSpec with Matchers with SharedSparkContext {

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

    it("can extract detail and header page from database and load into one record"){
      val csc = new CassandraSQLContext(sc)
      csc.setKeyspace("finncars")

      PropCar(url:String, title:String, location:String, year: String, km: String, price: String, properties:String, equipment:String, information:String, deleted:Boolean, load_time:Long, load_date:String)
    }




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
