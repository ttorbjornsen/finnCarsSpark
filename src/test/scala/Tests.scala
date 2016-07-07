import org.scalatest.{BeforeAndAfter, FunSpec, FunSuite, Matchers}
import play.api.libs.json._

import scala.io.Source


/**
  * Created by torbjorn.torbjornsen on 06.07.2016.
  */
class Tests extends FunSpec with Matchers {

  describe("application") {
    it("should be able to extract and correctly parse details page") {
      val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //temp
      val carDetails: Map[String, JsValue] = Utility.scrapeCarDetails(url)
      val carProperty: String = carDetails("properties")(0).toString
      val carEquipment: String = carDetails("equipment")(1).toString
      val carInformation: String = carDetails("information").toString

      carProperty should include("Årsmodell")
      carEquipment should include("Vinterhjul")
      carInformation should include("Xenonpakke")
    }

    it("can handle deleted detail car pages from finn") {
      val url = "http://m.finn.no/car/used/ad.html?finnkode=78601940"
      val carDetails = Utility.scrapeCarDetails(url)
      carDetails should contain key ("MISSING URL")
      carDetails should contain value (JsNull)
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
