import java.time.ZoneId

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.jsoup.{HttpStatusException, Jsoup}
import org.jsoup.nodes.{Entities, _}
import org.jsoup.nodes.Document.OutputSettings
import play.api.libs.json._
import org.jsoup.select.Elements
import org.jsoup.nodes.Document.OutputSettings

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */


object Utility {
  def createAcqCarDetailsObject(i:Int, jsonCarHdr:JsValue)= {
    val url = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("href").toString
    val jsonCarDetail = scrapeCarDetails(url)
    val carProperties = jsonCarDetail("properties").as[Map[String,String]]
    val carEquipment = jsonCarDetail("equipment").as[Set[String]]
    val carInformation = jsonCarDetail("information").toString
    val deleted = jsonCarDetail("deleted").as[Boolean]
    val load_time = new java.sql.Timestamp(jsonCarHdr.\\("timestamp")(0).as[Long])
    val load_date = new java.util.Date(load_time.getTime).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarDetails(url, carProperties, carEquipment, carInformation, deleted, load_time, load_date)
  }


  def createAcqCarHeaderObject(i:Int, jsonCarHdr:JsValue) = {
    val location = jsonCarHdr.\\("group")(0)(i).\("location")(0).\("text").toString
    val url = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("href").toString
    val title = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("text").toString
    val year = jsonCarHdr.\\("group")(0)(i).\("year")(0).\("text").toString
    val km = jsonCarHdr.\\("group")(0)(i).\("km")(0).\("text").toString
    val price = jsonCarHdr.\\("group")(0)(i).\("price")(0).\("text").toString
    val load_time = new java.sql.Timestamp(jsonCarHdr.\\("timestamp")(0).as[Long])
    val load_date = new java.util.Date(load_time.getTime).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarHeader(title, url, location, year, km, price, load_time, load_date)
  }

  def scrapeCarDetails(url:String):Map[String, JsValue]= {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78647939"
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //sold
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78601940" //deleted page
    val validUrl = url.replace("\"", "")
    try {
      val doc: Document = Jsoup.connect(validUrl).get
      val carPropElements: Element = doc.select(".mvn+ .col-count2from990").first()
      var carPropMap = Map[String, String]()

      if (carPropElements != null) {
        var i = 0
        for (elem: Element <- carPropElements.children()) {
          if ((i % 2) == 0) {
            val key = elem.text
            val value = elem.nextElementSibling().text
            carPropMap += (key.asInstanceOf[String] -> value.asInstanceOf[String])
          }
          i = i + 1
        }
      } else carPropMap = Map("MissingKeys" -> "MissingValues")

      var carEquipListBuffer: ListBuffer[String] = ListBuffer()
      val carEquipElements: Element = doc.select(".col-count2upto990").first()
      if (carEquipElements != null) {
        for (elem: Element <- carEquipElements.children()) {
          carEquipListBuffer += elem.text
        }
      } else carEquipListBuffer = ListBuffer("MissingValues")

      val carInfoElements: Element = doc.select(".object-description p[data-automation-id]").first()
      val carInfoElementsText = {
        if (carInfoElements != null) {
          carInfoElements.text
        } else "MissingValues"
      }

      val jsObj = Json.obj("url" -> url, "properties" -> carPropMap, "information" -> carInfoElementsText, "equipment" -> carEquipListBuffer.toList, "deleted" -> false)
      jsObj.value.toMap
    } catch {
      case e: HttpStatusException => {
        println("URL " + url + " has been deleted.")
        val jsObj = Json.obj("url" -> url, "properties" -> Map("NULL" -> "NULL"), "information" -> "NULL", "equipment" -> ListBuffer("NULL").toList, "deleted" -> true)
        jsObj.value.toMap
      }
    }


  }


  def getMapSubsetFromJsonMap(jsonString:String, keys:Seq[String]):Map[String,String] = {
//    val keys = Seq("Salgsform", "Girkasse")
//    val jsonString = "{\"Salgsform\":\"Bruktbil til salgs\",\"Girkasse\":\"Automat\",\"Antall seter\":\"5\"}"
    val jsValueMap: JsValue = Json.parse(jsonString)
    jsValueMap.as[Map[String,String]].filterKeys(keys.toSet)
  }

  def getListSubsetFromJsonArray(jsonString:String, elements:Seq[String]):Seq[String] = {
//    val jsonString = "[\"Aluminiumsfelger\",\"Automatisk klimaanlegg\",\"Skinnseter\"]"
//    val elements = Seq("Automatisk klimaanlegg", "Skinnseter")

    val jsValueArray:JsValue = Json.parse(jsonString)
    val list = jsValueArray.as[Seq[String]]
    list.filter(x => elements.contains(x))
  }

  def getStringFromJsonString(jsonString:String):String = {
//    val jsonString = "\"Fin bil\""
    Json.parse(jsonString).as[String]
  }

  def setupCassandraTestKeyspace() = {
    val conf = new SparkConf().setAppName("Testing").setMaster("local[*]").set("spark.cassandra.connection.host", "192.168.56.56")
    val ddl_prod = Source.fromFile("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\c.ddl").getLines.mkString
    val ddl_test = ddl_prod.replace("finncars", "test_finncars")
    val ddl_test_split = ddl_test.split(";")
    val ddl_test_cmds = ddl_test_split.map(elem => elem + ";")

    CassandraConnector(conf).withSessionDo { session =>
      ddl_test_cmds.map { cmd =>
        println(cmd)
        session.execute(cmd)
      }
    }
  }


  def mergeCarHeaderAndDetails(acqCarHeader:DataFrame, acqCarDetails:DataFrame) = {

//    val acqCarHeaderDF = sqlContext.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarHeader.json")
//    val acqCarDetailsDF = sqlContext.read.json("C:\\Users\\torbjorn.torbjornsen\\IdeaProjects\\finnCarsSpark\\files\\AcqCarDetails.json")
//
//    val dfCarHeaderAndDetails = acqCarHeaderDF.as("h").join(acqCarDetailsDF.as("d"))//, col("h.url") === col("d.url"), "left")
//    dfCarHeaderAndDetails.show
//    =  .select
//    acqCarHeader.join(acqCarDetails, $"url")
  }

}

