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

import scala.collection.immutable.Map
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import scala.util.{Failure, Success}
import java.net.URL
import java.time.{LocalDate, ZoneId}

/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */


object Utility {
  def createAcqCarDetailsObject(i:Int, jsonCarHdr:JsValue)= {
    val url = jsonCarHdr.\\("group").head(i).\("title").head.\("href").as[String]
    val jsonCarDetail = scrapeCarDetails(url)
    val carProperties = jsonCarDetail("properties").toString
    val carEquipment = jsonCarDetail("equipment").toString
    val carInformation = jsonCarDetail("information").toString
    val deleted = jsonCarDetail("deleted").as[Boolean]
    val load_time = jsonCarHdr.\\("timestamp")(0).as[Long]
    val load_date = new java.util.Date(load_time).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarDetails(url, carProperties, carEquipment, carInformation, deleted, load_time, load_date)
  }


  def createAcqCarHeaderObject(i:Int, jsonCarHdr:JsValue) = {
    val location = jsonCarHdr.\\("group")(0)(i).\("location")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val url = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("href").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val title = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val year = jsonCarHdr.\\("group")(0)(i).\("year")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val km = jsonCarHdr.\\("group")(0)(i).\("km")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val price = jsonCarHdr.\\("group")(0)(i).\("price")(0).\("text").asOpt[String].getOrElse(Utility.Constants.EmptyString)
    val load_time = jsonCarHdr.\\("timestamp")(0).as[Long]
    val load_date = new java.util.Date(load_time).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarHeader(title, url, location, year, km, price, load_time, load_date)
  }

  def getURL(url: String)(retry: Int): Try[Document] = {
    Try(Jsoup.connect(url).get)
      .recoverWith {
        case _ if(retry > 0) => {
          Thread.sleep(3000)
          println("Retry url " + url + " - " + retry + " retries left")
          getURL(url)(retry - 1)
        }
      }
  }
  def scrapeCarDetails(url:String):Map[String, JsValue]= {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78647939"
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //sold
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78601940" //deleted page
    val validUrl = url.replace("\"", "")
    val doc: Try[Document] = getURL(validUrl)(10)
    doc match {
      case Success(doc) =>
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

        val carPriceElement: Element = doc.select(".mtn.r-margin").first()
        var carPriceText = {
          if (carPriceElement != null) carPriceElement.text else "MissingValue"
        }

        val carTitleElement: Element = doc.select(".tcon").first()
        var carTitleText = {
          if (carTitleElement != null) carTitleElement.text else "MissingValue"
        }

        val carLocationElement: Element = doc.select(".hide-lt768 h2").first()
        var carLocationText= {
          if (carLocationElement != null) carLocationElement.text else "MissingValue"
        }

        val carYearElement: Element = doc.select("hr+ .col-count2from990 dd:nth-child(2) , .mvn+ .col-count2from990 dt:nth-child(2)").first()
        var carYearText= {
          if (carYearElement != null) carYearElement.text else "MissingValue"
        }

        val carKMElement: Element = doc.select(".mvn+ .col-count2from990 dd:nth-child(6)").first()
        val carKMText = {
          if (carKMElement != null) carKMElement.text else "MissingValue"
        }

        val jsObj = Json.obj("url" -> url, "properties" -> carPropMap, "information" -> carInfoElementsText, "equipment" -> carEquipListBuffer.toList, "deleted" -> false, "title" -> carTitleText, "location" -> carLocationText, "price" -> carPriceText, "year" -> carYearText, "km" -> carKMText)
        jsObj.value.toMap
      case Failure(e) => {
        println("URL " + url + " has been deleted.")
        val jsObj = Json.obj("url" -> url, "properties" -> Map("NULL" -> "NULL"), "information" -> "NULL", "equipment" -> ListBuffer("NULL").toList, "deleted" -> true, "title" -> "NULL", "location" -> "NULL", "price" -> "NULL", "year" -> "NULL", "km" -> "NULL")
        jsObj.value.toMap
      }
    }
  }


  def getMapFromJsonMap(jsonString:String, excludedKeys:Seq[String]=Seq("None")):Map[String,String] = {
    //    val keys = Seq("Salgsform", "Girkasse")
    //    val jsonString = "{\"Salgsform\":\"Bruktbil til salgs\",\"Girkasse\":\"Automat\",\"Antall seter\":\"5\"}"
    val jsValueMap: JsValue = Json.parse(jsonString)
    val propertiesMap = jsValueMap.as[Map[String,String]]
    val hashMap = new HashMap[String, String]
    propertiesMap.map{case(k,v) => if (!excludedKeys.contains(k)) hashMap.put(k,v) }
    hashMap.toMap
  }

  def getSetFromJsonArray(jsonString:String, excludedElements:Seq[String]=Seq("None")):Set[String] = {
//    val jsonString = "[\"Aluminiumsfelger\",\"Automatisk klimaanlegg\",\"Skinnseter\"]"
//    val elements = Seq("Automatisk klimaanlegg", "Skinnseter")
    val jsValueArray:JsValue = Json.parse(jsonString)
    val set = jsValueArray.as[Set[String]]
    set.filter(x => !excludedElements.contains(x))
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

  def parseKM(km:String):Int = {
    //val km = "99 000 km"
    val parsedKM = km.replace(" ", "").replace("km", "").replace("\"", "")
    if (parsedKM.forall(_.isDigit)) parsedKM.toInt else -1
  }

  def parseFinnkode(url:String):Int = {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=72921101"
    val parsedFinnkode = url.substring(url.length-8,url.length).replace("\"", "")
    if (parsedFinnkode.forall(_.isDigit)) parsedFinnkode.toInt else -1
  }


  def parseYear(year:String):Int = {
    //val year = "2007"
    val parsedYear = year.replace("\"", "").replace("\"", "")
    if (parsedYear.forall(_.isDigit)) parsedYear.toInt else -1
  }

  def carMarkedAsSold(price:String):Boolean = {
    if (price == "Solgt") true else false
  }

  def getDatesBetween(dateStart:LocalDate, dateEnd:LocalDate):Seq[String] = {
    val daysBetween = dateStart.until(dateEnd).getDays
    val listOfDays = ListBuffer[String]()

    for (i <- 0 to daysBetween) {
      listOfDays += (dateStart.plusDays(i)).toString
    }

    listOfDays.toList
  }


  object Constants {
    val EmptyMap = Map("NULL" -> "NULL")
    val EmptyList = Set("NULL")
    val EmptyString = "NULL"
    val ETLSafetyMargin = 7 //days
    val ETLFirstLoadDate = LocalDate.of(2016,7,1)
  }





}

