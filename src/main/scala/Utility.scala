import java.time.ZoneId

import org.jsoup.{HttpStatusException, Jsoup}
import org.jsoup.nodes.{Entities, _}
import org.jsoup.nodes.Document.OutputSettings
import play.api.libs.json._
import org.jsoup.select.Elements
import org.jsoup.nodes.Document.OutputSettings

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */


object Utility {
  def createAcqCarDetailsObject(i:Int, jsonCarHdr:JsValue)= {
    val url = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("href").toString
    val jsonCarDetail:JsObject = getCarsDetailFromFinn(url)
    val carProperties = jsonCarDetail.\\("properties")(0).toString
    val carEquipment = jsonCarDetail.\\("equipment")(0).toString
    val carInformation = jsonCarDetail.\\("information")(0).toString
    val load_time = jsonCarHdr.\\("timestamp")(0).as[Long]
    val load_date = new java.util.Date(load_time).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarDetails(url, carProperties, carEquipment, carInformation, load_time, load_date)
  }

  def createAcqCarHeaderObject(i:Int, jsonCarHdr:JsValue) = {
    val location = jsonCarHdr.\\("group")(0)(i).\("location")(0).\("text").toString
    val url = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("href").toString
    val title = jsonCarHdr.\\("group")(0)(i).\("title")(0).\("text").toString
    val year = jsonCarHdr.\\("group")(0)(i).\("year")(0).\("text").toString
    val km = jsonCarHdr.\\("group")(0)(i).\("km")(0).\("text").toString
    val price = jsonCarHdr.\\("group")(0)(i).\("price")(0).\("text").toString
    val load_time = jsonCarHdr.\\("timestamp")(0).as[Long]
    val load_date = new java.util.Date(load_time).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarHeader(title, url, location, year, km, price, load_time, load_date)
  }

  def getCarsDetailFromFinn(url:String):JsObject = {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78647939"
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //malformed url?
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78601940" //deleted page
    val validUrl = url.replace("\"", "")
    try {
      val doc: Document = Jsoup.connect(validUrl).get
      val carPropElements: Element = doc.select(".mvn+ .col-count2from990").first()
      var carPropListBuffer: ListBuffer[Map[String, String]] = ListBuffer()

      if (carPropElements != null) {
        var i = 0
        for (elem: Element <- carPropElements.children()) {
          if ((i % 2) == 0) {
            val key = elem.text
            val value = elem.nextElementSibling().text
            carPropListBuffer += Map(key.asInstanceOf[String] -> value.asInstanceOf[String])
          }
          i = i + 1
        }
      } else carPropListBuffer = ListBuffer(Map("MissingKeys" -> "MissingValues"))

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

      val jsObj = Json.obj("url" -> url, "properties" -> carPropListBuffer.toList, "information" -> carInfoElementsText, "equipment" -> carEquipListBuffer.toList)
      jsObj
    } catch {
      case e: HttpStatusException => {
        println("URL " + url + " has been deleted.")
        Json.obj()
      }
    }


  }

}

