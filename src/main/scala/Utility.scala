import java.time.ZoneId

import play.api.libs.json._

/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */

case class AcqCarHeader(title:String, url:String, location:String, year: String, km: String, price: String, load_time:Long, load_date:String)

object Utility {
  def createAcqCarHeaderObject(i:Int, json:JsValue) = {
    val location = json.\\("group")(0)(i).\("location")(0).\("text").toString
    val url = json.\\("group")(0)(i).\("title")(0).\("href").toString
    val title = json.\\("group")(0)(i).\("title")(0).\("text").toString
    val year = json.\\("group")(0)(i).\("year")(0).\("text").toString
    val km = json.\\("group")(0)(i).\("km")(0).\("text").toString
    val price = json.\\("group")(0)(i).\("price")(0).\("text").toString
    val load_time = json.\\("timestamp")(0).as[Long]
    val load_date = new java.util.Date(load_time).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().toString
    AcqCarHeader(title, url, location, year, km, price, load_time, load_date)
  }
}
