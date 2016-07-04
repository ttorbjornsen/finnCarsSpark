import play.api.libs.json._

/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */

case class CarHeaderRaw(location:String, title:String, url: String, year: String, km: String, price: String)

object Utility {

  def createCarHeaderRawObject(i:Int, json:JsValue) = {
    CarHeaderRaw(json.\\("group")(0)(i).\("location")(0).\("text").toString, json.\\("group")(0)(i).\("title")(0).\("text").toString, json.\\("group")(0)(i).\("title")(0).\("href").toString, json.\\("group")(0)(i).\("year")(0).\("text").toString, json.\\("group")(0)(i).\("km")(0).\("text").toString,json.\\("group")(0)(i).\("price")(0).\("text").toString)
  }


}
