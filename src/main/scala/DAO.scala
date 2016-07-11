/**
  * Created by torbjorn.torbjornsen on 11.07.2016.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class DAO (sqlc: SQLContext, csc:CassandraSQLContext) {
  def getAcqDetailsFromURL(url:String):DataFrame = {
    val dfAcqCarDetails = csc.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_details", "keyspace" -> "finncars")).
      load().
      select("url", "properties", "equipment", "information", "deleted", "load_time", "load_date").
      limit(3).
      toDF

  }
}
