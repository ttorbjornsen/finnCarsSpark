import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/** Lazily instantiated singleton instance of SQLContext */
object HiveContextSingleton {

  @transient  private var instance: HiveContext = _

  def getInstance(sparkContext: SparkContext): HiveContext = {
    if (instance == null) {
      instance = new HiveContext(sparkContext)
    }
    instance
  }
}