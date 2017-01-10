package KQI

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 10/19/16.
  */
object TestNewData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestNewData").setMaster(s"local[40]").set("spark.akka.frameSize", "2047")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data =sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema",true.toString)
      .load("xxx")
    var col = data.columns
    val df=data.select(col(0),col.tail.take(col.length-2):_*)
    col = df.columns
    println(filterData(df,0,3,1).select(col(0)).distinct().count())
  }
  def filterData(data:DataFrame,Time:Int,Space:Int,Terminal:Int)={
    val fdata=data.filter(s"Time_flag=${Time} AND Space_flag=${Space} AND Terminal_flag=${Terminal}")
    fdata.describe(fdata.columns:_*).show(10,false)
    fdata/*.repartition(1).write.format("com.databricks.spark.csv").option("header","false").save(s"xxx${Time}${Space}${Terminal}")*/
  }
}
