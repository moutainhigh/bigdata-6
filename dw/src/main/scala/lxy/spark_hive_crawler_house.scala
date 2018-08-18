package lxy

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.DateTime

/**
  * Created by liuxinyuan on 2018/1/9.
  * 解析下面两个topic
  * bigdata_crawler_renthouse_count_info
  * bigdata_crawler_renthouse_info
  *
  */
object spark_hive_crawler_house {
  def main(args: Array[String]) {
    //起始日期
    var start: String = null
    if (args.length == 0) {
      start = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    } else if (args.length >= 1) {
      start = args(0)
    }
    print("start is " + start)

    val conf = new SparkConf().setAppName("crawler_house")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    //读取bigdata_crawler_renthouse_count_info
    val schemaString1 = "id,businesscount,city,fetchdate,fetchtime,rentcount,secondhandcount,website"
    val schema1 = StructType(schemaString1.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val json_rdd1 = sc.textFile("/datahouse/ods/topic/bigdata_crawler_renthouse_count_info/" + start + "/*")
      //   val json_rdd1 = sc.textFile("file:///D://data//bigdata_crawler_renthouse_count_info.json")
      .map(x => x.replaceAll("\\\\r\\\\n", " ")
      .replaceAll("\\\\n", " ")
      .replaceAll("\\\\r", " "))
      .map(x => renthousecountToJson(x))

    val json_dataframe1 = sqlContext.read.schema(schema1).json(json_rdd1)
    val dataframe_query1 = json_dataframe1.select(json_dataframe1.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)


    //持久化bigdata_crawler_renthouse_count_info
    val expr1 = concat_ws("`", dataframe_query1.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/crawler_house_count_info/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/crawler_house_count_info/"), true)
    val rs1 = dataframe_query1.na.fill("NULL").select(expr1).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/crawler_house_count_info/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_crawler_house_count_info/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_crawler_house_count_info/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/crawler_house_count_info/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_crawler_house_count_info/part-00000.snappy"))

    //读取bigdata_crawler_renthouse_info
    val schemaString2 = "id,address,area,city,feature,fetchdate,fetchtime,floor,housetype,orientation,price,renttype,space,title,type,website"
    val schema2 = StructType(schemaString2.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val json_rdd2 = sc.textFile("/datahouse/ods/topic/bigdata_crawler_renthouse_info/" + start + "/*")
   // val json_rdd2 = sc.textFile("file:///D://data//bigdata_crawler_renthouse_info.json")
      .map(x => x.replaceAll("\\\\r\\\\n", " ")
        .replaceAll("\\\\n", " ")
        .replaceAll("\\\\r", " "))
      .map(x => renthouseToJson(x))

    val json_dataframe2 = sqlContext.read.schema(schema2).json(json_rdd2)
    val dataframe_query2 = json_dataframe2.select(json_dataframe2.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)


    //持久化bigdata_crawler_renthouse_info
    val expr2 = concat_ws("`", dataframe_query2.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/crawler_house_info/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/crawler_house_info/"), true)
    val rs2 = dataframe_query2.na.fill("NULL").select(expr2).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/crawler_house_info/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_crawler_house_info/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_crawler_house_info/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/crawler_house_info/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_crawler_house_info/part-00000.snappy"))

    sc.stop()

  }

  //解析bigdata_crawler_renthouse_count_info
  def renthousecountToJson(originText: String) = {
    val data = new JSONObject()
    try {
      //id,businesscount,city,fetchdate,fetchtime,rentcount,secondhandcount,website

      val jsonData = JSON.parseObject(originText)
      val propData = jsonData.getJSONObject("data")
      data.put("id", propData.getString("rowkey"))
      data.put("businesscount", propData.getString("businessCount"))
      data.put("city", propData.getString("city"))
      data.put("fetchdate", propData.getString("fetchDate"))
      data.put("fetchtime", propData.getString("fetchTime"))
      data.put("rentcount", propData.getString("rentCount"))
      data.put("secondhandcount", propData.getString("secondHandCount"))
      data.put("website", propData.getString("website"))
    } catch {
      case e: Exception => println("解析失败:" + e.getMessage)
    }
    data.toJSONString
  }

  //解析bigdata_crawler_renthouse_info
  def renthouseToJson(originText: String) = {
    val data = new JSONObject()
    try {
      //id,address,area,city,feature,fetchdate,fetchtime,floor,housetype,orientation,price,renttype,space,title,type,website
      val jsonData = JSON.parseObject(originText)
      val propData = jsonData.getJSONObject("data")
      data.put("id", propData.getString("rowkey"))
      data.put("address", propData.getString("address"))
      data.put("area", propData.getString("area"))
      data.put("city", propData.getString("city"))
      data.put("feature", propData.getString("feature"))
      data.put("fetchdate", propData.getString("fetchDate"))
      data.put("fetchtime", propData.getString("fetchTime"))
      data.put("floor", propData.getString("floor"))
      data.put("housetype", propData.getString("houseType"))
      data.put("orientation", propData.getString("orientation"))
      data.put("price", propData.getString("price"))
      data.put("renttype", propData.getString("rentType"))
      data.put("space", propData.getString("space"))
      data.put("title", propData.getString("title"))
      data.put("type", propData.getString("type"))
      data.put("website", propData.getString("website"))
    } catch {
      case e: Exception => println("解析失败:" + e.getMessage)
    }
    data.toJSONString
  }
}


