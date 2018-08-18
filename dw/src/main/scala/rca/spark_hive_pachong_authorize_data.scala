package rca

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime


/**
  * Created by liuxinyuan on 2017/10/17.
  */
object spark_hive_pachong_authorize_data {
  def main(args: Array[String]) {
    //起始日期
    var start: String = null
    try {
      if (args.length == 0) {
        start = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
      } else if (args.length == 1) {
        start = addDays(args(0).toInt)
      } else {
        System.out.print("参数格式错误！")
        return
      }
    } catch {
      case ex: Exception => println("参数必须是数字！")
    }
    print("start is " + start)
    val conf = new SparkConf().setAppName("spark_hive_pachong_authorize_data")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    //解析mobanker_authadptation_biz_data
    val schemaString1 = "userId,mobile,channel,website,timestamp"
    val schema1 = StructType(schemaString1.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    //todo  修改时间  解析topic biz_microsite_zhuoyi_apphisinfo
    val json_rdd = sc.textFile("/datahouse/ods/topic/mobanker_authadptation_biz_data/" + start + "/*")
      // val json_rdd = sc.textFile("file:\\D:\\DATA\\mobanker_authadptation_biz_data.txt")
      .filter( x => x.contains("聚信立数据流程采集"))
      .map(x => x.replaceAll("\\\\r\\\\n", " ")
        .replaceAll("\\\\n", " ")
        .replaceAll("\\\\r", " "))
      .map(x => authadptationToJson(x))

    val json_dataframe = sqlContext.read.schema(schema1).json(json_rdd)
    val dataframe_select = json_dataframe.select(json_dataframe.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)



    //todo  修改时间  解析topic biz_crawler_query_new
    val schemaString = "data_type,timestamp,provider,mobile"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val query_rdd = sc.textFile("/datahouse/ods/topic/biz_crawler_query_new/" + start + "/*")
      // val query_rdd = sc.textFile("file:\\D:\\DATA\\biz_crawler_query_new.txt")
      .map(x => x.replaceAll("\\\\r\\\\n", " ")
      .replaceAll("\\\\n", " ")
      .replaceAll("\\\\r", " "))
      .map(x => queryToJson(x))

    val query_dataframe = sqlContext.read.schema(schema).json(query_rdd)
    val dataframe_query = query_dataframe.select(query_dataframe.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)

    //持久化biz_microsite_zhuoyi_apphisinfo
    val expr1 = concat_ws("`", dataframe_select.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/dw_crawler_user_info/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/dw_crawler_user_info/"), true)
    val rs1 = dataframe_select.na.fill("NULL").select(expr1).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/dw_crawler_user_info/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_dw_crawler_user_info/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_dw_crawler_user_info/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/dw_crawler_user_info/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_dw_crawler_user_info/part-00000.snappy"))


    //持久化biz_crawler_query_new
    val expr2 = concat_ws("`", dataframe_query.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/dw_query_data/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/dw_query_data/"), true)
    val rs2 = dataframe_query.na.fill("NULL").select(expr2).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/dw_query_data/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_dw_query_data/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_dw_query_data/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/dw_query_data/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_dw_query_data/part-00000.snappy"))
    sc.stop()
  }

  //解析kafka biz_microsite_zhuoyi_apphisinfo
  //userId,mobile,website,channel,timestamp
  def authadptationToJson(originText: String) = {
    var data = "{"
    //("\"data_type\":\"" +  prop.getString("data_type") + "\",")
    try {
      val jsonData = JSON.parseObject(originText)
      val prop = jsonData.getJSONObject("properties")
      data += "\"userId\":\"" + (prop.getString("userId") + "\",")
      data += "\"mobile\":\"" + (prop.getJSONObject("request_text").getString("mobile") + "\",")
      val website = prop.getJSONObject("request_text").getString("website")
      var channel = jsonData.getJSONObject("data").getString("channel")
      if ("bigData".equalsIgnoreCase(channel)) {
        channel = "ql"
      }
      data += "\"channel\":\"" + channel + "\","

      val head = prop.getJSONObject("request_text").getString("mobile").substring(0, 3)
      val chinaMobile = Array("134", "135", "136", "137", "138", "139", "147", "150", "151", "152", "157", "158", "159", "178", "182", "183", "184", "187", "188")
      val chinaUnicom = Array("130", "131", "132", "145", "155", "156", "185", "186", "175", "176")
      val chinaTelecom = Array("133\",\"153\",\"177\",\"180\",\"189\",\"181\",\"173")
      if (chinaMobile.contains(head)) {
        data += "\"website\":\"ChinaMobile\","
      } else if (chinaUnicom.contains(head)) {
        data += "\"website\":\"ChinaUnicom\","
      } else if (chinaTelecom.contains(head)) {
        data += "\"website\":\"ChinaTelecom\","
      } else if ("jingdong".equalsIgnoreCase(website)) {
        data += "\"website\":\"jingdong\","
      } else {
        data += "\"website\":\"" + website + "\","
      }

      //timestamp
      data += "\"timestamp\":" + jsonData.getLong("timestamp") + "}"
    } catch {
      case ex: Exception => {
        println("error:" + ex.printStackTrace() + "orgintext==============>" + originText)
      }
    }
    data
  }

  //判断用户是否登录成功逻辑
  def login(df: DataFrame) = {
    if (!df.where("response_text like '%10008%'").rdd.isEmpty()) {
      1
    } else if (!df.rdd.isEmpty() && df.where("response_text like '%10008%'").rdd.isEmpty()) {
      0
    } else {
      -1
    }
  }


  //解析kafka biz_crawler_query_new
  def queryToJson(originText: String) = {
    var data = "{"
    try {
      val jsonData = JSON.parseObject(originText)
      val prop = jsonData.getJSONObject("properties")
      data += ("\"data_type\":\"" + prop.getString("data_type") + "\",")
      data += ("\"timestamp\":" + (jsonData.getLong("timestamp") + ","))
      data += ("\"provider\":\"" + (prop.getString("provider") + "\","))
      data += ("\"mobile\":\"" + prop.getString("mobile") + "\"")
      data += "}"
    } catch {
      case e: Exception => println(e.getMessage)
    }
    data
  }

  def addDays(i: Integer) = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(new Date())
    calendar.add(Calendar.DAY_OF_MONTH, -i)
    format.format(calendar.getTime)
  }

}
