package lxy

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime


/**
  * Created by liuxinyuan on 2017/10/17.
  * 解析topic mobanker_authadptation_biz_data
  * /datahouse/ods/topic/mobanker_authadptation_biz_data/
  */
object spark_hive_customer_auth_log {
  def main(args: Array[String]) {
    //起始日期
    var start: String = null
    try {
      if (args.length == 0) {
        start = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
      } else if (args.length >= 1) {
        start = args(0)
      }

      print("start is " + start)
      val conf = new SparkConf().setAppName("identity_log_model")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      val hdfsconf = sc.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)


      //解析identity_log_model
      val schemaString = "typeid,add_time,channel,status,error,transeriaisid,user_id,product,productType,system,bizborrowid,remoteIP"
      val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
      val json_rdd = sc.textFile("/datahouse/ods/topic/mobanker_authadptation_biz_data/" + start + "/*")
        //val json_rdd = sc.textFile("file:///D://data//mobanker_authadptation_biz_data.json")
        .map(x => x.replaceAll("\\\\r\\\\n", " ")
        .replaceAll("\\\\n", " ")
        .replaceAll("\\\\r", " ")
      ).map(x => queryToJson(x))

      val json_dataframe = sqlContext.read.schema(schema).json(json_rdd)
      val dataframe_query = json_dataframe.select(json_dataframe.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)

      //持久化identity_log_model
      val expr2 = concat_ws("`", dataframe_query.columns.map(col): _*)
      if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/customer_auth_log/")))
        fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/customer_auth_log/"), true)
      val rs2 = dataframe_query.na.fill("NULL").select(expr2).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/customer_auth_log/")
      if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_customer_auth_log/part-00000.snappy"))) {
        fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_customer_auth_log/part-00000.snappy"), true)
      }
      fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/customer_auth_log/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_customer_auth_log/part-00000.snappy"))
      sc.stop()
    }


    //解析kafka biz_crawler_query_new
    def queryToJson(originText: String) = {
      val data = new JSONObject();
      try {
        val jsonData = JSON.parseObject(originText)
        val propData = jsonData.getJSONObject("data")
        val properties = jsonData.getJSONObject("properties")
        data.put("typeid", jsonData.getString("typeId"))
        data.put("add_time", jsonData.getString("timestamp"))
        data.put("channel", propData.getString("channel"))
        data.put("status", propData.getInteger("status"))
        data.put("error", propData.getString("error"))
        data.put("transeriaisid", propData.getString("transerialsId"))
        data.put("user_id", properties.getString("userId"))
        data.put("product", properties.getString("product"))
        data.put("productType", properties.getString("productType"))
        data.put("system", properties.getString("system"))
        data.put("bizborrowid", properties.getString("bizBorrowId"))
        data.put("remoteIP", properties.getString("remoteIP"))
      } catch {
        case e: Exception => println("解析失败:" + e.getMessage)
      }
      data.toJSONString
    }

  }
}