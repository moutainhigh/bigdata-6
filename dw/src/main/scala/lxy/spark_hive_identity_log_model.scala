package lxy


import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime


/**
  * Created by liuxinyuan on 2017/10/17.
  */
object spark_hive_identity_log_model {
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
      val schemaString = "id,merchantid,merchantproductcode,requesttext,responsetext,starproductcode,transerialid,updatetime,createtime"
      val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
      val json_rdd = sc.textFile("/datahouse/ods/topic/star_product_identity-identity_log_model/" + start + "/*")
        // val json_rdd = sc.textFile("file:///D://data//identify.json")
        .map(x => x.replaceAll("\\\\r\\\\n", " ")
        .replaceAll("\\\\n", " ")
        .replaceAll("\\\\r", " ")
      )
        .map(x => queryToJson(x))

      val json_dataframe = sqlContext.read.schema(schema).json(json_rdd)
      val dataframe_query = json_dataframe.select(json_dataframe.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)

      //持久化identity_log_model
      val expr2 = concat_ws("`", dataframe_query.columns.map(col): _*)
      if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/identity_log_model/")))
        fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/identity_log_model/"), true)
      val rs2 = dataframe_query.na.fill("NULL").select(expr2).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/identity_log_model/")
      if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_identity_log_model/part-00000.snappy"))) {
        fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_identity_log_model/part-00000.snappy"), true)
      }
      fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/identity_log_model/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_identity_log_model/part-00000.snappy"))
      sc.stop()
    }


    //解析kafka biz_crawler_query_new
    def queryToJson(originText: String) = {
      val data = new JSONObject()
      try {
        val jsonData = JSON.parseObject(originText)
        val propData = jsonData.getJSONObject("data")
        data.put("id", propData.getString("id"))
        data.put("merchantid", propData.getString("merchantId"))
        data.put("merchantproductcode", propData.getString("merchantProductCode"))
        data.put("requesttext", propData.getString("requestText"))
        data.put("responsetext", propData.getString("responseText"))
        data.put("starproductcode", propData.getString("starProductCode"))
        data.put("transerialid", propData.getString("tranSerialId"))
        data.put("updatetime", propData.getString("updateTime"))
        data.put("createtime", jsonData.getString("timestamp"))
      } catch {
        case e: Exception => println("解析失败:" + e.getMessage)
      }
      data.toJSONString
    }
  }
}