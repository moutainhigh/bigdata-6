package lxy

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/19.
  * sudo -u hive hadoop dfs -put mobile /user/hive/warehouse/dw_qlml.db/mobile201705/
  *
  * sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/mobile_deal/mobile-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/mobile201705/
  *
  * su hdfs
  * spark-submit --class spark_hive_mobile --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_mobile.jar
  */
object spark_hive_biz_microsite_bill_statistics {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("biz_microsite_bill_statistics")
    //.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val schemaString = "user_id,product,merchant_id,ip,bill,add_channel,create_time"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val json_rdd = sc.textFile("/datahouse/ods/mongo/biz_microsite_bill_statistics/biz_microsite_bill_statistics.json").map(x => x.replaceAll("\\\\r\\\\n", " ").replaceAll("\\\\n", " ").replaceAll("\\\\r", " "))
    val json_dataframe = sqlContext.read.schema(schema).json(json_rdd .map(x => stringToJson(x)))
    val dataframe_query = json_dataframe.select(json_dataframe.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)


    //持久化bigdata_crawler_renthouse_info
    val expr1 = concat_ws("`", dataframe_query.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/biz_microsite_bill_statistics_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/biz_microsite_bill_statistics_tmp/"), true)
    val rs1 = dataframe_query.na.fill("NULL").select(expr1).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/biz_microsite_bill_statistics_tmp/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_biz_microsite_bill_statistics/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_biz_microsite_bill_statistics/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/biz_microsite_bill_statistics_tmp/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_biz_microsite_bill_statistics/part-00000.snappy"))
    sc.stop()
  }

  def stringToJson(text: String) = {
    try {
      val obj = new JSONObject()
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jsonObject = JSON.parseObject(text)
      //user_id,product,merchant_id,ip,bill,add_channel,create_time
      //userId,product,merchantId,ip,bill,addChannel,create_update_timestamp
      obj.put("user_id", jsonObject.getInteger("userId").toString)
      obj.put("product", jsonObject.getString("product"))
      obj.put("merchant_id", jsonObject.getString("merchantId"))
      obj.put("ip", jsonObject.getString("ip"))
      obj.put("bill", jsonObject.getJSONObject("bill").toJSONString)
      obj.put("add_channel", jsonObject.getString("addChannel"))
      obj.put("create_time", format.format(new Date(jsonObject.getLong("create_update_timestamp"))))
      obj.toJSONString
    } catch {
      case e: Exception => println("解析失败" + e.getMessage)
        null
    }
  }
}
