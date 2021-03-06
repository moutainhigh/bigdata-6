package zfs

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
object spark_hive_mobile {
  def main(args: Array[String]) {
//    if (args.length<1){
//      System.err.println("please give the correct params")
//      System.exit(1)
//    }
//
//    val year =args(0).toString

    val conf = new SparkConf().setAppName("spark_hive_mobile")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyyMMdd")
    println("=========================================================date_etl:"+date_etl)


    val schemaString = "user_id,addtime,phone_name,phone_number,phone_os,phone_model,phone_ip,phone_user_info,querytime,device_id,device_id_type,mac,idfa,imei,imsi,add_product,add_channel,networktype,type,ip,version,phone_time,photos,appNames,qqNumbers,weixinNumbers"

    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val years = List("201705","201704","201703","201702","201701","201601","201602","201603","201604","201605","201606","201607","201608","201609","201610","201611","201612","2016ago")
//    for(year <- years){
    val json_rdd = sc.textFile("/datahouse/ods/mongo/mobile/mobile-"+date_etl+".json").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    val mobile = sqlContext.read.schema(schema).json(json_rdd)

    val mobile_select = mobile.select(
      regexp_replace(col("user_id"),"\\`","_").alias("user_id"),
      regexp_replace(col("addtime"),"\\`","_").alias("addtime"),
      regexp_replace(col("phone_name"),"\\`","_").alias("phone_name"),
      regexp_replace(col("phone_number"),"\\`","_").alias("phone_number"),
      regexp_replace(col("phone_os"),"\\`","_").alias("phone_os"),
      regexp_replace(col("phone_model"),"\\`","_").alias("phone_model"),
      regexp_replace(col("phone_ip"),"\\`","_").alias("phone_ip"),
      regexp_replace(col("phone_user_info"),"`","_").alias("phone_user_info"),
      regexp_replace(col("querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("device_id"),"\\`","_").alias("device_id"),
      regexp_replace(col("device_id_type"),"\\`","_").alias("device_id_type"),
      regexp_replace(col("mac"),"\\`","_").alias("mac"),
      regexp_replace(col("idfa"),"\\`","_").alias("idfa"),
      regexp_replace(col("imei"),"\\`","_").alias("imei"),
      regexp_replace(col("imsi"),"\\`","_").alias("imsi"),
      regexp_replace(col("add_product"),"\\`","_").alias("add_product"),
      regexp_replace(col("add_channel"),"\\`","_").alias("add_channel"),
      regexp_replace(col("networktype"),"\\`","_").alias("networktype"),
      regexp_replace(col("type"),"\\`","_").alias("type"),
      regexp_replace(col("ip"),"\\`","_").alias("ip"),
      regexp_replace(col("version"),"\\`","_").alias("version"),
      regexp_replace(col("phone_time"),"\\`","_").alias("phone_time"),
      regexp_replace(col("photos"),"\\`","_").alias("photos"),
      regexp_replace(col("appNames"),"\\`","_").alias("appNames"),
      regexp_replace(col("qqNumbers"),"\\`","_").alias("qqNumbers"),
      regexp_replace(col("weixinNumbers"),"\\`","_").alias("weixinNumbers"))

    val expr = concat_ws("`", mobile_select.columns.map(col): _*)
//
    if(fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/mobile_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/mobile_tmp/"),true)

    mobile_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/mobile_tmp/")
//      mobile_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/mobile_deal/mobile-"+year)
//      println("year:"+year+" is done========================================================================================================================")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/mobile_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/mobile_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/mobile_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/mobile_tmp/part-00000.snappy"))

//    }
  }
}
