package zhuoyi

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class spark_hive_biz_biz_zhuoyi_user_simple_sms --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_biz_biz_zhuoyi_user_simple_sms.jar
  */
object spark_hive_biz_zhuoyi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark_hive_biz_zhuoyi")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    println("=========================================================date_etl:"+date_etl)

    val schemaString = "user_id,imei,imsi,fromChannel,ac,record_time"

    val table_schema_string = Array("biz_zhuoyi_mobile_album:user_id,num,lbs_list,imei,imsi,fromChannel,ac,record_time",
    "biz_zhuoyi_mobile_app_default:user_id,apk_def_list,imei,imsi,fromChannel,ac,record_time",
    "biz_zhuoyi_mobile_app_his_info:user_id,app_name,apk,op,imei,imsi,fromChannel,ac,record_time",
    "biz_zhuoyi_mobile_app_info:user_id,app_name,apk,imei,imsi,fromChannel,ac,record_time",
    "biz_zhuoyi_mobile_app_use_info:user_id,apk_use_list,imei,imsi,fromChannel,ac,record_time,duration,lbs",
    "biz_zhuoyi_user_simple_sms:user_id,imei,imsi,fromChannel,ac,record_time")

    for(tss <-table_schema_string){
      val table_name = tss.split(":")(0)
      val schemaString = tss.split(":")(1)
      val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
      val json_rdd = sc.textFile("/datahouse/ods/topic/"+table_name+"/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
      val biz_zhuoyi = sqlContext.read.schema(schema).json(json_rdd)

      val biz_zhuoyi_select = biz_zhuoyi.select(biz_zhuoyi.columns.map(c => regexp_replace(col(c),"\\`","_").alias(c)):_*)

      val expr = concat_ws("`", biz_zhuoyi_select.columns.map(col): _*)

      if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"/")))
        fs.delete(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/"),true)

      biz_zhuoyi_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/"+table_name+"_tmp/")

      if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))){
        fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"),true)
      }

      fs.rename(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))

      println("table has done:"+table_name)
    }


    sc.stop()
  }

}
