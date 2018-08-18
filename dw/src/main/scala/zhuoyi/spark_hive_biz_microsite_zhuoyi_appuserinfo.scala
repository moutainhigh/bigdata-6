package zhuoyi

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class spark_hive_biz_biz_microsite_zhuoyi_appuserinfo --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_biz_biz_microsite_zhuoyi_appuserinfo.jar
  */
object spark_hive_biz_microsite_zhuoyi_appuserinfo {
  def main(args: Array[String]) {
    if (args.length<1){
      System.err.println("please give the correct params")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("spark_hive_biz_microsite_zhuoyi_appuserinfo")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    var date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    //参数:mode date  ==> mode=1 && date=2017-08-09  代表跑以前历史数据 ; mode=2 && date=1 代表每天定时跑 date=1代表跑上一天数据  什么都不传 默认跑上一天数据
    if (args.length==2 && args(0).toString=="1")//跑历史数据
    {
      date_etl = args(1).toString
    }else if(args.length==2 && args(0).toString=="2"){
      date_etl=DateTime.now().withTimeAtStartOfDay().plusDays(-args(1).toInt).toString("yyyy-MM-dd")
    }
    println("=========================================================date_etl:"+date_etl)

    val originToJson = (originText:String) => {
      var result = ("")
      try {
        val jsonData = JSON.parseObject(originText)
        val data = jsonData.getString("data")
        result = (data)
      } catch {
        case ex: Exception => {
          println("error:"+ex.printStackTrace()+"orgintext==============>"+originText)
        }
      }
      result
    }

    val schemaString = "user_id,apk_use_list,imei,imsi,fromChannel,ac,record_time,duration,lbs"

    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val json_rdd = sc.textFile("/datahouse/ods/topic/biz_microsite_zhuoyi_appuserinfo/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
      .map(x=>originToJson(x))
    val biz_microsite_zhuoyi_appuserinfo = sqlContext.read.schema(schema).json(json_rdd)

    val biz_microsite_zhuoyi_appuserinfo_select = biz_microsite_zhuoyi_appuserinfo.select(
      regexp_replace(col("user_id"),"\\`","_").alias("user_id"),
      regexp_replace(col("apk_use_list"),"\\`","_").alias("apk_use_list"),
      regexp_replace(col("imei"),"\\`","_").alias("imei"),
      regexp_replace(col("imsi"),"\\`","_").alias("imsi"),
      regexp_replace(col("fromChannel"),"\\`","_").alias("fromChannel"),
      regexp_replace(col("ac"),"\\`","_").alias("ac"),
      regexp_replace(col("record_time"),"\\`","_").alias("record_time"),
      regexp_replace(col("duration"),"\\`","_").alias("duration"),
      regexp_replace(col("lbs"),"\\`","_").alias("lbs"))

    val expr = concat_ws("`", biz_microsite_zhuoyi_appuserinfo_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/biz_microsite_zhuoyi_appuserinfo_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/biz_microsite_zhuoyi_appuserinfo_tmp/"),true)

    biz_microsite_zhuoyi_appuserinfo_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/biz_microsite_zhuoyi_appuserinfo_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/biz_microsite_zhuoyi_appuserinfo_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/biz_microsite_zhuoyi_appuserinfo_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/biz_microsite_zhuoyi_appuserinfo_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/biz_microsite_zhuoyi_appuserinfo_tmp/part-00000.snappy"))

    sc.stop()
  }

}
