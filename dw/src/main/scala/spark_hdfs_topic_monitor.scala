import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/19.
  * sudo -u hive hadoop dfs -put lbs /user/hive/warehouse/dw_qlml.db/lbs201705/
  *
  * sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201705/
  *
  * spark-submit --class spark_hive_lbs --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_lbs.jar
  */
object spark_hdfs_topic_monitor {
  def main(args: Array[String]) {
//    if (args.length<1){
//      System.err.println("please give the correct params")
//      System.exit(1)
//    }
//
//    val year =args(0).toString

    val conf = new SparkConf().setAppName("spark_hdfs_topic_monitor")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
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

    //新增字段 imsi,ac,lbs,province,city,record_time,fromChannel
    val schemaString = "user_id,openid,addtime,lat,lon,map,type,precision,device_id,device_id_type,ip,mac,idfa,imei,add_product,add_channel,imsi,ac,lbs,province,city,record_time,fromChannel"

    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val years = List("201705","201704","201703","201702","201701","201601","201602","201603","201604","201605","201606","201607","201608","201609","201610","201611","201612","2016ago")
//    for(year <- years){
    val json_rdd = sc.textFile("/datahouse/ods/topic/biz_microsite_grablbs/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    .map(x=>originToJson(x))
    val lbs = sqlContext.read.schema(schema).json(json_rdd)

    val lbs_select = lbs.select(
      regexp_replace(col("user_id"),"\\`","_").alias("user_id"),
      regexp_replace(col("openid"),"\\`","_").alias("openid"),
      regexp_replace(col("addtime"),"\\`","_").alias("addtime"),
      regexp_replace(col("lat"),"\\`","_").alias("lat"),
      regexp_replace(col("lon"),"\\`","_").alias("lon"),
      regexp_replace(col("map"),"\\`","_").alias("mapp"),
      regexp_replace(col("type"),"\\`","_").alias("type"),
      regexp_replace(col("precision"),"`","_").alias("precision"),
      regexp_replace(col("device_id"),"\\`","_").alias("device_id"),
      regexp_replace(col("device_id_type"),"\\`","_").alias("device_id_type"),
      regexp_replace(col("ip"),"\\`","_").alias("ip"),
      regexp_replace(col("mac"),"\\`","_").alias("mac"),
      regexp_replace(col("idfa"),"\\`","_").alias("idfa"),
      regexp_replace(col("imei"),"\\`","_").alias("imei"),
      regexp_replace(col("add_product"),"\\`","_").alias("add_product"),
      regexp_replace(col("add_channel"),"\\`","_").alias("add_channel"),
      regexp_replace(col("imsi"),"\\`","_").alias("imsi"),
      regexp_replace(col("ac"),"\\`","_").alias("ac"),
      regexp_replace(col("lbs"),"\\`","_").alias("lbs"),
      regexp_replace(col("province"),"\\`","_").alias("province"),
      regexp_replace(col("city"),"\\`","_").alias("city"),
      regexp_replace(col("record_time"),"\\`","_").alias("record_time"),
      regexp_replace(col("fromChannel"),"\\`","_").alias("fromChannel"))

    val expr = concat_ws("`", lbs_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/lbs_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/lbs_tmp/"),true)

    lbs_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/lbs_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/lbs_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/lbs_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/lbs_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/lbs_tmp/part-00000.snappy"))

    sc.stop()
//      lbs_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/lbs_deal/lbs-"+year)
//      println("year:"+year+" is done==============================")
//    }
  }
}
