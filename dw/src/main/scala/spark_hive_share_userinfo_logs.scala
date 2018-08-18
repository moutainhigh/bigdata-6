import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/19.
  * sudo -u hive hadoop dfs -put share_userinfo_logs /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201705/
  *
  * sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201705/
  *
  * spark-submit --class spark_hive_share_userinfo_logs --deploy-mode client --num-executors 2 --executor-memory 2g --executor-cores 2 --driver-memory 5g --master yarn /data2/gsj/ql_etl_share_userinfo_logs.jar
  */
object spark_hive_share_userinfo_logs {
  def main(args: Array[String]) {
    //    if (args.length<1){
    //      System.err.println("please give the correct params")
    //      System.exit(1)
    //    }
    //
    //    val year =args(0).toString

    val conf = new SparkConf().setAppName("spark_hive_share_userinfo_logs")//.setMaster("local")
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

    val schemaString = "user_id,addtime,type,add_ip,productcode,channel,xin,old,status,fromChannel"

    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val years = List("201705","201704","201703","201702","201701","201601","201602","201603","201604","201605","201606","201607","201608","201609","201610","201611","201612","2016ago")
    //    for(year <- years){
    val json_rdd = sc.textFile("/datahouse/ods/topic/biz_microsite_grabshareuserinfologs/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
      .map(x=>originToJson(x))
    val share_userinfo_logs = sqlContext.read.schema(schema).json(json_rdd)

    val share_userinfo_logs_select = share_userinfo_logs.select(
      regexp_replace(col("user_id"),"\\`","_").alias("user_id"),
      regexp_replace(col("addtime"),"\\`","_").alias("addtime"),
      regexp_replace(col("type"),"\\`","_").alias("type"),
      regexp_replace(col("add_ip"),"\\`","_").alias("add_ip"),
      regexp_replace(col("productcode"),"\\`","_").alias("productcode"),
      regexp_replace(col("channel"),"\\`","_").alias("channel"),
      regexp_replace(col("xin"),"\\`","_").alias("xin_info"),
      regexp_replace(col("old"),"`","_").alias("old_info"),
      regexp_replace(col("status"),"\\`","_").alias("status"),
      regexp_replace(col("fromChannel"),"\\`","_").alias("fromChannel"))

    val expr = concat_ws("`", share_userinfo_logs_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/share_userinfo_logs_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/share_userinfo_logs_tmp/"),true)

    share_userinfo_logs_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/share_userinfo_logs_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/share_userinfo_logs_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/share_userinfo_logs_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/share_userinfo_logs_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/share_userinfo_logs_tmp/part-00000.snappy"))
    sc.stop()

    //      share_userinfo_logs_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-"+year)
    //      println("year:"+year+" is done==============================")
    //    }
  }
}
