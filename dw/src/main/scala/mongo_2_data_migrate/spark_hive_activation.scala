package mongo_2_data_migrate

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class spark_hive_biz_activation_user_simple_sms --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_biz_activation_user_simple_sms.jar
  */
object spark_hive_activation {
  def main(args: Array[String]) {
    if (args.length<1){
      System.err.println("please give the correct params")
      System.exit(1)
    }

    var date_etl=DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyyMMdd")
    //参数:mode date  ==> mode=1 && date=201707  代表跑以前历史数据 ; mode=2 && date=1 代表每天定时跑 date=1代表跑上一天数据  什么都不传 默认跑上一天数据
    if (args.length==2 && args(0).toString=="1")
    {
      date_etl = args(1).toString
    }else if(args.length==2 && args(0).toString=="2"){
      date_etl=DateTime.now().withTimeAtStartOfDay().plusDays(-args(1).toInt).toString("yyyyMMdd")
    }

    println("activation=========================================================date_etl:"+date_etl)

    val conf = new SparkConf().setAppName("spark_hive_activation")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val table_schema_string = "activation:device_id,add_channel,add_product,addtime,device_id_type,idfa,imei,ip,mac,phone_model,phone_number,phone_os,type"

    val table_name = table_schema_string.split(":")(0)
    val schemaString = table_schema_string.split(":")(1)
    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    ///datahouse/ods/mongo/activation/activation-201707.json
    val json_rdd = sc.textFile("/datahouse/ods/mongo/activation/activation-"+date_etl+".json").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    val activation = sqlContext.read.schema(schema).json(json_rdd)


    val activation_select = activation.select(activation.columns.map(c => regexp_replace(col(c),"\\`","_").alias(c)):_*)

    val expr = concat_ws("`", activation_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/"),true)

    activation_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/"+table_name+"_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))

    println("table has done:"+table_name+" date:"+date_etl)
    sc.stop()
  }

}
