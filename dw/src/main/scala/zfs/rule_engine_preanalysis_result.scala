package zfs


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class rule_engine_preanalysis_result --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/zfs/ql_etl.jar
  */
object rule_engine_preanalysis_result {
  def main(args: Array[String]) {
    if (args.length<1){
      System.err.println("please give the correct params")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("rule_engine_preanalysis_result")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    //val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    var date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    //参数:mode date  ==> mode=1 && date=2017-08-09  代表跑以前历史数据 ; mode=2 && date=1 代表每天定时跑 date=1代表跑上一天数据  什么都不传 默认跑上一天数据
    if (args.length==2 && args(0).toString=="1")//跑历史数据
    {
      date_etl = args(1).toString
    }else if(args.length==2 && args(0).toString=="2"){
      date_etl=DateTime.now().withTimeAtStartOfDay().plusDays(-args(1).toInt).toString("yyyy-MM-dd")
    }
    println("=========================================================date_etl:"+date_etl)

    val schemaString = "taskId,taskTime,appName,appRequestId,inputParam,outputParam,ruleVersion,productType"

    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

   //val json_rdd = sc.textFile("E:\\testjson.txt").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))

    val json_rdd = sc.textFile("/datahouse/ods/topic/rule_engine_preanalysis_result/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    val rule_engine_preanalysis_result_data = sqlContext.read.json(json_rdd).select("data").map(x=>x.getString(0)).filter(x=>x!=null)

    val rule_engine_preanalysis_result = sqlContext.read.schema(schema).json(rule_engine_preanalysis_result_data)

    rule_engine_preanalysis_result.show(10)

    val rule_engine_preanalysis_result_select = rule_engine_preanalysis_result.select(
      regexp_replace(col("taskId"),"\\`","_").alias("taskId"),
      regexp_replace(col("taskTime"),"\\`","_").alias("updateTime"),
      regexp_replace(col("appName"),"\\`","_").alias("appName"),
      regexp_replace(col("appRequestId"),"\\`","_").alias("appRequestId"),
      regexp_replace(col("inputParam"),"\\`","_").alias("inputParam"),
      regexp_replace(col("outputParam"),"\\`","_").alias("outputParam"),
      regexp_replace(col("ruleVersion"),"\\`","_").alias("ruleVersion"),
      regexp_replace(col("productType"),"\\`","_").alias("productType"))

    rule_engine_preanalysis_result_select.show(10)

    val expr = concat_ws("`", rule_engine_preanalysis_result_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/rule_engine_preanalysis_result_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/rule_engine_preanalysis_result_tmp/"),true)

    rule_engine_preanalysis_result_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/rule_engine_preanalysis_result_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/es_rule.db/biz_rule_engine_preanalysis_result_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/es_rule.db/biz_rule_engine_preanalysis_result_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/rule_engine_preanalysis_result_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/es_rule.db/biz_rule_engine_preanalysis_result_tmp/part-00000.snappy"))

    sc.stop()
  }

}
