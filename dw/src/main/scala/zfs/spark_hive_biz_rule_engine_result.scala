package zfs

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class spark_hive_biz_rule_engine_result --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_biz_rule_engine_result.jar
  */
object spark_hive_biz_rule_engine_result {
  def main(args: Array[String]) {
    //起始日期
    var date_etl: String = null
    if (args.length == 0) {
      date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    } else if (args.length >= 1) {
      date_etl = args(0)
    }
    print("start is " + date_etl)


    val conf = new SparkConf().setAppName("spark_hive_biz_rule_engine_result").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

  //  val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    println("=========================================================date_etl:"+date_etl)

    val schemaString = "taskId,taskTime,productType,appName,appRequestId,inputParam,outputParam,ruleVersion"

    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val json_rdd = sc.textFile("/datahouse/ods/topic/rule_engine_result/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    val rule_engine_result_data = sqlContext.read.json(json_rdd).select("data").map(x=>x.getString(0)).filter(x=>x!=null)
    val rule_engine_result = sqlContext.read.schema(schema).json(rule_engine_result_data)
    val rule_engine_result_select = rule_engine_result.select(
      regexp_replace(col("taskId"),"\\`","_").alias("taskId"),
      regexp_replace(col("taskTime"),"\\`","_").alias("taskTime"),
      regexp_replace(col("productType"),"\\`","_").alias("productType"),
      regexp_replace(col("appName"),"\\`","_").alias("appName"),
      regexp_replace(col("appRequestId"),"\\`","_").alias("appRequestId"),
      regexp_replace(col("inputParam"),"\\`","_").alias("inputParam"),
      regexp_replace(col("outputParam"),"\\`","_").alias("outputParam"),
      regexp_replace(col("ruleVersion"),"\\`","_").alias("ruleVersion"))

    val expr = concat_ws("`", rule_engine_result_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/rule_engine_result_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/rule_engine_result_tmp/"),true)

    rule_engine_result_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/rule_engine_result_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/es_rule.db/biz_rule_engine_result_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/es_rule.db/biz_rule_engine_result_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/rule_engine_result_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/es_rule.db/biz_rule_engine_result_tmp/part-00000.snappy"))

    sc.stop()
  }

}
