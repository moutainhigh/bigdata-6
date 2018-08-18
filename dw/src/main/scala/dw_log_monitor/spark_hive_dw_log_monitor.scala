package dw_log_monitor

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class spark_hive_biz_dw_log_monitor_user_simple_sms --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_biz_dw_log_monitor_user_simple_sms.jar
  */
object spark_hive_dw_log_monitor {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("please give the correct params")
      System.exit(1)
    }

    var date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy/MM/dd")
    //参数:mode date  ==> mode=1 && date=201707  代表跑以前历史数据 ; mode=2 && date=1 代表每天定时跑 date=1代表跑上一天数据  什么都不传 默认跑上一天数据
    if (args.length == 2 && args(0).toString == "1") {
      date_etl = args(1).toString
    } else if (args.length == 2 && args(0).toString == "2") {
      date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-args(1).toInt).toString("yyyy/MM/dd")
    }

    println("dw_log_monitor=========================================================date_etl:" + date_etl)


    val job_submittedToJson = (originText: String) => {
      var result = ("")
      try {
        val jsonData = JSON.parseObject(originText)
        val type_string = jsonData.getString("type")
        val event = JSON.parseObject(jsonData.getString("event"))

        val data = new JSONObject();
        if (type_string == "JOB_SUBMITTED") {
          val jobSubmitted = JSON.parseObject(event.getString("org.apache.hadoop.mapreduce.jobhistory.JobSubmitted"))
          data.put("query_id", jobSubmitted.getString("jobid"))
          data.put("user_id", jobSubmitted.getString("jobQueueName"))
          data.put("submit_time", jobSubmitted.getString("submitTime"))
          data.put("query_string", JSON.parseObject(jobSubmitted.getString("workflowName")).getString("string"))
        }

        // 返回
        result = (data.toJSONString)
      } catch {
        case _: Throwable =>
      }
      result
    }

    val job_finishedToJson = (originText: String) => {
      var result = ("")
      try {
        val jsonData = JSON.parseObject(originText)
        val type_string = jsonData.getString("type")
        val event = JSON.parseObject(jsonData.getString("event"))

        val data = new JSONObject();
        if (type_string == "JOB_FINISHED") {
          val jobSubmitted = JSON.parseObject(event.getString("org.apache.hadoop.mapreduce.jobhistory.JobFinished"))
          data.put("jobid", jobSubmitted.getString("jobid"))
          data.put("finishTime", jobSubmitted.getString("finishTime"))
        }

        // 返回
        result = (data.toJSONString)
      } catch {
        case _: Throwable =>
      }
      result
    }

    val calc_cost_time: (String, String) => Double = (submit_time: String, finished_time: String) => {
      (finished_time.toDouble - submit_time.toDouble) / 1000
    }
    val sqlfunc_calc_cost_time = udf(calc_cost_time)

    val conf = new SparkConf().setAppName("spark_hive_dw_log_monitor")
    //.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val table_schema_string = "dw_log_monitor:user_id,query_id,query_string,submit_time,cost_time,finishTime"

    val table_name = table_schema_string.split(":")(0)
    val schemaString = table_schema_string.split(":")(1)
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val json_rdd = sc.textFile("/user/history/done/" + date_etl + "/*/*.jhist").map(x => x.replaceAll("\\\\r\\\\n", " ").replaceAll("\\\\n", " ").replaceAll("\\\\r", " "))
    //    val json_rdd = sc.textFile("D:\\software\\ql_etl\\data\\*.jhist").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))


    val job_submitted = sqlContext.read.json(json_rdd.filter(x => x.contains("\"type\":\"JOB_SUBMITTED\"")).map(x => job_submittedToJson(x)))

    println("job_submitted 数据量" + job_submitted.count())
    val job_finished = sqlContext.read.json(json_rdd.filter(x => x.contains("\"type\":\"JOB_FINISHED\"")).map(x => job_finishedToJson(x)))
    println("开始join" + job_submitted.schema.toList.toString())
    val dw_log_monitor_select = job_submitted.join(job_finished, job_submitted("query_id") === job_finished("jobid"))
      .withColumn("cost_time", sqlfunc_calc_cost_time(col("submit_time"), col("finishTime")))
      .select(schemaString.split(",").map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)

    val expr = concat_ws("`", dw_log_monitor_select.columns.map(col): _*)

    if (fs.exists(new org.apache.hadoop.fs.Path("/tmp/" + table_name + "_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/" + table_name + "_tmp/"), true)

    dw_log_monitor_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/" + table_name + "_tmp/")

    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/" + table_name + "_tmp/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/" + table_name + "_tmp/part-00000.snappy"), true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/" + table_name + "_tmp/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/" + table_name + "_tmp/part-00000.snappy"))

    println("table has done:" + table_name + " date:" + date_etl)
    sc.stop()
  }

}
