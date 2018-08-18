package mongo_2_data_migrate

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * 该清洗逻辑每天全量清洗
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class mongo_2_data_migrate.spark_hive_xinshen_approve_full_log --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl.jar
  */
object spark_hive_xinshen_approve_full_log {
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

    println("xinshen_approve_full_log=========================================================date_etl:"+date_etl)

    val conf = new SparkConf().setAppName("spark_hive_xinshen_approve_full_log")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val stringToJson = (originText:String) => {
      var result = ("")
      try {
        val jsonData = JSON.parseObject(originText)
        val approve_note = JSON.parseObject(jsonData.getString("approve_note"))
        val approve_special = JSON.parseObject(jsonData.getString("approve_special"))
        val approve_remark = JSON.parseObject(jsonData.getString("approve_remark"))
        val approve_conclusion = JSON.parseObject(jsonData.getString("approve_conclusion"))

        val data = new JSONObject();
        data.put("user_id", jsonData.getString("user_id"))
        data.put("borrow_nid", jsonData.getString("borrow_nid"))
        data.put("approve_info_check", jsonData.getString("approve_info_check"))
        data.put("approve_info_hash", jsonData.getString("approve_info_hash"))
        data.put("risk_code_log", jsonData.getString("risk_code_log"))

        data.put("approve_note_tv1", approve_note.getString("tv1"))
        data.put("approve_note_tv2", approve_note.getString("tv2"))
        data.put("approve_note_tv3", approve_note.getString("tv3"))
        data.put("approve_note_tv_describe", approve_note.getString("tv_describe"))
        data.put("approve_note_version", approve_note.getString("version"))


        data.put("approve_special_ai", approve_special.getString("ai"))
        data.put("approve_special_ae", approve_special.getString("ae"))
        data.put("approve_special_oc", approve_special.getString("oc"))
        data.put("approve_special_oh", approve_special.getString("oh"))
        data.put("approve_special_spc", approve_special.getString("spc"))
        data.put("approve_special_spc2", approve_special.getString("spc2"))
        data.put("approve_special_spc3", approve_special.getString("spc3"))
        data.put("approve_special_sy", approve_special.getString("sy"))
        data.put("approve_special_registered_capital", approve_special.getString("registered_capital"))
        data.put("approve_special_registered_time", approve_special.getString("registered_time"))
        data.put("approve_special_version", approve_special.getString("version"))


        data.put("approve_remark_remarks", approve_remark.getString("remarks"))
        data.put("approve_remark_personal_remarks", approve_remark.getString("personal_remarks"))
        data.put("approve_remark_version", approve_remark.getString("version"))


        data.put("approve_conclusion_conclusion", approve_conclusion.getString("conclusion"))
        data.put("approve_conclusion_money", approve_conclusion.getString("money"))
        data.put("approve_conclusion_reason_code1", approve_conclusion.getString("reason_code1"))
        data.put("approve_conclusion_reason_code2", approve_conclusion.getString("reason_code2"))
        data.put("approve_conclusion_reason_code3", approve_conclusion.getString("reason_code3"))
        data.put("approve_conclusion_version", approve_conclusion.getString("version"))
        data.put("approve_conclusion_amount", approve_conclusion.getString("amount"))
        data.put("approve_conclusion_equipment_matching", approve_conclusion.getString("equipment_matching"))
        data.put("approve_conclusion_stage", approve_conclusion.getString("stage"))
        data.put("approve_conclusion_latest_money", approve_conclusion.getString("latest_money"))

        // 返回
        result = (data.toJSONString)
      } catch {
        case _ : Throwable =>
      }
      result
    }

    val table_schema_string = "xinshen_approve_full_log:user_id,borrow_nid,approve_info_check,approve_info_hash,risk_code_log,approve_note_tv1,approve_note_tv2,approve_note_tv3,approve_note_tv_describe,approve_note_version,approve_special_ai,approve_special_ae,approve_special_oc,approve_special_oh,approve_special_spc,approve_special_spc2,approve_special_spc3,approve_special_sy,approve_special_registered_capital,approve_special_registered_time,approve_special_version,approve_remark_remarks,approve_remark_personal_remarks,approve_remark_version,approve_conclusion_conclusion,approve_conclusion_money,approve_conclusion_reason_code1,approve_conclusion_reason_code2,approve_conclusion_reason_code3,approve_conclusion_version,approve_conclusion_amount,approve_conclusion_equipment_matching,approve_conclusion_stage,approve_conclusion_latest_money"

    val table_name = table_schema_string.split(":")(0)
    val schemaString = table_schema_string.split(":")(1)
    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    ///datahouse/ods/mongo/xinshen_approve_full_log/xinshen_approve_full_log-201707.json
    val json_rdd = sc.textFile("/datahouse/ods/mongo/xinshen_approve_full_log/xinshen_approve_full_log-"+date_etl+".json").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
      .map(x=>stringToJson(x))
    val xinshen_approve_full_log = sqlContext.read.schema(schema).json(json_rdd)


    val xinshen_approve_full_log_select = xinshen_approve_full_log.select(xinshen_approve_full_log.columns.map(c => regexp_replace(col(c),"\\`","_").alias(c)):_*)


    val expr = concat_ws("`", xinshen_approve_full_log_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/"),true)

    xinshen_approve_full_log_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/"+table_name+"_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))

    println("table has done:"+table_name+" date:"+date_etl)
    sc.stop()
  }

}
