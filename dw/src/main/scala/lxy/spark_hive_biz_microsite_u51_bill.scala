package lxy

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.joda.time.DateTime


/**
  * Created by liuxinyuan on 2018/1/15.
  * 落地表  tmp.tmp_t_feature_creditsource
  * su hdfs -c "spark-submit --class lxy.spark_hive_t_module_his --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar,/data2/lxy/jars/elasticsearch-hadoop-5.1.1.jar
  * --deploy-mode client --num-executors 3 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn  /data2/lxy/jars/es_t_feature_creditsource/es_t_feature_creditsource.jar"
  * 取数逻辑
  * addProduct in (xczx-helin,sjd) and addProductType in (xczx-helin,sjd-gxd)
  *
  * user_id,borrow_nid,fund_name,is_fund_side_qualified,add_time,engine_Input_Str,cal_Time,module_Name,module_Field_Name,rule_Engine_Name,from_Source,product_Type,module_Input_Str,engine_Output_Str,create_Time,update_Time
  *
  */


object spark_hive_biz_microsite_u51_bill {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("biz_microsite_u51_bill")
    //.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val schemaString = "user_id,product,merchant_id,mail_account,cash_amt_avg_lst6,cash_amt_avg_lst3,credit_limit_sum,credit_usage_all,cash_amt_sum,late_fees_sum,card_num,fees_sum,credit_limit_max,new_bal_sum,fq_month_cnt_lst3,fq_per_lst6,overdue_amt_lst3,fq_per_lst3,overdue_amt_lst6,overdue_month_cnt_lst6,is_autopay,new_bal_avg_lst6,overdue_month_cnt_lst3,limit_avail_sum,new_bal_avg_lst3,consume_cnt,overspend_cnt_avg_lst6,overspend_cnt,fraud_flag,fees_sum_lst6,fees_sum_lst3,bill_source,credit_usage_all_lst3,fq_month_cnt_lst6,is_fq,consume_cnt_avg_lst6,add_channel,update_time"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val json_rdd = sc.textFile("/datahouse/ods/mongo/biz_microsite_u51_bill/biz_microsite_u51_bill.json").map(x => x.replaceAll("\\\\r\\\\n", " ").replaceAll("\\\\n", " ").replaceAll("\\\\r", " "))
    val json_dataframe = sqlContext.read.schema(schema).json(json_rdd.map(x => stringToJson(x)))
    val dataframe_query = json_dataframe.select(json_dataframe.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)


    //持久化bigdata_crawler_renthouse_info
    val expr1 = concat_ws("`", dataframe_query.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/biz_microsite_u51_bill_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/biz_microsite_u51_bill_tmp/"), true)
    val rs1 = dataframe_query.na.fill("NULL").select(expr1).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/biz_microsite_u51_bill_tmp/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_biz_microsite_u51_bill/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_biz_microsite_u51_bill/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/biz_microsite_u51_bill_tmp/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_biz_microsite_u51_bill/part-00000.snappy"))
    sc.stop()
  }


  def stringToJson(text: String) = {
    val obj = new JSONObject()
    try {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jsonObject = JSON.parseObject(text)
      val bill = jsonObject.getJSONObject("bill")
      val billSummary = bill.getJSONObject("billSummary")
      //user_id,product,merchant_id,mail_account,cash_amt_avg_lst6,cash_amt_avg_lst3,credit_limit_sum,
      // credit_usage_all,cash_amt_sum,late_fees_sum,card_num,fees_sum,credit_limit_max,new_bal_sum,fq_month_cnt_lst3,
      // fq_per_lst6,overdue_amt_lst3,fq_per_lst3,overdue_amt_lst6,overdue_month_cnt_lst6,is_autopay,new_bal_avg_lst6,
      // overdue_month_cnt_lst3,limit_avail_sum,new_bal_avg_lst3,consume_cnt,overspend_cnt_avg_lst6,overspend_cnt,
      // fraud_flag,fees_sum_lst6,fees_sum_lst3,bill_source,credit_usage_all_lst3,fq_month_cnt_lst6,is_fq,consume_cnt_avg_lst6,
      // add_channel,update_time
      obj.put("user_id", jsonObject.getInteger("userId").toString)
      obj.put("product", jsonObject.getString("product"))
      obj.put("merchant_id", jsonObject.getString("merchantId"))
      obj.put("mail_account", bill.getString("mailaccount"))
      obj.put("cash_amt_avg_lst6", billSummary.getDoubleValue("cash_amt_avg_lst6"))
      obj.put("cash_amt_avg_lst3", billSummary.getDoubleValue("cash_amt_avg_lst3"))
      obj.put("credit_limit_sum", billSummary.getDoubleValue("credit_limit_sum"))
      obj.put("credit_usage_all", billSummary.getDoubleValue("credit_usage_all"))
      obj.put("cash_amt_sum", billSummary.getDoubleValue("cash_amt_sum"))
      obj.put("late_fees_sum", billSummary.getDoubleValue("late_fees_sum"))
      obj.put("card_num", billSummary.getIntValue("card_num"))
      obj.put("fees_sum", billSummary.getDoubleValue("fees_sum"))
      obj.put("credit_limit_max", billSummary.getDoubleValue("credit_limit_max"))
      obj.put("new_bal_sum", billSummary.getDoubleValue("new_bal_sum"))
      obj.put("fq_month_cnt_lst3", billSummary.getIntValue("fq_month_cnt_lst3"))
      obj.put("fq_per_lst6", billSummary.getDoubleValue("fq_per_lst6"))
      obj.put("overdue_amt_lst3", billSummary.getDoubleValue("overdue_amt_lst3"))
      obj.put("fq_per_lst3", billSummary.getDoubleValue("fq_per_lst3"))
      obj.put("overdue_amt_lst6", billSummary.getDoubleValue("overdue_amt_lst6"))
      obj.put("overdue_month_cnt_lst6", billSummary.getIntValue("overdue_month_cnt_lst6"))
      obj.put("is_autopay", billSummary.getString("is_autopay"))
      obj.put("new_bal_avg_lst6", billSummary.getDoubleValue("new_bal_avg_lst6"))
      obj.put("overdue_month_cnt_lst3", billSummary.getIntValue("overdue_month_cnt_lst3"))
      obj.put("limit_avail_sum", billSummary.getDoubleValue("limit_avail_sum"))
      obj.put("new_bal_avg_lst3", billSummary.getDoubleValue("new_bal_avg_lst3"))
      obj.put("consume_cnt", billSummary.getIntValue("consume_cnt"))
      obj.put("overspend_cnt_avg_lst6", billSummary.getDoubleValue("overspend_cnt_avg_lst6"))
      obj.put("overspend_cnt", billSummary.getIntValue("overspend_cnt"))
      obj.put("fraud_flag", billSummary.getString("fraud_flag"))
      obj.put("fees_sum_lst6", billSummary.getDoubleValue("fees_sum_lst6"))
      obj.put("fees_sum_lst3", billSummary.getDoubleValue("fees_sum_lst3"))
      obj.put("bill_source", billSummary.getString("billsource"))
      obj.put("credit_usage_all_lst3", billSummary.getDoubleValue("credit_usage_all_lst3"))
      obj.put("fq_month_cnt_lst6", billSummary.getIntValue("fq_month_cnt_lst6"))
      obj.put("is_fq", billSummary.getString("is_fq"))
      obj.put("consume_cnt_avg_lst6", billSummary.getDoubleValue("consume_cnt_avg_lst6"))
      obj.put("add_channel", jsonObject.getString("addChannel"))

      try {
        obj.put("update_time", format.format(new Date(jsonObject.getLongValue("create_update_timestamp"))))
      } catch {
        case e: Exception => println("解析日期失败:" + e.getMessage)
          obj.put("update_time", null)
      }

    } catch {
      case e: Exception => println("解析失败" + e.getMessage)
    }
    obj.toJSONString
  }

}
