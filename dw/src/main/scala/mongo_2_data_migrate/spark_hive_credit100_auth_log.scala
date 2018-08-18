package mongo_2_data_migrate

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class mongo_2_data_migrate.spark_hive_credit100_auth_log --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl.jar
  */
object spark_hive_credit100_auth_log {
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

    println("credit100_auth_log=========================================================date_etl:"+date_etl)

    val conf = new SparkConf().setAppName("spark_hive_credit100_auth_log")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val stringToJson = (originText:String) => {
      var result = ("")
      try {
        val jsonData = JSON.parseObject(originText)
        val credit100Eexecute = JSON.parseObject(jsonData.getString("credit100Eexecute"))
        val req = JSON.parseObject(jsonData.getString("req"))

        val data = new JSONObject();
        data.put("user_id", jsonData.getString("user_id"))
        data.put("borrow_nid", jsonData.getString("borrow_nid"))
        data.put("addtime", jsonData.getString("addtime"))
        data.put("validate", jsonData.getString("validate"))

        if (credit100Eexecute.containsKey("QianLong")){
          val qianlong = JSON.parseObject(credit100Eexecute.getString("QianLong"))
          data.put("credit100Eexecute_cons_m6_fzps_visits",qianlong.getString("cons_m6_fzps_visits"))
          data.put("credit100Eexecute_sl_id_a1_degree",qianlong.getString("sl_id_a1_degree"))
          data.put("credit100Eexecute_cons_m12_tx_level",qianlong.getString("cons_m12_tx_level"))
          data.put("credit100Eexecute_cons_m12_tx_maxpay",qianlong.getString("cons_m12_tx_maxpay"))
          data.put("credit100Eexecute_ac_m1m3_credit_in",qianlong.getString("ac_m1m3_credit_in"))
          data.put("credit100Eexecute_cons_m12_ydhw_level",qianlong.getString("cons_m12_ydhw_level"))
          data.put("credit100Eexecute_cons_m3_fzps_visits",qianlong.getString("cons_m3_fzps_visits"))
          data.put("credit100Eexecute_stab_cell_num",qianlong.getString("stab_cell_num"))
          data.put("credit100Eexecute_cons_m12_tx_visits",qianlong.getString("cons_m12_tx_visits"))
          data.put("credit100Eexecute_cons_m12_n_avgpay",qianlong.getString("cons_m12_n_avgpay"))
          data.put("credit100Eexecute_cons_m3_whyl_visits",qianlong.getString("cons_m3_whyl_visits"))
          data.put("credit100Eexecute_cons_m3_n_avgpay",qianlong.getString("cons_m3_n_avgpay"))
          data.put("credit100Eexecute_max_cons_m6_numcate",qianlong.getString("max_cons_m6_numcate"))
          data.put("credit100Eexecute_cons_m12_cwsh_visits",qianlong.getString("cons_m12_cwsh_visits"))
          data.put("credit100Eexecute_cons_m3_tx_pay",qianlong.getString("cons_m3_tx_pay"))
          data.put("credit100Eexecute_max_cons_m3_pay",qianlong.getString("max_cons_m3_pay"))
          data.put("credit100Eexecute_cons_m6_ghhz_visits",qianlong.getString("cons_m6_ghhz_visits"))
          data.put("credit100Eexecute_ac_m1m3_debit_in",qianlong.getString("ac_m1m3_debit_in"))
          data.put("credit100Eexecute_max_cons_m3_numcate",qianlong.getString("max_cons_m3_numcate"))
          data.put("credit100Eexecute_sl_id_b1_degree",qianlong.getString("sl_id_b1_degree"))
          data.put("credit100Eexecute_ac_m4m6_debit_out",qianlong.getString("ac_m4m6_debit_out"))
          data.put("credit100Eexecute_cons_m3_wlyxxnwp_visits",qianlong.getString("cons_m3_wlyxxnwp_visits"))
          data.put("credit100Eexecute_sl_id_b2_degree",qianlong.getString("sl_id_b2_degree"))
          data.put("credit100Eexecute_is_cons_m3_jjqc_num",qianlong.getString("is_cons_m3_jjqc_num"))
          data.put("credit100Eexecute_cons_m12_fc_visits",qianlong.getString("cons_m12_fc_visits"))
          data.put("credit100Eexecute_sl_cell_b5_degree",qianlong.getString("sl_cell_b5_degree"))

        }

        data.put("credit100Eexecute_al_m6_id_notbank_orgnum", credit100Eexecute.getString("al_m6_id_notbank_orgnum"))
        data.put("credit100Eexecute_swift_number", credit100Eexecute.getString("swift_number"))
        data.put("credit100Eexecute_al_m6_cell_notbank_orgnum", credit100Eexecute.getString("al_m6_cell_notbank_orgnum"))
        data.put("credit100Eexecute_al_m12_id_notbank_orgnum", credit100Eexecute.getString("al_m12_id_notbank_orgnum"))
        data.put("credit100Eexecute_al_m6_id_notbank_allnum", credit100Eexecute.getString("al_m6_id_notbank_allnum"))
        data.put("credit100Eexecute_al_m3_cell_bank_allnum", credit100Eexecute.getString("al_m3_cell_bank_allnum"))
        data.put("credit100Eexecute_al_m3_cell_notbank_selfnum", credit100Eexecute.getString("al_m3_cell_notbank_selfnum"))
        data.put("credit100Eexecute_flag_rulespeciallist", credit100Eexecute.getString("flag_rulespeciallist"))
        data.put("credit100Eexecute_flag_specialList_c", credit100Eexecute.getString("flag_specialList_c"))
        data.put("credit100Eexecute_al_m12_cell_bank_selfnum", credit100Eexecute.getString("al_m12_cell_bank_selfnum"))
        data.put("credit100Eexecute_al_m12_id_notbank_allnum", credit100Eexecute.getString("al_m12_id_notbank_allnum"))
        data.put("credit100Eexecute_al_m12_id_bank_selfnum", credit100Eexecute.getString("al_m12_id_bank_selfnum"))
        data.put("credit100Eexecute_al_m3_cell_bank_orgnum", credit100Eexecute.getString("al_m3_cell_bank_orgnum"))
        data.put("credit100Eexecute_al_m6_id_bank_allnum", credit100Eexecute.getString("al_m6_id_bank_allnum"))
        data.put("credit100Eexecute_al_m6_id_bank_selfnum", credit100Eexecute.getString("al_m6_id_bank_selfnum"))
        data.put("credit100Eexecute_Rule_final_decision", credit100Eexecute.getString("Rule_final_decision"))
        data.put("credit100Eexecute_al_m6_cell_notbank_allnum", credit100Eexecute.getString("al_m6_cell_notbank_allnum"))
        data.put("credit100Eexecute_al_m3_id_bank_selfnum", credit100Eexecute.getString("al_m3_id_bank_selfnum"))
        data.put("credit100Eexecute_flag_ruleapplyloan", credit100Eexecute.getString("flag_ruleapplyloan"))
        data.put("credit100Eexecute_al_m6_cell_bank_selfnum", credit100Eexecute.getString("al_m6_cell_bank_selfnum"))
        data.put("credit100Eexecute_Rule_weight_QJF030", credit100Eexecute.getString("Rule_weight_QJF030"))
        data.put("credit100Eexecute_al_m3_id_notbank_selfnum", credit100Eexecute.getString("al_m3_id_notbank_selfnum"))
        data.put("credit100Eexecute_al_m6_cell_notbank_selfnum", credit100Eexecute.getString("al_m6_cell_notbank_selfnum"))
        data.put("credit100Eexecute_al_m12_id_bank_orgnum", credit100Eexecute.getString("al_m12_id_bank_orgnum"))
        data.put("credit100Eexecute_al_m12_id_bank_allnum", credit100Eexecute.getString("al_m12_id_bank_allnum"))
        data.put("credit100Eexecute_al_m12_cell_notbank_orgnum", credit100Eexecute.getString("al_m12_cell_notbank_orgnum"))
        data.put("credit100Eexecute_al_m12_id_notbank_selfnum", credit100Eexecute.getString("al_m12_id_notbank_selfnum"))
        data.put("credit100Eexecute_al_m6_cell_bank_allnum", credit100Eexecute.getString("al_m6_cell_bank_allnum"))
        data.put("credit100Eexecute_code", credit100Eexecute.getString("code"))
        data.put("credit100Eexecute_al_m3_cell_bank_selfnum", credit100Eexecute.getString("al_m3_cell_bank_selfnum"))
        data.put("credit100Eexecute_Rule_name_QJF030", credit100Eexecute.getString("Rule_name_QJF030"))
        data.put("credit100Eexecute_Rule_final_weight", credit100Eexecute.getString("Rule_final_weight"))
        data.put("credit100Eexecute_flag_applyLoan", credit100Eexecute.getString("flag_applyLoan"))
        data.put("credit100Eexecute_al_m3_cell_notbank_allnum", credit100Eexecute.getString("al_m3_cell_notbank_allnum"))
        data.put("credit100Eexecute_al_m6_cell_bank_orgnum", credit100Eexecute.getString("al_m6_cell_bank_orgnum"))
        data.put("credit100Eexecute_al_m3_id_bank_allnum", credit100Eexecute.getString("al_m3_id_bank_allnum"))
        data.put("credit100Eexecute_al_m6_id_notbank_selfnum", credit100Eexecute.getString("al_m6_id_notbank_selfnum"))
        data.put("credit100Eexecute_al_m3_cell_notbank_orgnum", credit100Eexecute.getString("al_m3_cell_notbank_orgnum"))
        data.put("credit100Eexecute_al_m12_cell_notbank_allnum", credit100Eexecute.getString("al_m12_cell_notbank_allnum"))
        data.put("credit100Eexecute_al_m12_cell_bank_allnum", credit100Eexecute.getString("al_m12_cell_bank_allnum"))
        data.put("credit100Eexecute_al_m12_cell_notbank_selfnum", credit100Eexecute.getString("al_m12_cell_notbank_selfnum"))
        data.put("credit100Eexecute_al_m12_cell_bank_orgnum", credit100Eexecute.getString("al_m12_cell_bank_orgnum"))
        data.put("credit100Eexecute_al_m3_id_bank_orgnum", credit100Eexecute.getString("al_m3_id_bank_orgnum"))
        data.put("credit100Eexecute_al_m3_id_notbank_allnum", credit100Eexecute.getString("al_m3_id_notbank_allnum"))
        data.put("credit100Eexecute_al_m6_id_bank_orgnum", credit100Eexecute.getString("al_m6_id_bank_orgnum"))
        data.put("credit100Eexecute_al_m3_id_notbank_orgnum", credit100Eexecute.getString("al_m3_id_notbank_orgnum"))

        data.put("credit100Eexecute_Flag", credit100Eexecute.getString("Flag"))
        data.put("credit100Eexecute_flag_media_c", credit100Eexecute.getString("flag_media_c"))
        data.put("credit100Eexecute_scorecust", credit100Eexecute.getString("scorecust"))
        data.put("credit100Eexecute_flag_score", credit100Eexecute.getString("flag_score"))
        data.put("credit100Eexecute_flag_consumption_c", credit100Eexecute.getString("flag_consumption_c"))
        data.put("credit100Eexecute_flag_stability_c", credit100Eexecute.getString("flag_stability_c"))

        data.put("req_borrowNid", req.getString("borrowNid"))
        data.put("req_cell", req.getString("cell"))
        data.put("req_bank_running_att_num", req.getString("bank_running_att_num"))
        data.put("req_apiType", req.getString("apiType"))
        data.put("req_id", req.getString("id"))
        data.put("req_qlModuleType", req.getString("qlModuleType"))
        data.put("req_refund_periods", req.getString("refund_periods"))
        data.put("req_income", req.getString("income"))
        data.put("req_age", req.getString("age"))
        data.put("req_app_visit_num", req.getString("app_visit_num"))
        data.put("req_name", req.getString("name"))
        data.put("req_tokenid", req.getString("tokenid"))
        data.put("req_edu_att_num", req.getString("edu_att_num"))
        data.put("req_qlMeal", req.getString("qlMeal"))
        data.put("req_userId", req.getString("userId"))
        data.put("req_gid", req.getString("gid"))

        // 返回
        result = (data.toJSONString)
      } catch {
        case _ : Throwable =>
      }
      result
    }

    val table_schema_string = "credit100_auth_log:user_id,borrow_nid,addtime,validate,credit100Eexecute_cons_m6_fzps_visits,credit100Eexecute_sl_id_a1_degree,credit100Eexecute_cons_m12_tx_level,credit100Eexecute_cons_m12_tx_maxpay,credit100Eexecute_ac_m1m3_credit_in,credit100Eexecute_cons_m12_ydhw_level,credit100Eexecute_cons_m3_fzps_visits,credit100Eexecute_stab_cell_num,credit100Eexecute_cons_m12_tx_visits,credit100Eexecute_cons_m12_n_avgpay,credit100Eexecute_cons_m3_whyl_visits,credit100Eexecute_cons_m3_n_avgpay,credit100Eexecute_max_cons_m6_numcate,credit100Eexecute_cons_m12_cwsh_visits,credit100Eexecute_cons_m3_tx_pay,credit100Eexecute_max_cons_m3_pay,credit100Eexecute_cons_m6_ghhz_visits,credit100Eexecute_ac_m1m3_debit_in,credit100Eexecute_max_cons_m3_numcate,credit100Eexecute_sl_id_b1_degree,credit100Eexecute_ac_m4m6_debit_out,credit100Eexecute_cons_m3_wlyxxnwp_visits,credit100Eexecute_sl_id_b2_degree,credit100Eexecute_is_cons_m3_jjqc_num,credit100Eexecute_cons_m12_fc_visits,credit100Eexecute_sl_cell_b5_degree,credit100Eexecute_al_m6_id_notbank_orgnum,credit100Eexecute_swift_number,credit100Eexecute_al_m6_cell_notbank_orgnum,credit100Eexecute_al_m12_id_notbank_orgnum,credit100Eexecute_al_m6_id_notbank_allnum,credit100Eexecute_al_m3_cell_bank_allnum,credit100Eexecute_al_m3_cell_notbank_selfnum,credit100Eexecute_flag_rulespeciallist,credit100Eexecute_flag_specialList_c,credit100Eexecute_al_m12_cell_bank_selfnum,credit100Eexecute_al_m12_id_notbank_allnum,credit100Eexecute_al_m12_id_bank_selfnum,credit100Eexecute_al_m3_cell_bank_orgnum,credit100Eexecute_al_m6_id_bank_allnum,credit100Eexecute_al_m6_id_bank_selfnum,credit100Eexecute_Rule_final_decision,credit100Eexecute_al_m6_cell_notbank_allnum,credit100Eexecute_al_m3_id_bank_selfnum,credit100Eexecute_flag_ruleapplyloan,credit100Eexecute_al_m6_cell_bank_selfnum,credit100Eexecute_Rule_weight_QJF030,credit100Eexecute_al_m3_id_notbank_selfnum,credit100Eexecute_al_m6_cell_notbank_selfnum,credit100Eexecute_al_m12_id_bank_orgnum,credit100Eexecute_al_m12_id_bank_allnum,credit100Eexecute_al_m12_cell_notbank_orgnum,credit100Eexecute_al_m12_id_notbank_selfnum,credit100Eexecute_al_m6_cell_bank_allnum,credit100Eexecute_code,credit100Eexecute_al_m3_cell_bank_selfnum,credit100Eexecute_Rule_name_QJF030,credit100Eexecute_Rule_final_weight,credit100Eexecute_flag_applyLoan,credit100Eexecute_al_m3_cell_notbank_allnum,credit100Eexecute_al_m6_cell_bank_orgnum,credit100Eexecute_al_m3_id_bank_allnum,credit100Eexecute_al_m6_id_notbank_selfnum,credit100Eexecute_al_m3_cell_notbank_orgnum,credit100Eexecute_al_m12_cell_notbank_allnum,credit100Eexecute_al_m12_cell_bank_allnum,credit100Eexecute_al_m12_cell_notbank_selfnum,credit100Eexecute_al_m12_cell_bank_orgnum,credit100Eexecute_al_m3_id_bank_orgnum,credit100Eexecute_al_m3_id_notbank_allnum,credit100Eexecute_al_m6_id_bank_orgnum,credit100Eexecute_al_m3_id_notbank_orgnum,credit100Eexecute_Flag,credit100Eexecute_flag_media_c,credit100Eexecute_scorecust,credit100Eexecute_flag_score,credit100Eexecute_flag_consumption_c,credit100Eexecute_flag_stability_c,req_borrowNid,req_cell,req_bank_running_att_num,req_apiType,req_id,req_qlModuleType,req_refund_periods,req_income,req_age,req_app_visit_num,req_name,req_tokenid,req_edu_att_num,req_qlMeal,req_userId,req_gid"

    val table_name = table_schema_string.split(":")(0)
    val schemaString = table_schema_string.split(":")(1)
    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    ///datahouse/ods/mongo/credit100_auth_log/credit100_auth_log-201707.json
    val json_rdd = sc.textFile("/datahouse/ods/mongo/credit100_auth_log/credit100_auth_log-"+date_etl+".json").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
      .map(x=>stringToJson(x))
    val credit100_auth_log = sqlContext.read.schema(schema).json(json_rdd)


    val credit100_auth_log_select = credit100_auth_log.select(credit100_auth_log.columns.map(c => regexp_replace(col(c),"\\`","_").alias(c)):_*)


    val expr = concat_ws("`", credit100_auth_log_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/"),true)

//    credit100_auth_log_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).take(10).foreach(println)
    credit100_auth_log_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/"+table_name+"_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))

    println("table has done:"+table_name+" date:"+date_etl)
    sc.stop()
  }

}
