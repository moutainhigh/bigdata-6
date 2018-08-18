package lxy

import java.text.SimpleDateFormat

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


object spark_hive_t_module_his {
  def main(args: Array[String]) {
    //起始日期
    var start: String = null
    if (args.length == 0) {
      start = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    } else if (args.length >= 1) {
      start = args(0)
    }
    print("start is " + start)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startTimeStamp = format.parse(start + " 00:00:00").getTime
    val endTimeStamp = format.parse(start + " 23:59:59").getTime
    //查询指定时间段的数据
    val conf = new SparkConf().setAppName("t_module_his")

    //addProduct in (xczx-helin,sjd) and addProductType in (xczx-helin,sjd-gxd)
    val query =
      """{
                    "query": {
                      "range": {
                        "updateTime": {
                          "gte":  %d,
                          "lte":  %d
                        }
                      }
                    }
                  }""".format(startTimeStamp, endTimeStamp)
    conf.set("es.nodes", "10.15.185.236")
    conf.set("es.port", "9200")
    conf.set("es.query", query)
    conf.set("es.resource", "rca-t_module_his/t_module_his")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val schemaString = "user_id,borrow_nid,fund_name,is_fund_side_qualified,engine_input_str,cal_time,module_name,module_field_name,rule_engine_name,from_source,product_type,model_file,engine_output_str,create_time,update_time"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val t_feature_his = sc.esRDD("rca-t_module_his/t_module_his").values


    val json_rdd1 = t_feature_his.map(x => tfeaturehisToJson(x))
    val json_dataframe1 = sqlContext.read.schema(schema).json(json_rdd1).where("module_field_name='sjd_FundsRules'")

    val dataframe_query1 = json_dataframe1.select(json_dataframe1.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)

    //持久化bigdata_crawler_renthouse_info
    val expr1 = concat_ws("`", dataframe_query1.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/t_module_his/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/t_module_his/"), true)
    val rs1 = dataframe_query1.na.fill("NULL").select(expr1).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/t_module_his/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_t_module_his/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_t_module_his/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/t_module_his/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_t_module_his/part-00000.snappy"))
    sc.stop()

  }

  def tfeaturehisToJson(originMap: scala.collection.Map[String, AnyRef]) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val data = new JSONObject()
    try {
      val moduleInputStr = JSON.parseObject(originMap.get("moduleInputStr").getOrElse().toString)
      val engineOutputStrData = JSON.parseObject(originMap.get("engineOutputStr").getOrElse().toString).getJSONObject("data")

      //user_id,borrow_nid,fund_name,is_fund_side_qualified,add_time,engine_input_str,cal_time,module_name,module_field_name,rule_engine_name,from_source,product_type,model_file,engine_output_str,create_time,update_time
      data.put("user_id", moduleInputStr.getString("userId"))
      data.put("borrow_nid", moduleInputStr.getString("borrowNid"))
      data.put("fund_name", engineOutputStrData.getJSONArray("fund_name").toJSONString)
      data.put("is_fund_side_qualified", engineOutputStrData.getIntValue("is_fund_side_qualified"))
      data.put("engine_input_str", originMap.get("engineInputStr").getOrElse().toString)
      data.put("cal_time", Integer.parseInt(originMap.get("calTime").getOrElse().toString))
      data.put("module_name", originMap.get("moduleName").getOrElse().toString)
      data.put("module_field_name", originMap.get("moduleFieldName").getOrElse().toString)
      data.put("rule_engine_name", originMap.get("ruleEngineName").getOrElse().toString)
      data.put("from_source", originMap.get("fromSource").getOrElse().toString)
      data.put("product_type", originMap.get("productType").getOrElse().toString)
      data.put("model_file", moduleInputStr.getString("modelFile"))
      data.put("engine_output_str", originMap.get("engineOutputStr"))
      data.put("create_time", format.format(originMap.get("createTime").getOrElse()))
      data.put("update_time", format.format(originMap.get("updateTime").getOrElse()))
      //data.put("outstr", originMap.get("outStr").getOrElse().toString.replace("\"{", "{").replace("""}"""", "}").replace("\\","").replace("""}"""", "}"))
    } catch {
      case e: Exception => println("解析失败:" + e.getMessage)
    }
    data.toJSONString
  }


}
