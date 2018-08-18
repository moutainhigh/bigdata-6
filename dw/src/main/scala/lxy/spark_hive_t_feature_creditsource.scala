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
  * su hdfs -c "spark-submit --class lxy.spark_hive_es_featrue --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar,/data2/lxy/jars/elasticsearch-hadoop-5.1.1.jar
  * --deploy-mode client --num-executors 3 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn  /data2/lxy/jars/es_t_feature_creditsource/es_t_feature_creditsource.jar"
  * 取数逻辑
  * addProduct in (xczx-helin,sjd) and addProductType in (xczx-helin,sjd-gxd)
  */


object spark_hive_t_feature_creditsource {
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
    val conf = new SparkConf().setAppName("t_feature_creditsource")

    //addProduct in (xczx-helin,sjd) and addProductType in (xczx-helin,sjd-gxd)
    val query =
      """{
                    "query": {
                      "range": {
                        "createTime": {
                          "gte":  %d,
                          "lte":  %d
                        }
                      }
                    }
                  }""".format(startTimeStamp, endTimeStamp)
    conf.set("es.nodes", "10.15.185.236")
    conf.set("es.port", "9200")
    conf.set("es.query", query)
    conf.set("es.resource", "rca-t_feature_creditsource/t_feature_creditsource")


    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)


    val schemaString = "user_id,add_product,add_product_type,borrow_nid,id_card,phone_number,credit_main_name,credit_child_name,create_time,outstr,instr"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val t_feature_his = sc.esRDD("rca-t_feature_creditsource/t_feature_creditsource").values
    val json_rdd1 = t_feature_his.map(x => tfeaturehisToJson(x))
    val json_dataframe1 = sqlContext.read.schema(schema).json(json_rdd1).where("(add_product in ('xczx-helin','sjd') and add_product_type in ('xczx-helin','sjd-gxd')) or add_product = 'yhfq' ")
    val dataframe_query1 = json_dataframe1.select(json_dataframe1.columns.map(c => regexp_replace(col(c), "\\`", "_").alias(c)): _*)


    //持久化bigdata_crawler_renthouse_info
    val expr1 = concat_ws("`", dataframe_query1.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/t_feature_creditsource/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/t_feature_creditsource/"), true)
    val rs1 = dataframe_query1.na.fill("NULL").select(expr1).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/t_feature_creditsource/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_t_feature_creditsource/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_t_feature_creditsource/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/t_feature_creditsource/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_t_feature_creditsource/part-00000.snappy"))
    sc.stop()

  }

  def tfeaturehisToJson(originMap: scala.collection.Map[String, AnyRef]) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val data = new JSONObject()
    try {
      val inStr = JSON.parseObject(originMap.get("inStr").getOrElse().toString)
      //user_id,add_product,add_product_type,borrow_nid,id_card,phone_number,credit_main_name,credit_child_name,create_time
      data.put("user_id", inStr.getString("userId"))
      data.put("add_product", inStr.getString("addProduct"))
      data.put("add_product_type", inStr.getString("addProductType"))
      data.put("borrow_nid", inStr.getString("borrowNid"))
      data.put("id_card", inStr.getString("idCard"))
      data.put("phone_number", inStr.getString("phoneNumber"))
      data.put("credit_main_name", originMap.get("creditMainName").getOrElse().toString)
      data.put("credit_child_name", originMap.get("creditChildName").getOrElse().toString)
      data.put("create_time", format.format(originMap.get("createTime").getOrElse()))
      data.put("outstr", originMap.get("outStr").getOrElse().toString.replace("\"{", "{").replace("""}"""", "}").replace("\\", "").replace("""}"""", "}"))
      data.put("instr", originMap.get("inStr").getOrElse().toString.replace("\"{", "{").replace("""}"""", "}").replace("\\", "").replace("""}"""", "}"))

    } catch {
      case e: Exception => println("解析失败:" + e.getMessage)
    }
    data.toJSONString
  }


}
