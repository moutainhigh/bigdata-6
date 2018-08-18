package ubt

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
 * Created by lilin on 2017/11/29.
 *
 * spark-submit --class ubt.spark_hive_ee_ubtevent --jars /data2/lilin/fastjson-1.2.35.jar --deploy-mode client --num-executors 5 --executor-memory 1g --executor-cores 2 --driver-memory 1g --master yarn /data2/lilin/ubt/spark_hive_ee_ubtevent.jar
 */
object spark_hive_ee_ubtevent {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("spark_hive_ee_ubtevent") //.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    var date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    //参数:mode date  ==> mode=1 && date=2017-11-30  代表跑以前历史数据 ; mode=2 && date=1 代表每天定时跑 date=1代表跑上一天数据  什么都不传 默认跑上一天数据
    if (args.length == 2 && args(0).toString == "1") {
      date_etl = args(1).toString
    } else if (args.length == 2 && args(0).toString == "2") {
      date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-args(1).toInt).toString("yyyy-MM-dd")
    }
    println("=========================================================date_etl:" + date_etl)

    val schemaString = "domain,distinctId,originalId,timestamp,action,event,manufacturer,model,os,os_version,app_version,screen_width,screen_height,longitude,latitude,ip,province,city,district,market,map_type," +
      "to_page,credit_card_number,credit_bank_name,credit_amount,credit_validity,repay_remind_day,real_name,id_number,contact_relationship_1,contact_name_1,contact_phone_1,contact_relationship_2,contact_name_2,contact_phone_2," +
      "job,corporation,corporation_address,education,marry,debit_card_number,debit_bank_name,loan_amount,loan_days,loan_card_number,captcha,is_logout,phone_number,addr_detail,coupon_id,coupon_name,mobile,ubtPhone,ubtUserid," +
      "pageStayTime,productName,productSortValue,bannerUrl,bannerSortValue,templateName,bannerDesc,bannerCode,entranceTypeName,entranceTypeCode,deviceId,fromPage,platform," +
      "gold_coin_value,bank_name,account_type,account,bank_card_number,user_name,market_comment_from,content,promotionId"

    val columns = schemaString.split(",")

    val pairToJson = (key: String, iterable: Iterable[Row]) => {
      var result = ("")

      try {
        val rows = iterable.toArray

        var timestamp: Long = 0
        var event: String = null
        var index: Int = 0

        var timestamp1: Long = 0
        var event1: String = null
        var index1: Int = 0

        val ja: JSONArray = new JSONArray
        for (i <- 0 until rows.length) {
          val jo: JSONObject = new JSONObject
          for (j <- 0 until columns.length) {
            jo.put(columns(j), rows(i).getAs(columns(j)))
          }
          ja.add(jo)

          if ("goin".equals(jo.get("action")) || "open".equals(jo.get("action")) || "close".equals(jo.get("action"))
            || "signin".equals(jo.get("action")) || "signup".equals(jo.get("action"))) {
            if (event != null && timestamp > 0) {
              jo.put("fromPage", event)
              ja.get(index).asInstanceOf[JSONObject].put("to_page", jo.getString("event"))
              ja.get(index).asInstanceOf[JSONObject].put("pageStayTime", jo.getLong("timestamp") - timestamp)
            }

            event = jo.getString("event")
            timestamp = jo.getLong("timestamp")
            index = i
          }

          if("input".equals(jo.get("action"))){
            if (event1 != null && timestamp1 > 0 && !event1.equals(jo.getString("event"))) {
              ja.get(index1).asInstanceOf[JSONObject].put("pageStayTime", jo.getLong("timestamp") - timestamp1)
            }

            event1 = jo.getString("event")
            timestamp1 = jo.getLong("timestamp")
            index1 = i
          }

        }

        result = ja.toJSONString
      } catch {
        case ex: Exception => {
          println("error:" + ex.printStackTrace() + "key==============>" + key)
        }
      }

      result
    }

    val schema = StructType(columns.map(fieldName => StructField(fieldName, StringType, true)))

    //val textfile_rdd = sc.textFile("D:/0").map(x => x.replaceAll("\\\\r\\\\n", " ").replaceAll("\\\\n", " ").replaceAll("\\\\r", " "))
    val textfile_rdd = sc.textFile("/user/hive/warehouse/ee.db/ee_ubtevent/dt=" + date_etl + "/*").map(x => x.replaceAll("\\\\r\\\\n", " ").replaceAll("\\\\n", " ").replaceAll("\\\\r", " ")).filter(x => !x.trim.isEmpty)
    //println("=========================================================textfile_rdd:" + textfile_rdd.count())

    val ubtevent_rdd = textfile_rdd.map(x => x.split("\u0001", -1)).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20),
      x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40), x(41), x(42), x(43), x(44), x(45), x(46), x(47), x(48), x(49), x(50),
      x(51), x(52), x(53), x(54), x(55), x(56), x(57), x(58), x(59), x(60), x(61), x(62), x(63), x(64), x(65), x(66), x(67), x(68), x(69), x(70), x(71), x(72), x(73), x(74), x(75)
    ))

    val ubtevent_df = sqlContext.createDataFrame(ubtevent_rdd, schema).dropDuplicates(Seq("domain", "distinctId", "timestamp", "action", "event")).orderBy("domain", "distinctId", "timestamp")
    //ubtevent_df.printSchema()
    //ubtevent_df.show(100)

    val ubtevent_pair = ubtevent_df.map(row => ((row.getString(1) + row.getString(0), row))).groupByKey()
    //val json_rdd = ubtevent_pair.map(x=>pairToJson(x._1,x._2))
    val json_rdd = ubtevent_pair.mapPartitions(iter => iter.map(x => pairToJson(x._1, x._2)))
    val json_df = sqlContext.read.schema(schema).json(json_rdd)

    val expr = concat_ws("\u0001", json_df.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/tmp/ubt/ee_ubtevent_parquet_tmp/"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/ubt/ee_ubtevent_parquet_tmp/"), true)
    }
    json_df.select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/ubt/ee_ubtevent_parquet_tmp/")

    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/ee.db/ee_ubtevent_parquet_tmp/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/ee.db/ee_ubtevent_parquet_tmp/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/tmp/ubt/ee_ubtevent_parquet_tmp/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/ee.db/ee_ubtevent_parquet_tmp/part-00000.snappy"))

    sc.stop()

  }

}