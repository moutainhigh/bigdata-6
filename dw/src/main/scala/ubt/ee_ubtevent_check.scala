package ubt

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
 * Created by lilin on 2017/11/29.
 *
 * spark-submit --class ubt.ee_ubtevent_check --deploy-mode client --num-executors 5 --executor-memory 1g --executor-cores 2 --driver-memory 1g --master yarn /data2/lilin/ubt/spark_hive_ee_ubtevent.jar
 */
object ee_ubtevent_check {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ee_ubtevent_check") //.setMaster("local[2]")
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

    val schemaString = "domain,distinctId,timestamp,action,event,size"
    val columns = schemaString.split(",")
    val schema = StructType(columns.map(fieldName => StructField(fieldName, StringType, true)))

    //val textfile_rdd = sc.textFile("D:/0").map(x => x.replaceAll("\\\\r\\\\n", " ").replaceAll("\\\\n", " ").replaceAll("\\\\r", " "))
    val textfile_rdd = sc.textFile("/user/hive/warehouse/ee.db/ee_ubtevent/dt=" + date_etl + "/*").map(x => x.replaceAll("\\\\r\\\\n", " ").replaceAll("\\\\n", " ").replaceAll("\\\\r", " "))
    //println("=========================================================textfile_rdd:" + textfile_rdd.count())

    val ubtevent_rdd=textfile_rdd.map(x => x.split("\u0001", -1)).map(x=>Row(x(0),x(1),x(3),x(4),x(5),x.size.toString))
    val ubtevent_df = sqlContext.createDataFrame(ubtevent_rdd, schema).registerTempTable("dd")
    val ubtevent_table= sqlContext.sql("SELECT * FROM dd WHERE size != 76");
    ubtevent_table.show(100)

    sc.stop()

  }

}