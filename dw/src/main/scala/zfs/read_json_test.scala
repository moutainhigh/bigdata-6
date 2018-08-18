package zfs

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gongshaojie on 2017/6/15.
  * 启动spark-shell
  * su hdfs
  * spark-shell
  */
object read_json_test {
  def main(args: Array[String]) {
//    if (args.length<1){
//      System.err.println("please give the correct params")
//      System.exit(1)
//    }
//    val datapath =args(0).toString
//    val modelpath =args(1).toString
//    val cityorderpath =args(2).toString()

    val conf = new SparkConf().setAppName("read_json_test").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val jsonRdd = sc.textFile("D:\\software\\ql_etl\\data\\mobile")
    val customers = sqlContext.read.json("D:\\software\\ql_etl\\data\\mobile")
    val mobile=sqlContext.read.json(jsonRdd)
    mobile.printSchema()
    mobile.show()

    customers.select("_id.$oid","add_channel","add_product","addtime","appNames","device_id","device_id_type","idfa","imei","imsi","ip","mac","networktype","phone_ip","phone_model","phone_name","phone_number","phone_os","phone_time","photos","qqNumbers","type","user_id","version")
      .write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("dw_qlml.mobile")
    customers.printSchema()
    customers.show()
  }
}
