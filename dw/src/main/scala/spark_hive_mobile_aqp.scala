import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/19.
  * sudo -u hive hadoop dfs -put mobile /user/hive/warehouse/dw_qlml.db/mobile201705/
  *
  * sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/mobile_deal/mobile-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/mobile201705/
  *
  * su hdfs
  * spark-submit --class spark_hive_mobile --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_mobile.jar
  */
object spark_hive_mobile_aqp {
  def main(args: Array[String]) {
//    if (args.length<1){
//      System.err.println("please give the correct params")
//      System.exit(1)
//    }
//
//    val year =args(0).toString
//    println("year===============================================:"+year)

    val conf = new SparkConf().setAppName("spark_hive_mobile_aqp")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    println("=========================================================date_etl:"+date_etl)


    val originToJson = (originText:String) => {
      var result = ("","","")
      try {

        val jsonData1 = JSON.parseObject(originText)
        val data_mobile = jsonData1.getString("data")


        val jsonData = JSON.parseObject(data_mobile)
        val user_id = jsonData.getString("user_id")
        val device_id = jsonData.getString("device_id")
        val addtime = jsonData.getString("addtime")

        val photos = jsonData.getString("photos")
        val qqnumbers = jsonData.getString("qqNumbers")
        val appnames = jsonData.getString("appNames")

        var appnames_list="[]"
        if (appnames!=null && appnames.startsWith("{")){
          appnames_list = "["
          val a_list = JSON.parseObject(appnames).values().toArray
          for(a<-a_list){
            appnames_list+="\""+a+"\","
          }
          appnames_list+="]"
          appnames_list = appnames_list.replace(",]","]")

        }else{
          appnames_list=appnames
        }

        val data_appnames = new JSONObject();
        data_appnames.put("user_id",user_id)
        data_appnames.put("device_id",device_id)
        data_appnames.put("type","appname")
        data_appnames.put("photo_name","")
        data_appnames.put("photo_number","")
        data_appnames.put("qqnumber","")
        data_appnames.put("appnames",JSON.parseArray(appnames_list))
        data_appnames.put("addtime",addtime)

        val data_qqnumbers = new JSONObject();
        data_qqnumbers.put("user_id",user_id)
        data_qqnumbers.put("device_id",device_id)
        data_qqnumbers.put("type","qqnumber")
        data_qqnumbers.put("photo_name","")
        data_qqnumbers.put("photo_number","")
        data_qqnumbers.put("qqnumbers",JSON.parseArray(qqnumbers))
        data_qqnumbers.put("appname","")
        data_qqnumbers.put("addtime",addtime)

        val data_photos = new JSONObject();
        data_photos.put("user_id",user_id)
        data_photos.put("device_id",device_id)
        data_photos.put("type","photo")
        data_photos.put("photos",JSON.parseArray(photos))
        data_photos.put("qqnumber","")
        data_photos.put("appname","")
        data_photos.put("addtime",addtime)


        result=(data_appnames.toJSONString,data_qqnumbers.toJSONString,data_photos.toJSONString)//appname,qqnumber,photo
      } catch {
        case ex: Exception => {
          println("error:"+ex.printStackTrace()+"orgintext==============>"+originText)
        }
      }
      result
    }

    //新增 ,pt,phone_brand,ram,rom,cpu,lcd,wifitime,sen,ac,fromChannel,record_time
    val appname_schemaString = "user_id,device_id,type,photo_name,photo_number,qqnumber,appnames,addtime"
    val appname_schema = StructType(appname_schemaString.split(",").map(fieldName => if(fieldName=="appnames" || fieldName=="qqnumbers") StructField(fieldName, ArrayType(StringType), true) else StructField(fieldName, StringType, true)))

    val qqnumber_schemaString = "user_id,device_id,type,photo_name,photo_number,qqnumbers,appname,addtime"
    val qqnumber_schema = StructType(qqnumber_schemaString.split(",").map(fieldName => if(fieldName=="appnames" || fieldName=="qqnumbers") StructField(fieldName, ArrayType(StringType), true) else StructField(fieldName, StringType, true)))

    val photo_schemaString = "user_id,device_id,type,photos,qqnumber,appname,addtime"
    val photo_schema = StructType(photo_schemaString.split(",").map(fieldName => if(fieldName=="photos") StructField(fieldName, ArrayType(MapType(StringType, StringType)), true) else StructField(fieldName, StringType, true)))


    val json_rdd = sc.textFile("/datahouse/ods/topic/biz_microsite_grabmobile/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
                      .map(x=>originToJson(x))

//    val json_rdd = sc.textFile("D:\\software\\ql_etl\\data\\test.json").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
//                          .map(x=>originToJson(x))
//    val json_rdd = sc.textFile("/datahouse/ods/mongo/mobile/mobile-"+year+".json").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
//      .map(x=>originToJson(x))

    val appnames = sqlContext.read.schema(appname_schema).json(json_rdd.map(x=>x._1)).withColumn("appname",explode(col("appnames"))).drop("appnames")
    val qqnumbers = sqlContext.read.schema(qqnumber_schema).json(json_rdd.map(x=>x._2)).withColumn("qqnumber",explode(col("qqnumbers"))).drop("qqnumbers")
    //.select(explode(col("photos"))).select(explode(col("col")))
    val photos = sqlContext.read.schema(photo_schema).json(json_rdd.map(x=>x._3)).withColumn("photo",explode(when(col("photos").isNotNull, col("photos")).otherwise(array(lit(null).cast(MapType(StringType, StringType)))))).drop("photos")
      .select(col("user_id"),col("device_id"),col("type"),col("qqnumber"),col("appname"),col("addtime"),explode( when(col("photo").isNull, (lit(null))).otherwise(col("photo")) ).as(Seq("photo_name","photo_number"))).drop("photo")

    val ss = "user_id,device_id,type,photo_name,photo_number,qqnumber,appname,addtime"
    val mobile_select = appnames.select(ss.split(",").map(c => regexp_replace(col(c),"\\`","_").alias(c)):_*)
            .unionAll(qqnumbers.select(ss.split(",").map(c => regexp_replace(col(c),"\\`","_").alias(c)):_*)).unionAll(photos.select(ss.split(",").map(c => regexp_replace(col(c),"\\`","_").alias(c)):_*))

    val expr = concat_ws("`", mobile_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/mobile_appnames_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/mobile_appnames_tmp/"),true)

    mobile_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/mobile_appnames_tmp/")
//      mobile_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/mobile_deal/mobile-"+year)
//      println("year:"+year+" is done========================================================================================================================")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/mobile_appnames_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/mobile_appnames_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/mobile_appnames_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/mobile_appnames_tmp/part-00000.snappy"))

    sc.stop()


  }
}
