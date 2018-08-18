import java.util.regex.{Matcher, Pattern}

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/19.
  * sudo -u hive hadoop dfs -put mobile /user/hive/warehouse/dw_qlml.db/mobile201705/
  *
  * sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/mobile_deal/mobile-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/mobile201705/
  *
  * spark-submit --class spark_hive_ncontacts --deploy-mode client --num-executors 2 --executor-memory 2g --executor-cores 2 --driver-memory 5g --master yarn /data2/gsj/ql_etl_ncontacts.jar
  */
object spark_hive_ncontacts {
  def main(args: Array[String]) {
//    if (args.length<1){
//      System.err.println("please give the correct params")
//      System.exit(1)
//    }
//
    val date_etl =args(0).toString
    println("=========================================================year:"+date_etl)

    val conf = new SparkConf().setAppName("spark_hive_ncontacts")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

//    val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
//    println("=========================================================date_etl:"+date_etl)


    val phoneType: (String => String) = (str: String) => {
      var p1: Pattern = null
      var p2: Pattern = null
      var m: Matcher = null
      var phone: Boolean = false
      var mobile: Boolean = false
      p1 = Pattern.compile("^[0][1-9]{2,3}[0-9]{5,10}$")
      p2 = Pattern.compile("^[1-9]{1}[0-9]{5,8}$")
      if(str!=null){
        if (str.length > 9) {
          m = p1.matcher(str)
          phone = m.matches
        }
        else {
          m = p2.matcher(str)
          phone = m.matches
        }


        var str_replace = str.replaceAll("-", "").replace(" ", "")
        val p1_m: Pattern = Pattern.compile("^((\\+{0,1}86|17951|12593){0,1})1[0-9]{10}")

        val m1: Matcher = p1_m.matcher(str_replace)
        if (m1.matches) {
          if (!"0".equals( str_replace.substring(1, 2))) {
            mobile = true
          }
          else {
            mobile = false
          }
        }
        else {
          mobile = false
        }
      }


      if(phone==true && mobile==false)
         "phone"
      else if(phone==false && mobile==true)
         "mobile"
      else
         "other"
    }
    val sqlfunc_phoneType = udf(phoneType)

    /**
      *
      */
    val trimTelNum: (String => String) = (telNum: String) => {
      val IPPFXS4: Array[String] = Array("1790", "1791", "1793", "1795", "1796", "1797", "1799")
      val IPPFXS5: Array[String] = Array("12583", "12593", "12589", "12520", "10193", "11808")
      val IPPFXS6: Array[String] = Array("118321")
      val substring=(s:String, from:Integer) => {
        try {
           s.substring(from)
        } catch {
          case e: Exception => {
             s
          }
        }
      }

      val substring3=(s:String, from:Integer , len:Integer )=> {
        try {
           s.substring(from, from + len)
        } catch {
          case e: Exception => {
             s
          }
        }
      }

      val inArray=(target: String, arr: Array[String]) => {
        if (arr == null || arr.length == 0) {
           false
        }
        if (target == null) {
           false
        }
        for (s <- arr) {
          if (target == s) {
             true
          }
        }
         false
      }


      var telNum_trim=telNum
      if (telNum_trim == null || "" == telNum_trim) {
        ""
      }
      try {
        val ippfx6: String = substring3(telNum_trim, 0, 6)
        val ippfx5: String = substring3(telNum_trim, 0, 5)
        val ippfx4: String = substring3(telNum_trim, 0, 4)
        if (telNum_trim.length > 7 && (substring3(telNum_trim, 5, 1) == "0" || substring3(telNum_trim, 5, 1) == "1" || substring3(telNum_trim, 5, 3) == "400" || substring3(telNum_trim, 5, 3) == "+86") && (inArray(ippfx5, IPPFXS5) || inArray(ippfx4, IPPFXS4)))
          telNum_trim = substring(telNum_trim, 5)
        else if (telNum_trim.length > 8 && (substring3(telNum_trim, 6, 1) == "0" || substring3(telNum_trim, 6, 1) == "1" || substring3(telNum_trim, 6, 3) == "400" || substring3(telNum_trim, 6, 3) == "+86") && inArray(ippfx6, IPPFXS6))
          telNum_trim = substring(telNum_trim, 6)
        telNum_trim = telNum_trim.replace("-", "")
        telNum_trim = telNum_trim.replace(" ", "")
        if (substring3(telNum_trim, 0, 4) == "0086") telNum_trim = substring(telNum_trim, 4)
        else if (substring3(telNum_trim, 0, 2) == "86") telNum_trim = substring(telNum_trim, 2)
        else if (substring3(telNum_trim, 0, 3) == "+86") telNum_trim = substring(telNum_trim, 3)
        else if (substring3(telNum_trim, 0, 5) == "00186") telNum_trim = substring(telNum_trim, 5)

        telNum_trim
      }
      catch {
        case e: Exception => {
          telNum_trim
        }
      }

    }
    val sqlfunc_trimTelNum = udf(trimTelNum)

    val originToJson = (originText:String) => {
      var result = ("")
      try {
        val jsonData = JSON.parseObject(originText)
        val data = jsonData.getString("data")
        result = (data)
      } catch {
        case ex: Exception => {
          println("error:"+ex.printStackTrace()+"orgintext==============>"+originText)
        }
      }
      result
    }



    val schemaString = "user_id,addtime,tel,name,querytime,device_id,device_id_type,mac,idfa,imei,deviceupdatetime,count"

    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val years = List("201705","201704","201703","201702","201701","201601","201602","201603","201604","201605","201606","201607","201608","201609","201610","201611","201612","2016ago")

//    for (year <- years){
    val json_rdd = sc.textFile("/datahouse/ods/topic/biz_microsite_grabcontacts/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    .map(x=>originToJson(x))

    val mobile = sqlContext.read.schema(schema).json(json_rdd)

    val mobile_select = mobile.select(
      regexp_replace(col("user_id"),"\\`","_").alias("user_id"),
      regexp_replace(col("addtime"),"\\`","_").alias("addtime"),
      regexp_replace(col("tel"),"\\`","_").alias("tel"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("device_id"),"\\`","_").alias("device_id"),
      regexp_replace(col("device_id_type"),"\\`","_").alias("device_id_type"),
      regexp_replace(col("mac"),"\\`","_").alias("mac"),
      regexp_replace(col("idfa"),"\\`","_").alias("idfa"),
      regexp_replace(col("imei"),"`","_").alias("imei"),
      regexp_replace(col("deviceupdatetime"),"\\`","_").alias("deviceupdatetime"),
      regexp_replace(col("count"),"`","_").alias("count"))
      .withColumn("tel_clean",sqlfunc_trimTelNum(col("tel")))
      .withColumn("phonetype",sqlfunc_phoneType(col("tel")))


    val expr = concat_ws("`", mobile_select.columns.map(col): _*)
//      mobile_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/ncontacts_deal/ncontacts-"+year)
//      println("year:"+year+" is done==============================")

    if(fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/ncontacts_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/ncontacts_tmp/"),true)

    mobile_select.na.fill("NULL").filter($"user_id"!=="NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/ncontacts_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/ncontacts_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/ncontacts_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/ncontacts_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/dw_qlml.db/ncontacts_tmp/part-00000.snappy"))
    sc.stop()


    //    }


  }
}
