package zfs

/**
  * Created by zhangfusheng on 2017/9/12.
  */

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  *
  * spark-submit --class spark_ocr_identify --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar /data2/zfs/ql_etl.jar 2 1
  */

object spark_ocr_identify {
  def main(args: Array[String]) {
    if (args.length<1){
      System.err.println("please give the correct params")
      System.exit(1)
    }



    val conf = new SparkConf().setAppName("spark_ocr_identify")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    //val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    var date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    //参数:mode date  ==> mode=1 && date=2017-08-09  代表跑以前历史数据 ; mode=2 && date=1 代表每天定时跑 date=1代表跑上一天数据  什么都不传 默认跑上一天数据
    if (args.length==2 && args(0).toString=="1")//跑历史数据
    {
      date_etl = args(1).toString
    }else if(args.length==2 && args(0).toString=="2"){
      date_etl=DateTime.now().withTimeAtStartOfDay().plusDays(-args(1).toInt).toString("yyyy-MM-dd")
    }
    println("=========================================================date_etl:"+date_etl)



    val originIdCardToJson = (originText:String) => {
      var result = ("")
      try{
        val jsonData = JSON.parseObject(originText)
        val properties = JSON.parseObject(jsonData.getString("properties"))
        val userId = properties.getString("userId")
        val transerialsId = properties.getString("transerialsId")
        val askTime = properties.getString("askTime")
        val askReturnTime = properties.getString("askReturnTime")

        val data = JSON.parseObject(jsonData.getString("data"))
        val status = data.getString("status")
        val error = data.getString("error")
        val msg = data.getString("msg")

        val data1 = JSON.parseObject(data.getString("data"))
        var dtype, validity, issueAuthority, address, birthday, idNumber, name, sex, people=""

        if (data1 != null) {
          dtype = data1.getString("type")
          validity = data1.getString("validity")
          issueAuthority = data1.getString("issueAuthority")
          address = data1.getString("address")
          birthday = data1.getString("birthday")
          idNumber = data1.getString("idNumber")
          name = data1.getString("name")
          people = data1.getString("people")
          sex = data1.getString("sex")
        }
        else {
          dtype = null
          validity = null
          issueAuthority = null
          address = null
          birthday = null
          idNumber = null
          name = null
          people = null
          sex = null
        }


        val accounts_data = new JSONObject()
        accounts_data.put("userId", userId)
        accounts_data.put("transerialsId", transerialsId)
        accounts_data.put("askTime", askTime)
        accounts_data.put("askReturnTime", askReturnTime)
        accounts_data.put("status", status)
        accounts_data.put("error", error)
        accounts_data.put("msg", msg)
        accounts_data.put("dtype", dtype)
        accounts_data.put("validity", validity)
        accounts_data.put("issueAuthority", issueAuthority)
        accounts_data.put("address", address)
        accounts_data.put("birthday", birthday)
        accounts_data.put("idNumber", idNumber)
        accounts_data.put("name", name)
        accounts_data.put("people", people)
        accounts_data.put("sex", sex)

        // 返回 (report,report_structure,report_extend,report_risk)

        result = (accounts_data.toJSONString);}
      catch {
        case ex: Exception => {
          println("error:" + ex.printStackTrace() + "orgintext==============>" + originText)
        }
      }
      result
    }
    val originBankCardToJson = (originText:String) => {
      var result = ("")
      try{
        val jsonData = JSON.parseObject(originText)
        val properties = JSON.parseObject(jsonData.getString("properties"))
        val userId = properties.getString("userId")
        val transerialsId = properties.getString("transerialsId")
        val askTime = properties.getString("askTime")
        val askReturnTime = properties.getString("askReturnTime")

        val data = JSON.parseObject(jsonData.getString("data"))
        val status = data.getString("status")
        val error = data.getString("error")
        val msg = data.getString("msg")

        val data1 = JSON.parseObject(data.getString("data"))
        var dtype, cardNumber, validate, issuer=""

        if (data1 != null) {
          dtype = data1.getString("type")
          cardNumber = data1.getString("cardNumber")
          validate = data1.getString("validate")
          issuer = data1.getString("issuer")

        }
        else {
          dtype = null
          cardNumber = null
          validate = null
          issuer = null
        }


        val accounts_data = new JSONObject()
        accounts_data.put("userId", userId)
        accounts_data.put("transerialsId", transerialsId)
        accounts_data.put("askTime", askTime)
        accounts_data.put("askReturnTime", askReturnTime)
        accounts_data.put("status", status)
        accounts_data.put("error", error)
        accounts_data.put("msg", msg)
        accounts_data.put("dtype", dtype)
        accounts_data.put("cardNumber", cardNumber)
        accounts_data.put("validate", validate)
        accounts_data.put("issuer", issuer)
        // 返回 (report,report_structure,report_extend,report_risk)

        result = (accounts_data.toJSONString);}
      catch {
        case ex: Exception => {
          println("error:" + ex.printStackTrace() + "orgintext==============>" + originText)
        }
      }
      result
    }
    //val json_rdd = sc.textFile("E:\\13.1505278800051.txt").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))

    val json_rdd = sc.textFile("/datahouse/ods/topic/mobanker_authadptation_biz_data/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    val spark_ocr_identify_data = sqlContext.read.json(json_rdd).where(col("name").contains("合合识别")).select("data","properties","name")
    val bankcard_data=spark_ocr_identify_data.where(col("name").contains("合合识别银行卡")).toJSON.map(x=>originBankCardToJson(x))
    val idcard_data=spark_ocr_identify_data.where(col("name").contains("合合识别身份证")).toJSON.map(x=>originIdCardToJson(x))

    val schemaString1 = "userId,cardNumber,validate,dtype,issuer,status,error,msg,transerialsId,askTime,askReturnTime"

    val schema1 =StructType(schemaString1.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val schemaString2 = "userId,transerialsId,askTime,askReturnTime,status,error,msg,dtype,validity,issueAuthority,address,birthday,idNumber,name,people,sex"

    val schema2 =StructType(schemaString2.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val bankcard_data_df = sqlContext.read.schema(schema1).json(bankcard_data)
    val idcard_data_df = sqlContext.read.schema(schema2).json(idcard_data)

    val bankcard_data_select = bankcard_data_df.select(
      regexp_replace(col("userId"),"\\`","_").alias("userId"),
      regexp_replace(col("cardNumber"),"\\`","_").alias("cardNumber"),
      regexp_replace(col("validate"),"\\`","_").alias("validate"),
      regexp_replace(col("dtype"),"\\`","_").alias("type"),
      regexp_replace(col("issuer"),"\\`","_").alias("issuer"),
      regexp_replace(col("status"),"\\`","_").alias("status"),
      regexp_replace(col("error"),"\\`","_").alias("error"),
      regexp_replace(col("msg"),"\\`","_").alias("msg"),
      regexp_replace(col("transerialsId"),"\\`","_").alias("transerialsId"),
      regexp_replace(col("askTime"),"\\`","_").alias("askTime"),
      regexp_replace(col("askReturnTime"),"\\`","_").alias("askReturnTime"))

    val idcard_data_select = idcard_data_df.select(
      regexp_replace(col("userId"),"\\`","_").alias("userId"),
      regexp_replace(col("dtype"),"\\`","_").alias("type"),
      regexp_replace(col("validity"),"\\`","_").alias("validity"),
      regexp_replace(col("issueAuthority"),"\\`","_").alias("issueAuthority"),
      regexp_replace(col("address"),"\\`","_").alias("address"),
      regexp_replace(col("birthday"),"\\`","_").alias("birthday"),
      regexp_replace(col("idNumber"),"\\`","_").alias("idNumber"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("people"),"\\`","_").alias("people"),
      regexp_replace(col("sex"),"\\`","_").alias("sex"),
      regexp_replace(col("status"),"\\`","_").alias("status"),
      regexp_replace(col("error"),"\\`","_").alias("error"),
      regexp_replace(col("msg"),"\\`","_").alias("msg"),
      regexp_replace(col("transerialsId"),"\\`","_").alias("transerialsId"),
      regexp_replace(col("askTime"),"\\`","_").alias("askTime"),
      regexp_replace(col("askReturnTime"),"\\`","_").alias("askReturnTime"))



    val expr = concat_ws("`", idcard_data_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/idcard_data_select_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/idcard_data_select_tmp/"),true)

    idcard_data_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/idcard_data_select_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_idcard_validity_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_idcard_validity_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/idcard_data_select_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_idcard_validity_tmp/part-00000.snappy"))

    val expr2 = concat_ws("`", bankcard_data_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/bankcard_data_select_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/bankcard_data_select_tmp/"),true)

    bankcard_data_select.na.fill("NULL").select(expr2).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/bankcard_data_select_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_bankcard_validity_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_bankcard_validity_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/bankcard_data_select_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_bankcard_validity_tmp/part-00000.snappy"))


    sc.stop()
  }

}

