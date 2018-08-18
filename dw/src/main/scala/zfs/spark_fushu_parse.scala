package zfs

/**
  * Created by zhangfusheng on 2017/9/7.
  */


import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  *
  * spark-submit --class spark_fushu_parse --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar  /data2/zfs/ql_etl.jar 2 1
  */
object spark_fushu_parse {
  def main(args: Array[String]) {
    if (args.length<1){
      System.err.println("please give the correct params")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("spark_fushu_parse")//.setMaster("local")
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

    //val schemaString = "taskId,taskTime,appName,appRequestId,inputParam,outputParam,ruleVersion,productType"

    //val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val schema = StructType(Seq(
      StructField("userId",StringType,true),
      StructField("createTime",StringType,true),
      StructField("updateTime",StringType,true),
      StructField("loginAccout",StringType,true),
      StructField("loginType",StringType,true),
        StructField("accounts",ArrayType(StructType(Seq(
          StructField("id",StringType,true),
          StructField("type",StringType,true),
          StructField("card",StringType,true),
          StructField("bank",StringType,true),
          StructField("holder",StringType,true),
          StructField("status",StringType,true),
          StructField("bill_day",StringType,true),
          StructField("repay_day",StringType,true),
          StructField("credit_limit",StringType,true),
          StructField("usable_limit",StringType,true)
        )))),
      StructField("bills",ArrayType(StructType(Seq(
        StructField("id",StringType,true),
        StructField("account_id",StringType,true),
        StructField("begin_date",StringType,true),
        StructField("end_date",StringType,true),
        StructField("bill_date",StringType,true),
        StructField("repay_date",StringType,true),
        StructField("payment",StringType,true),
        StructField("least_payment",StringType,true)
      )))),
      StructField("flows",ArrayType(StructType(Seq(
        StructField("account_id",StringType,true),
        StructField("bill_id",StringType,true),
        StructField("trade_time",StringType,true),
        StructField("settle_time",StringType,true),
        StructField("trade_amount",StringType,true),
        StructField("trade_amount_rmb",StringType,true),
        StructField("trade_currency",StringType,true),
        StructField("settle_amount",StringType,true),
        StructField("settle_amount_rmb",StringType,true),
        StructField("settle_currency",StringType,true),
        StructField("balance",StringType,true),
        StructField("account_no",StringType,true),
        StructField("trade_description",StringType,true),
        StructField("trade_nation",StringType,true),
        StructField("trade_place",StringType,true),
        StructField("trade_channel",StringType,true),
        StructField("oppesite_name",StringType,true),
        StructField("oppesite_bank",StringType,true),
        StructField("oppesite_account",StringType,true),
        StructField("summary",StringType,true),
        StructField("postscript",StringType,true)
      ))))

    ))


    val originToJson = (originText:String) => {
      var result = ("")
      try {
        val jsonData = JSON.parseObject(originText)
        val userId = jsonData.getString("userId")
        val createTime = jsonData.getString("createTime")
        val updateTime = jsonData.getString("updateTime")
        val loginAccout = jsonData.getString("loginAccout")
        val loginType = jsonData.getString("loginType")

        val responseData = JSON.parseObject(jsonData.getString("responseData"))
        val accounts = responseData.getJSONArray("accounts")
        val flows = responseData.getJSONArray("flows")
        val bills = responseData.getJSONArray("bills")


        val accounts_data = new JSONObject();
        accounts_data.put("userId",userId)
        accounts_data.put("createTime",createTime)
        accounts_data.put("updateTime",updateTime)
        accounts_data.put("loginAccout",loginAccout)
        accounts_data.put("loginType",loginType)
        accounts_data.put("accounts",accounts)
        accounts_data.put("bills",bills)
        accounts_data.put("flows",flows)

        // 返回 (report,report_structure,report_extend,report_risk)

        result = (accounts_data.toJSONString)
      } catch {
        case ex: Exception => {
          println("error:"+ex.printStackTrace()+"orgintext==============>"+originText)
        }
      }
      result
    }
    //val json_rdd = sc.textFile("E:\\fushu1.txt").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))

    val json_rdd = sc.textFile("/datahouse/ods/topic/star_auth_fushu_raw/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    val spark_fushu_parse_data = sqlContext.read.json(json_rdd).select("data").map(x=>x.getString(0)).filter(x=>x!=null).map(x=>originToJson(x))
    spark_fushu_parse_data.foreach(println)
    val spark_fushu_parse = sqlContext.read.schema(schema).json(spark_fushu_parse_data)


    val spark_fushu_parse_accounts = spark_fushu_parse.select(
      regexp_replace(col("userId"),"\\`","_").alias("userId"),
      regexp_replace(col("createTime"),"\\`","_").alias("createTime"),
      regexp_replace(col("updateTime"),"\\`","_").alias("updateTime"),
      regexp_replace(col("loginAccout"),"\\`","_").alias("loginAccout"),
      regexp_replace(col("loginType"),"\\`","_").alias("loginType"),
      explode(col("accounts")).as("accounts_collection")
    ).withColumn("type",lit("organization"))
      .select("userId","createTime","updateTime","loginAccout","loginType","accounts_collection.*")
      .select(
        regexp_replace(col("userId"),"\\`","_").alias("userId"),
        regexp_replace(col("createTime"),"\\`","_").alias("createTime"),
        regexp_replace(col("updateTime"),"\\`","_").alias("updateTime"),
        regexp_replace(col("loginAccout"),"\\`","_").alias("loginAccout"),
        regexp_replace(col("loginType"),"\\`","_").alias("loginType"),
        regexp_replace(col("id"),"\\`","_").alias("id"),
        regexp_replace(col("type"),"\\`","_").alias("type"),
        regexp_replace(col("card"),"\\`","_").alias("card"),
        regexp_replace(col("bank"),"\\`","_").alias("bank"),
        regexp_replace(col("holder"),"\\`","_").alias("holder"),
        regexp_replace(col("status"),"\\`","_").alias("status"),
        regexp_replace(col("bill_day"),"\\`","_").alias("bill_day"),
        regexp_replace(col("repay_day"),"\\`","_").alias("repay_day"),
        regexp_replace(col("credit_limit"),"\\`","_").alias("credit_limit"),
        regexp_replace(col("usable_limit"),"\\`","_").alias("usable_limit")
      )

    val spark_fushu_parse_bills = spark_fushu_parse.select(
      regexp_replace(col("userId"),"\\`","_").alias("userId"),
      regexp_replace(col("createTime"),"\\`","_").alias("createTime"),
      regexp_replace(col("updateTime"),"\\`","_").alias("updateTime"),
      regexp_replace(col("loginAccout"),"\\`","_").alias("loginAccout"),
      regexp_replace(col("loginType"),"\\`","_").alias("loginType"),
      explode(col("bills")).as("bills_collection")
    ).withColumn("type",lit("organization"))
      .select("userId","createTime","updateTime","loginAccout","loginType","bills_collection.*")
      .select(
        regexp_replace(col("userId"),"\\`","_").alias("userId"),
        regexp_replace(col("createTime"),"\\`","_").alias("createTime"),
        regexp_replace(col("updateTime"),"\\`","_").alias("updateTime"),
        regexp_replace(col("loginAccout"),"\\`","_").alias("loginAccout"),
        regexp_replace(col("loginType"),"\\`","_").alias("loginType"),
        regexp_replace(col("id"),"\\`","_").alias("id"),
        regexp_replace(col("account_id"),"\\`","_").alias("account_id"),
        regexp_replace(col("begin_date"),"\\`","_").alias("begin_date"),
        regexp_replace(col("end_date"),"\\`","_").alias("end_date"),
        regexp_replace(col("bill_date"),"\\`","_").alias("bill_date"),
        regexp_replace(col("repay_date"),"\\`","_").alias("repay_date"),
        regexp_replace(col("payment"),"\\`","_").alias("payment"),
        regexp_replace(col("least_payment"),"\\`","_").alias("least_payment")
      )

    val spark_fushu_parse_flows = spark_fushu_parse.select(
      regexp_replace(col("userId"),"\\`","_").alias("userId"),
      regexp_replace(col("createTime"),"\\`","_").alias("createTime"),
      regexp_replace(col("updateTime"),"\\`","_").alias("updateTime"),
      regexp_replace(col("loginAccout"),"\\`","_").alias("loginAccout"),
      regexp_replace(col("loginType"),"\\`","_").alias("loginType"),
      explode(col("flows")).as("flows_collection")
    ).withColumn("type",lit("organization"))
      .select("userId","createTime","updateTime","loginAccout","loginType","flows_collection.*")
      .select(
        regexp_replace(col("userId"),"\\`","_").alias("userId"),
        regexp_replace(col("createTime"),"\\`","_").alias("createTime"),
        regexp_replace(col("updateTime"),"\\`","_").alias("updateTime"),
        regexp_replace(col("loginAccout"),"\\`","_").alias("loginAccout"),
        regexp_replace(col("loginType"),"\\`","_").alias("loginType"),
        regexp_replace(col("account_id"),"\\`","_").alias("account_id"),
        regexp_replace(col("bill_id"),"\\`","_").alias("bill_id"),
        regexp_replace(col("trade_time"),"\\`","_").alias("trade_time"),
        regexp_replace(col("settle_time"),"\\`","_").alias("settle_time"),
        regexp_replace(col("trade_amount"),"\\`","_").alias("trade_amount"),
        regexp_replace(col("trade_amount_rmb"),"\\`","_").alias("trade_amount_rmb"),
        regexp_replace(col("trade_currency"),"\\`","_").alias("trade_currency"),
        regexp_replace(col("settle_amount"),"\\`","_").alias("settle_amount"),
        regexp_replace(col("settle_amount_rmb"),"\\`","_").alias("settle_amount_rmb"),
        regexp_replace(col("settle_currency"),"\\`","_").alias("settle_currency"),
        regexp_replace(col("balance"),"\\`","_").alias("balance"),
        regexp_replace(col("account_no"),"\\`","_").alias("account_no"),
        regexp_replace(col("trade_description"),"\\`","_").alias("trade_description"),
        regexp_replace(col("trade_nation"),"\\`","_").alias("trade_nation"),
        regexp_replace(col("trade_place"),"\\`","_").alias("trade_place"),
        regexp_replace(col("trade_channel"),"\\`","_").alias("trade_channel"),
        regexp_replace(col("oppesite_name"),"\\`","_").alias("oppesite_name"),
        regexp_replace(col("oppesite_bank"),"\\`","_").alias("oppesite_bank"),
        regexp_replace(col("oppesite_account"),"\\`","_").alias("oppesite_account"),
        regexp_replace(col("summary"),"\\`","_").alias("summary"),
        regexp_replace(col("postscript"),"\\`","_").alias("postscript")
      )

//    spark_fushu_parse_accounts.show(10)
//    spark_fushu_parse_bills.show(10)
//    spark_fushu_parse_flows.show(10)



//    val spark_fushu_parse_select = spark_fushu_parse.select(
//      regexp_replace(col("taskId"),"\\`","_").alias("taskId"),
//      regexp_replace(col("taskTime"),"\\`","_").alias("updateTime"),
//      regexp_replace(col("appName"),"\\`","_").alias("appName"),
//      regexp_replace(col("appRequestId"),"\\`","_").alias("appRequestId"),
//      regexp_replace(col("inputParam"),"\\`","_").alias("inputParam"),
//      regexp_replace(col("outputParam"),"\\`","_").alias("outputParam"),
//      regexp_replace(col("ruleVersion"),"\\`","_").alias("ruleVersion"),
//      regexp_replace(col("productType"),"\\`","_").alias("productType"))


// accounts
    val expr_accounts = concat_ws("`", spark_fushu_parse_accounts.columns.map(col): _*)
    //spark_fushu_parse_accounts.show(10)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_accounts_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_accounts_tmp/"),true)

    spark_fushu_parse_accounts.na.fill("NULL").select(expr_accounts).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/spark_fushu_accounts_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_accounts_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_accounts_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_accounts_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_accounts_tmp/part-00000.snappy"))

//bills
    val expr_bills = concat_ws("`", spark_fushu_parse_bills.columns.map(col): _*)
    //spark_fushu_parse_bills.show(10)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_bills_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_bills_tmp/"),true)

    spark_fushu_parse_bills.na.fill("NULL").select(expr_bills).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/spark_fushu_bills_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_bills_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_bills_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_bills_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_bills_tmp/part-00000.snappy"))

    //flows
    val expr_flows = concat_ws("`", spark_fushu_parse_flows.columns.map(col): _*)
    //spark_fushu_parse_flows.show(10)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_flows_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_flows_tmp/"),true)

    spark_fushu_parse_flows.na.fill("NULL").select(expr_flows).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/spark_fushu_flows_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_flows_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_flows_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/spark_fushu_flows_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/rca.db/yyd_rca_fushu_flows_tmp/part-00000.snappy"))

    sc.stop()
  }

}
