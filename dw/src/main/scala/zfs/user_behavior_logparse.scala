package zfs

/**
  * Created by zhangfusheng on 2017/8/31.
  */


import org.apache.spark.sql.functions.{array, col, explode, lit, struct, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class user_behavior_logparse --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/zfs/ql_etl.jar 2 1
  */
object user_behavior_logparse {
  def main(args: Array[String]) {
    if (args.length<1){
      System.err.println("please give the correct params")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("user_behavior_logparse")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def toLong(df: DataFrame, by: Seq[String]): DataFrame = {

      val (cols, types) = df.dtypes.filter{ case (c, _) => !by.contains(c)}.unzip
      require(types.distinct.size == 1)

      val kvs = explode(array(
        cols.map(c => struct(lit(c).alias("col_name"), col(c).alias("col_value"))): _*
      ))

      val byExprs = by.map(col(_))

      df
        .select(byExprs :+ kvs.alias("_kvs"): _*)
        .select(byExprs ++ Seq($"_kvs.col_name", $"_kvs.col_value"): _*)
    }

    val hdfsconf = sc.hadoopConfiguration


    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    //val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    var date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
    //参数:mode date  ==> mode=1 && date=2017-08-09  代表跑以前历史数据 ; mode=2 && date=1 代表每天定时跑 date=1代表跑上一天数据  什么都不传 默认跑上一天数据
    if (args.length==2 && args(0).toString=="1")//跑历史数据
    {
      date_etl = args(1).toString
    }else if(args.length==2 && args(0).toString=="2"){ //跑前n天数据
      date_etl=DateTime.now().withTimeAtStartOfDay().plusDays(-args(1).toInt).toString("yyyy-MM-dd")
    }
    println("=========================================================date_etl:"+date_etl)

    val schemaString = "userId,phone,phonePwd,userStatus,password,openid,openidU,realname,realnameTimes,sex,birthday,whetherLost,realnameStatus,province,city,district,avatar,downloadChannel,idCard,idCardAddress,cardPicFront,cardPicBack,realnameAddtime,guarantorUserId,guarantor,guarantorPhone,addressTel,addressProvince,addressCity,addressDistrict,address,addressPicId,schoolProvince,schoolCity,schoolDistrict,schoolAddress,schoolBuilding,schoolRoom,schoolId,campus,schoolTel,schoolAddressPic,regTime,regChannel,regProduct,regFrom,regVersion,education,educationStatus,regDeviceIdentify,idCardExpires,idCardPublic,companyName,companyTel,companyProvince,companyCity,companyDistrict,companyAddress,jobMonthlySalary,jobTitle,jobTypeId,jobEmail,jobPic,jobIncomePic,companyPeoples,companyProperty,companyTelCountry,companyTelFixed,companyTelSuffix,companyContact,companyContactPhone,employmentDate,stuEdu,eduGraduate,eduEducationDegree,eduEnrolDate,eduSpecialityName,eduGraduateTime,eduStudyResult,eduStudyStyle,eduVerifyChannel,oneCardNum,stuEnterSchoolYear,edu,marriage,house,car,title,live,jobInfoAllIsExpire,jobInfoAllStatus,companyNameStatus,companyTelStatus,companyAddressStatus,jobTitleStatus,jobIncomePicStatus,jobPicStatus,companyPropertyStatus,jobIncomeStatus,companyPeoplesStatus,jobEmailStatus,addressStatus,addressTelStatus,addressPicStatus,addressAllStatus,schoolAddressAllStatus,schoolAddressStatus,schoolTelStatus,schoolStatus,schoolAddressPicStatus,schoolAddressInfoIsExpire,realnameAllStatus,idCardStatus,stuEduStatus,stuEnterSchoolYearStatus,attestId,attestUpfilesId,attestTypeId,attestTypeNid,attestStatus,cardId,cardNum,cardBankName,cardMobile,cardCreateTime,cardYstatus,cardActiveStatus,cardVerifyStatus,cardBankBranchName,cardNumStatus,cardPicStatus,cardAmountLimit,cardExpire,repayDay,cardPicId,cardVerifyNum,cardType,linkmanId,linkman,linkmanPhone,linkmanType,linkmanNum,linkmanRelationship,linkmanExpires,linkmanVerifyStatus,linkmanStatus,linkmanPhoneStatus,linkmanSource,linkmanAddProduct,alipayScore,alipayScoreIsRisk,alipayScoreAddChannel,alipayScoreRiskQueryTime,alipayScoreExpireTime,alipayScoreQueryTime,alipayUsersStatus,alipaySkipAuthorize,tokenContents,tokenReExpiresIn,tokenIsOverdueRefresh,tokenRefreshTime,tokenRefreshStatus,tokenAddtime,tokenThirdName,tokenChannel,tokenStatus,tokenSkipAuthorize,timestamp,dataType,dataLog,addProduct,addChannel"

    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    //val json_rdd = sc.textFile("E:\\bigdata_msgmgt_mobanker-customer_userloginfo.log").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
//    json_rdd.foreach(println)
    val json_rdd = sc.textFile("/datahouse/ods/topic/bigdata_msgmgt_mobanker-customer_userloginfo/"+date_etl+"/*").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    val user_behavior_logparse_data = sqlContext.read.json(json_rdd).select("data").map(x=>x.getString(0)).filter(x=>x!=null)

    val user_behavior_logparse = sqlContext.read.schema(schema).json(user_behavior_logparse_data)

//    user_behavior_logparse.show(10)
    val df_trans=toLong(user_behavior_logparse, Seq("userId","dataType","dataLog","timestamp","addProduct","addChannel")) //userId,dataType,dataLog,timestamp,addProduct,addChannel
//    user_behavior_logparse.dtypes.foreach(println)

//    user_behavior_logparse.show()//转化前
//    df_trans.where($"column_value".isNotNull).show(150)//转化后

//    val user_behavior_logparse_select = user_behavior_logparse.select(
//      regexp_replace(col("taskId"),"\\`","_").alias("taskId"),
//      regexp_replace(col("taskTime"),"\\`","_").alias("updateTime"),
//      regexp_replace(col("appName"),"\\`","_").alias("appName"),
//      regexp_replace(col("appRequestId"),"\\`","_").alias("appRequestId"),
//      regexp_replace(col("inputParam"),"\\`","_").alias("inputParam"),
//      regexp_replace(col("outputParam"),"\\`","_").alias("outputParam"),
//      regexp_replace(col("ruleVersion"),"\\`","_").alias("ruleVersion"),
//      regexp_replace(col("productType"),"\\`","_").alias("productType"))

    val user_behavior_logparse_select= df_trans.where($"col_value".isNotNull)

    val expr = concat_ws("`", user_behavior_logparse_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/customer_alter_log_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/customer_alter_log_tmp/"),true)

    user_behavior_logparse_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/customer_alter_log_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/mongo.db/customer_alter_log_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/mongo.db/customer_alter_log_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/customer_alter_log_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/mongo.db/customer_alter_log_tmp/part-00000.snappy"))

    sc.stop()
  }

}
