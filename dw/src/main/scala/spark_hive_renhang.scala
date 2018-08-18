import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime



/**
  * Created by gongshaojie on 2017/7/5.
  */
object spark_hive_renhang {
  def main(args: Array[String]) {

    if (args.length<1){
      System.err.println("please give the correct params")
      System.exit(1)
    }

    val timespan = args(0).toInt

    val date_etl = DateTime.now().withTimeAtStartOfDay().plusDays(-timespan).toString("yyyy-MM-dd")
    println("=========================================================date_etl:"+date_etl)

    val data_path ="/datahouse/ods/topic/pbccrc_report/"+date_etl+"/*"

//    val data_path = "/datahouse/ods/topic/pbccrc_report/2017-08-14/17.1502702984871"
//    val data_path ="D:\\software\\ql_etl\\data\\pbccrc_report.json"
    println("=========================================================data_path:"+data_path)
    val conf = new SparkConf().setAppName("spark_hive_renhang")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val originToJson = (originText:String) => {
      var result = ("","","","")
      try {
        val jsonData = JSON.parseObject(originText)
        val domain = jsonData.getString("domain")
        val messageId = jsonData.getString("messageId")
        val properties = JSON.parseObject(jsonData.getString("properties"))
        val idCard = properties.getString("idCard")
        val name = properties.getString("name")
        val sellerId = jsonData.getString("sellerId")
        val timestamp = jsonData.getString("timestamp")

        val renhang_data = JSON.parseObject(jsonData.getString("data"))

        val renhang_data_data = JSON.parseObject(renhang_data.getString("data"))
        val renhang_data_success = renhang_data.getString("success")

        val renhang_data_data_report = JSON.parseObject(renhang_data_data.getString("report"))
        val renhang_data_data_report_structure = JSON.parseObject(renhang_data_data.getString("report_structure"))
        val renhang_data_data_report_extend = JSON.parseObject(renhang_data_data.getString("report_extend"))
        val renhang_data_data_report_risk = JSON.parseObject(renhang_data_data.getString("report_risk"))

        val querytime = JSON.parseObject(renhang_data_data_report.getString("reportinfo")).getString("querytime")



        val report_data = new JSONObject();
        report_data.put("domain",domain)
        report_data.put("messageId",messageId)
        report_data.put("idCard",idCard)
        report_data.put("name",name)
        report_data.put("sellerId",sellerId)
        report_data.put("timestamp",timestamp)
        report_data.put("success",renhang_data_success)
        report_data.put("creditRecord",renhang_data_data_report.getJSONObject("creditRecord"))
        report_data.put("personalinfo",renhang_data_data_report.getJSONObject("personalinfo"))
        report_data.put("reportinfo",renhang_data_data_report.getJSONObject("reportinfo"))
        report_data.put("publicRecord",renhang_data_data_report.getJSONObject("publicRecord"))
        report_data.put("queryRecord",renhang_data_data_report.getJSONObject("queryRecord"))

        val report_structure_data = new JSONObject();
        report_structure_data.put("domain",domain)
        report_structure_data.put("messageId",messageId)
        report_structure_data.put("idCard",idCard)
        report_structure_data.put("name",name)
        report_structure_data.put("sellerId",sellerId)
        report_structure_data.put("timestamp",timestamp)
        report_structure_data.put("success",renhang_data_success)
        report_structure_data.put("basic",renhang_data_data_report_structure.getJSONObject("basic"))
        report_structure_data.put("general",renhang_data_data_report_structure.getJSONObject("general"))
        report_structure_data.put("assets",renhang_data_data_report_structure.getJSONArray("assets"))
        report_structure_data.put("compensates",renhang_data_data_report_structure.getJSONArray("compensates"))
        report_structure_data.put("guarantees",renhang_data_data_report_structure.getJSONArray("guarantees"))
        report_structure_data.put("credits",renhang_data_data_report_structure.getJSONArray("credits"))
        report_structure_data.put("loans",renhang_data_data_report_structure.getJSONArray("loans"))
        report_structure_data.put("taxs",renhang_data_data_report_structure.getJSONArray("taxs"))
        report_structure_data.put("judgments",renhang_data_data_report_structure.getJSONArray("judgments"))
        report_structure_data.put("enforcements",renhang_data_data_report_structure.getJSONArray("enforcements"))
        report_structure_data.put("punishments",renhang_data_data_report_structure.getJSONArray("punishments"))
        report_structure_data.put("telecoms",renhang_data_data_report_structure.getJSONArray("telecoms"))
        report_structure_data.put("traces",renhang_data_data_report_structure.getJSONArray("traces"))


        val report_extend_data = new JSONObject();
        report_extend_data.put("domain",domain)
        report_extend_data.put("messageId",messageId)
        report_extend_data.put("idCard",idCard)
        report_extend_data.put("name",name)
        report_extend_data.put("sellerId",sellerId)
        report_extend_data.put("timestamp",timestamp)
        report_extend_data.put("success",renhang_data_success)
        report_extend_data.put("querytime",querytime)
        report_extend_data.put("cusName",renhang_data_data_report_extend.getString("cusName"))
        report_extend_data.put("cusIdNo",renhang_data_data_report_extend.getString("cusIdNo"))
        report_extend_data.put("binderName",renhang_data_data_report_extend.getString("binderName"))
        report_extend_data.put("binderIdNo",renhang_data_data_report_extend.getString("binderIdNo"))
        report_extend_data.put("operatorId",renhang_data_data_report_extend.getString("operatorId"))
        report_extend_data.put("extend_1",renhang_data_data_report_extend.getString("extend_1"))
        report_extend_data.put("extend_2",renhang_data_data_report_extend.getString("extend_2"))
        report_extend_data.put("extend_3",renhang_data_data_report_extend.getString("extend_3"))

        val report_risk_data = new JSONObject();
        report_risk_data.put("domain",domain)
        report_risk_data.put("messageId",messageId)
        report_risk_data.put("idCard",idCard)
        report_risk_data.put("name",name)
        report_risk_data.put("sellerId",sellerId)
        report_risk_data.put("timestamp",timestamp)
        report_risk_data.put("success",renhang_data_success)
        report_risk_data.put("reportinfo",renhang_data_data_report_risk.getJSONObject("reportinfo"))
        report_risk_data.put("personalinfo",renhang_data_data_report_risk.getJSONObject("personalinfo"))
        report_risk_data.put("overdues",renhang_data_data_report_risk.getJSONObject("overdues"))
        report_risk_data.put("debts",renhang_data_data_report_risk.getJSONObject("debts"))
        report_risk_data.put("creditLoanHis",renhang_data_data_report_risk.getJSONObject("creditLoanHis"))
        report_risk_data.put("creditLoanNeeds",renhang_data_data_report_risk.getJSONObject("creditLoanNeeds"))
        report_risk_data.put("others",renhang_data_data_report_risk.getJSONObject("others"))


//        val report = new JSONObject();
//        report.put("report",report_data.toJSONString)
//        val report_structure = new JSONObject();
//        report_structure.put("report_structure",report_structure_data.toJSONString)
//        val report_extend = new JSONObject();
//        report_extend.put("report_extend",report_extend_data.toJSONString)
//        val report_risk = new JSONObject();
//        report_risk.put("report_risk",report_risk_data.toJSONString)



        // 返回 (report,report_structure,report_extend,report_risk)

        result = (report_data.toJSONString,report_structure_data.toJSONString,report_extend_data.toJSONString,report_risk_data.toJSONString)
      } catch {
        case ex: Exception => {
          println("error:"+ex.printStackTrace()+"orgintext==============>"+originText)
        }
      }
      result
    }

    val renhangschemaString = "domain,messageId,idCard,name,sellerId,timestamp,success"
    //.map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
    val json_rdd = sc.textFile(data_path).map(x=>originToJson(x))


//    val renhang = sqlContext.read.schema(renhangschema).json(json_rdd)
//
//    val report_data = sqlContext.read.json(renhang.select("data").map(x=>x.getString(0)))


    val report_schema = StructType(Seq(
      StructField("domain",StringType,true),
      StructField("messageId",StringType,true),
      StructField("idCard",StringType,true),
      StructField("name",StringType,true),
      StructField("sellerId",StringType,true),
      StructField("timestamp",StringType,true),
      StructField("success",StringType,true),
      StructField("creditRecord",StructType(Seq(
        StructField("intro",StringType,true),
        StructField("summary",StructType(Seq(
          StructField("otherLoan",StructType(Seq(
            StructField("overdueTotal",StringType,true),
            StructField("activeTotal",StringType,true),
            StructField("accountTotal",StringType,true),
            StructField("overdue90Total",StringType,true),
            StructField("guarantee",StringType,true)))),
          StructField("assetDisposal",StringType,true),
          StructField("mortgage",StructType(Seq(
            StructField("overdueTotal",StringType,true),
            StructField("activeTotal",StringType,true),
            StructField("accountTotal",StringType,true),
            StructField("overdue90Total",StringType,true),
            StructField("guarantee",StringType,true)))),
          StructField("creditCard",StructType(Seq(
            StructField("overdueTotal",StringType,true),
            StructField("activeTotal",StringType,true),
            StructField("accountTotal",StringType,true),
            StructField("overdue90Total",StringType,true),
            StructField("guarantee",StringType,true)))),
          StructField("compensate",StringType,true)
        ))),
        StructField("detail",StructType(Seq(
          StructField("assetDisposal",StringType,true),
          StructField("compensate",StringType,true),
          StructField("guarantee",StringType,true),
          StructField("otherLoan",StructType(Seq(
            StructField("noOverdueDetails",StringType,true),
            StructField("overdueDetails",StringType,true)
          ))),
          StructField("mortgage",StructType(Seq(
            StructField("noOverdueDetails",StringType,true),
            StructField("overdueDetails",StringType,true)
          ))),
          StructField("creditCard",StructType(Seq(
            StructField("noOverdueDetails",StringType,true),
            StructField("overdueDetails",StringType,true)
          )))
        )))

      ))),
      StructField("personalinfo",StructType(Seq(
        StructField("IDnumber",StringType,true),
        StructField("IDtype",StringType,true),
        StructField("marital",StringType,true),
        StructField("name",StringType,true)
      ))),
      StructField("reportinfo",StructType(Seq(
        StructField("querytime",StringType,true),
        StructField("reportSN",StringType,true),
        StructField("reporttime",StringType,true)
      ))),
      StructField("publicRecord",StructType(Seq(
        StructField("detail",StructType(Seq(
          StructField("tax",StringType,true),
          StructField("judgment",StringType,true),
          StructField("enforcement",StringType,true),
          StructField("punishment",StringType,true),
          StructField("telecom",StringType,true)
        ))),
        StructField("intro",StringType,true),
        StructField("summary",StructType(Seq(
          StructField("tax",StringType,true),
          StructField("judgment",StringType,true),
          StructField("enforcement",StringType,true),
          StructField("punishment",StringType,true),
          StructField("telecom",StringType,true)
        )))
      ))),
      StructField("queryRecord",StructType(Seq(
        StructField("summary",StructType(Seq(
          StructField("individual",StringType,true),
          StructField("organization",StringType,true)
        ))),
        StructField("intro",StringType,true),
        StructField("detail",StructType(Seq(
          StructField("individual",ArrayType(StructType(Seq(
            StructField("date",StringType,true),
            StructField("operator",StringType,true),
            StructField("reason",StringType,true)
          )))),
          StructField("organization",ArrayType(StructType(Seq(
            StructField("date",StringType,true),
            StructField("operator",StringType,true),
            StructField("reason",StringType,true)
          ))))
        )))
      )))
    ))


    val report = sqlContext.read.schema(report_schema).json(json_rdd.map(x=>x._1))


    val report_extend_schema = StructType("domain,messageId,idCard,name,sellerId,timestamp,success,querytime,cusName,cusIdNo,binderName,binderIdNo,operatorId,extend_1,extend_2,extend_3".split(",").map(fieldName => StructField(fieldName, StringType, true)))

    val report_extend = sqlContext.read.schema(report_extend_schema).json(json_rdd.map(x=>x._3))

    val report_risk_schema = StructType(Seq(
      StructField("domain",StringType,true),
      StructField("messageId",StringType,true),
      StructField("idCard",StringType,true),
      StructField("name",StringType,true),
      StructField("sellerId",StringType,true),
      StructField("timestamp",StringType,true),
      StructField("success",StringType,true),
      StructField("reportinfo",StructType(Seq(
        StructField("reporttime",StringType,true),
        StructField("reportSN",StringType,true),
        StructField("querytime",StringType,true)
      ))),
      StructField("personalinfo",StructType(Seq(
        StructField("IDnumber",StringType,true),
        StructField("IDtype",StringType,true),
        StructField("name",StringType,true),
        StructField("marital",StringType,true)
      ))),
      StructField("overdues",StructType(Seq(
        StructField("creditOrgCounts",StringType,true),
        StructField("creditOrgCounts200",StringType,true),
        StructField("creditAmts",StringType,true),
        StructField("creditAmts200",StringType,true),
        StructField("creditCountsM60",StringType,true),
        StructField("creditCountsM60D90",StringType,true),
        StructField("loanCounts",StringType,true),
        StructField("loanAmts",StringType,true),
        StructField("loanCountsM60",StringType,true),
        StructField("loanCountsM60D90",StringType,true),
        StructField("countsM60",StringType,true),
        StructField("countsM60D90",StringType,true)
      ))),
      StructField("debts",StructType(Seq(
        StructField("creditLimitMax",StringType,true),
        StructField("creditLimitTotal",StringType,true),
        StructField("creditOrgCounts",StringType,true),
        StructField("creditLimitUsed",StringType,true),
        StructField("creditLimitUseRate",StringType,true),
        StructField("loanAmts",StringType,true),
        StructField("loanAmtsNoSettle",StringType,true),
        StructField("loanCounts",StringType,true),
        StructField("loanBalances",StringType,true),
        StructField("loanBalanceCounts",StringType,true),
        StructField("loanBalancesMortgage",StringType,true),
        StructField("loanBalancesCar",StringType,true),
        StructField("loanBalancesBiz",StringType,true),
        StructField("loanBalancesOther",StringType,true),
        StructField("loanBalancesMonth",StringType,true),
        StructField("loanBalancesMortgageMonth",StringType,true),
        StructField("loanBalancesCarMonth",StringType,true),
        StructField("loanBalancesBizMonth",StringType,true),
        StructField("loanBalancesOtherMonth",StringType,true),
        StructField("loanBalanceInfos",ArrayType(StructType(Seq(
          StructField("org",StringType,true),
          StructField("amts",StringType,true),
          StructField("type",StringType,true),
          StructField("balances",StringType,true),
          StructField("debtMonths",StringType,true)
        ))))
      ))),
      StructField("creditLoanHis",StructType(Seq(
        StructField("creditMOB",StringType,true),
        StructField("loanMOB",StringType,true)
      ))),
      StructField("creditLoanNeeds",StructType(Seq(
        StructField("creditOrgCountsM3",StringType,true),
        StructField("creditLimitTotalM3",StringType,true),
        StructField("loanCountsM3",StringType,true),
        StructField("loanAmtsM3",StringType,true),
        StructField("loanQueriesM3",StringType,true),
        StructField("selfQueriesM3",StringType,true)
      ))),
      StructField("others",StructType(Seq(
        StructField("guarantees",StringType,true),
        StructField("guaranteeAmts",StringType,true),
        StructField("month6TaxAmts",StringType,true)
      )))
    ))

    val report_risk = sqlContext.read.schema(report_risk_schema).json(json_rdd.map(x=>x._4))


    val report_structure_schema = StructType(Seq(
      StructField("domain",StringType,true),
      StructField("messageId",StringType,true),
      StructField("idCard",StringType,true),
      StructField("name",StringType,true),
      StructField("sellerId",StringType,true),
      StructField("timestamp",StringType,true),
      StructField("success",StringType,true),
      StructField("basic",StructType(Seq(
        StructField("orgId",StringType,true),
        StructField("operatorId",StringType,true),
        StructField("reportSn",StringType,true),
        StructField("queryTime",StringType,true),
        StructField("reportTime",StringType,true),
        StructField("idtype",StringType,true),
        StructField("idno",StringType,true),
        StructField("idname",StringType,true),
        StructField("marital",StringType,true),
        StructField("assetFlag",StringType,true),
        StructField("compensateFlag",StringType,true),
        StructField("creditFlag",StringType,true),
        StructField("loanFlag",StringType,true),
        StructField("guaranteeFlag",StringType,true),
        StructField("taxFlag",StringType,true),
        StructField("judgmentFlag",StringType,true),
        StructField("enforcementFlag",StringType,true),
        StructField("punishmentFlag",StringType,true),
        StructField("telecomFlag",StringType,true),
        StructField("checks",StringType,true),
        StructField("isfail",StringType,true)
      ))),
      StructField("general",StructType(Seq(
        StructField("assetTotal",StringType,true),
        StructField("compensateTotal",StringType,true),
        StructField("creditTotal",StringType,true),
        StructField("creditActive",StringType,true),
        StructField("creditOverdue",StringType,true),
        StructField("creditOverdue90",StringType,true),
        StructField("creditGuarantee",StringType,true),
        StructField("mortgageTotal",StringType,true),
        StructField("mortgageActive",StringType,true),
        StructField("mortgageOverdue",StringType,true),
        StructField("mortgageOverdue90",StringType,true),
        StructField("mortgageGuarantee",StringType,true),
        StructField("otherloanTotal",StringType,true),
        StructField("otherloanActive",StringType,true),
        StructField("otherloanOverdue",StringType,true),
        StructField("otherloanOverdue90",StringType,true),
        StructField("otherloanGuarantee",StringType,true),
        StructField("isfail",StringType,true)
      ))),
      StructField("assets",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("company",StringType,true),
        StructField("debtDate",StringType,true),
        StructField("debtAmount",StringType,true),
        StructField("lastRepayment",StringType,true),
        StructField("balance",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("compensates",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("organization",StringType,true),
        StructField("lastCompensate",StringType,true),
        StructField("sumRepayment",StringType,true),
        StructField("lastRepayment",StringType,true),
        StructField("balance",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("guarantees",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("guaranteeTime",StringType,true),
        StructField("guaranteeIdtype",StringType,true),
        StructField("guaranteeIdname",StringType,true),
        StructField("guaranteeIdno",StringType,true),
        StructField("organization",StringType,true),
        StructField("contractAmount",StringType,true),
        StructField("guaranteeAmount",StringType,true),
        StructField("principaAmount",StringType,true),
        StructField("recordTime",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("credits",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("creditStatus",StringType,true),
        StructField("cardType",StringType,true),
        StructField("accountStatus",StringType,true),
        StructField("issueBank",StringType,true),
        StructField("issueTime",StringType,true),
        StructField("accountType",StringType,true),
        StructField("recordOrCancellation",StringType,true),
        StructField("creditAmount",StringType,true),
        StructField("creditUsed",StringType,true),
        StructField("overAmount",StringType,true),
        StructField("overMonth",StringType,true),
        StructField("overMonth90days",StringType,true),
        StructField("declaration",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("loans",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("creditStatus",StringType,true),
        StructField("accountStatus",StringType,true),
        StructField("loanBank",StringType,true),
        StructField("loanType",StringType,true),
        StructField("loanTime",StringType,true),
        StructField("loanAmount",StringType,true),
        StructField("loanBalance",StringType,true),
        StructField("loanDeadline",StringType,true),
        StructField("recordOrCancellation",StringType,true),
        StructField("overAmount",StringType,true),
        StructField("overMonth",StringType,true),
        StructField("overMonth90days",StringType,true),
        StructField("declaration",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("taxs",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("government",StringType,true),
        StructField("recordTime",StringType,true),
        StructField("amount",StringType,true),
        StructField("idNo",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("judgments",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("court",StringType,true),
        StructField("docketNo",StringType,true),
        StructField("docketCause",StringType,true),
        StructField("filingWay",StringType,true),
        StructField("filingTime",StringType,true),
        StructField("filingResult",StringType,true),
        StructField("filingEffective",StringType,true),
        StructField("litigationSubject",StringType,true),
        StructField("litigationAmount",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("enforcements",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("court",StringType,true),
        StructField("docketNo",StringType,true),
        StructField("docketCause",StringType,true),
        StructField("filingWay",StringType,true),
        StructField("filingTime",StringType,true),
        StructField("docketStatus",StringType,true),
        StructField("applyEnforceSubject",StringType,true),
        StructField("executedEnforceSubject",StringType,true),
        StructField("applyEnforeAmount",StringType,true),
        StructField("executedEnforceAmount",StringType,true),
        StructField("closedTime",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("punishments",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("organization",StringType,true),
        StructField("docketNo",StringType,true),
        StructField("reconsiderationFlag",StringType,true),
        StructField("reconsideration",StringType,true),
        StructField("punishmentContent",StringType,true),
        StructField("punishmentAmount",StringType,true),
        StructField("punishmentEffective",StringType,true),
        StructField("punishmentDeadline",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("telecoms",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("telco",StringType,true),
        StructField("business",StringType,true),
        StructField("recordTime",StringType,true),
        StructField("businessTime",StringType,true),
        StructField("amount",StringType,true),
        StructField("isfail",StringType,true)
      )))),
      StructField("traces",ArrayType(StructType(Seq(
        StructField("original",StringType,true),
        StructField("queryTime",StringType,true),
        StructField("queryOperator",StringType,true),
        StructField("queryReason",StringType,true),
        StructField("isfail",StringType,true)
      ))))
    ))
    val report_structure = sqlContext.read.schema(report_structure_schema).json(json_rdd.map(x=>x._2))

    /**
      * report.reportinfo and personalinfo  新增字段:domain,messageId,idCard,name,sellerId,timestamp,success
      */
//    val report_reportinfo_schemastring = "reporttime,reportSN,querytime,IDnumber,IDtype,name,marital"
//    val report_reportinfo_schema = StructType(report_reportinfo_schemastring.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val report_reportpersonalinfo = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.reporttime"),"\\`","_").alias("reporttime"),
      regexp_replace(col("reportinfo.reportSN"),"\\`","_").alias("reportSN"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("personalinfo.IDtype"),"\\`","_").alias("IDtype"),
      regexp_replace(col("personalinfo.marital"),"\\`","_").alias("marital"))

    val report_reportpersonalinfo_expr = concat_ws("`", report_reportpersonalinfo.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_reportpersonalinfo_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_reportpersonalinfo_tmp/"),true)

    report_reportpersonalinfo.na.fill("NULL").select(report_reportpersonalinfo_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_reportpersonalinfo_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_reportpersonalinfo_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_reportpersonalinfo_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_reportpersonalinfo_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_reportpersonalinfo_tmp/part-00000.snappy"))

    println("report_reportpersonalinfo done===============================================================================================================================")

    /**
      * report_creditrecord_summary
      */
    val report_creditrecord_summary_trans_otherLoan = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("creditRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("creditRecord.summary.otherLoan.overdueTotal"),"\\`","_").alias("overdueTotal"),
      regexp_replace(col("creditRecord.summary.otherLoan.activeTotal"),"\\`","_").alias("activeTotal"),
      regexp_replace(col("creditRecord.summary.otherLoan.accountTotal"),"\\`","_").alias("accountTotal"),
      regexp_replace(col("creditRecord.summary.otherLoan.overdue90Total"),"\\`","_").alias("overdue90Total"),
      regexp_replace(col("creditRecord.summary.otherLoan.guarantee"),"\\`","_").alias("guarantee"),
      regexp_replace(col("creditRecord.summary.assetDisposal"),"\\`","_").alias("assetDisposal"),
      regexp_replace(col("creditRecord.summary.compensate"),"\\`","_").alias("compensate")
    ).withColumn("type",lit("otherLoan"))

    val report_creditrecord_summary_trans_mortgage = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("creditRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("creditRecord.summary.mortgage.overdueTotal"),"\\`","_").alias("overdueTotal"),
      regexp_replace(col("creditRecord.summary.mortgage.activeTotal"),"\\`","_").alias("activeTotal"),
      regexp_replace(col("creditRecord.summary.mortgage.accountTotal"),"\\`","_").alias("accountTotal"),
      regexp_replace(col("creditRecord.summary.mortgage.overdue90Total"),"\\`","_").alias("overdue90Total"),
      regexp_replace(col("creditRecord.summary.mortgage.guarantee"),"\\`","_").alias("guarantee"),
      regexp_replace(col("creditRecord.summary.assetDisposal"),"\\`","_").alias("assetDisposal"),
      regexp_replace(col("creditRecord.summary.compensate"),"\\`","_").alias("compensate")
    ).withColumn("type",lit("mortgage"))

    val report_creditrecord_summary_trans_creditCard = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("creditRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("creditRecord.summary.creditCard.overdueTotal"),"\\`","_").alias("overdueTotal"),
      regexp_replace(col("creditRecord.summary.creditCard.activeTotal"),"\\`","_").alias("activeTotal"),
      regexp_replace(col("creditRecord.summary.creditCard.accountTotal"),"\\`","_").alias("accountTotal"),
      regexp_replace(col("creditRecord.summary.creditCard.overdue90Total"),"\\`","_").alias("overdue90Total"),
      regexp_replace(col("creditRecord.summary.creditCard.guarantee"),"\\`","_").alias("guarantee"),
      regexp_replace(col("creditRecord.summary.assetDisposal"),"\\`","_").alias("assetDisposal"),
      regexp_replace(col("creditRecord.summary.compensate"),"\\`","_").alias("compensate")
    ).withColumn("type",lit("creditCard"))

    val report_creditrecord_summary = report_creditrecord_summary_trans_otherLoan.unionAll(report_creditrecord_summary_trans_mortgage).unionAll(report_creditrecord_summary_trans_creditCard)
    val report_creditrecord_summary_expr = concat_ws("`", report_creditrecord_summary.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_creditrecord_summary_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_creditrecord_summary_tmp/"),true)

    report_creditrecord_summary.na.fill("NULL").select(report_creditrecord_summary_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_creditrecord_summary_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_creditrecord_summary_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_creditrecord_summary_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_creditrecord_summary_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_creditrecord_summary_tmp/part-00000.snappy"))

    println("report_creditrecord_summary done===============================================================================================================================")


    /**
      * report_creditrecord_detail
      */
    val report_creditrecord_detail_trans_otherLoan = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("creditRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("creditRecord.detail.assetDisposal"),"\\`","_").alias("assetDisposal"),
      regexp_replace(col("creditRecord.detail.compensate"),"\\`","_").alias("compensate"),
      regexp_replace(col("creditRecord.detail.guarantee"),"\\`","_").alias("guarantee"),
      regexp_replace(col("creditRecord.detail.otherLoan.noOverdueDetails"),"\\`","_").alias("noOverdueDetails"),
      regexp_replace(col("creditRecord.detail.otherLoan.overdueDetails"),"\\`","_").alias("overdueDetails")
    ).withColumn("type",lit("otherLoan"))

    val report_creditrecord_detail_trans_mortgage = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("creditRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("creditRecord.detail.assetDisposal"),"\\`","_").alias("assetDisposal"),
      regexp_replace(col("creditRecord.detail.compensate"),"\\`","_").alias("compensate"),
      regexp_replace(col("creditRecord.detail.guarantee"),"\\`","_").alias("guarantee"),
      regexp_replace(col("creditRecord.detail.mortgage.noOverdueDetails"),"\\`","_").alias("noOverdueDetails"),
      regexp_replace(col("creditRecord.detail.mortgage.overdueDetails"),"\\`","_").alias("overdueDetails")
    ).withColumn("type",lit("mortgage"))

    val report_creditrecord_detail_trans_creditCard = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("creditRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("creditRecord.detail.assetDisposal"),"\\`","_").alias("assetDisposal"),
      regexp_replace(col("creditRecord.detail.compensate"),"\\`","_").alias("compensate"),
      regexp_replace(col("creditRecord.detail.guarantee"),"\\`","_").alias("guarantee"),
      regexp_replace(col("creditRecord.detail.creditCard.noOverdueDetails"),"\\`","_").alias("noOverdueDetails"),
      regexp_replace(col("creditRecord.detail.creditCard.overdueDetails"),"\\`","_").alias("overdueDetails")
    ).withColumn("type",lit("creditCard"))

    val report_creditrecord_detail = report_creditrecord_detail_trans_otherLoan.unionAll(report_creditrecord_detail_trans_mortgage).unionAll(report_creditrecord_detail_trans_creditCard)
    val report_creditrecord_detail_expr = concat_ws("`", report_creditrecord_detail.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_creditrecord_detail_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_creditrecord_detail_tmp/"),true)

    report_creditrecord_detail.na.fill("NULL").select(report_creditrecord_detail_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_creditrecord_detail_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_creditrecord_detail_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_creditrecord_detail_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_creditrecord_detail_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_creditrecord_detail_tmp/part-00000.snappy"))

    println("report_creditrecord_detail done===============================================================================================================================")


    /**
      * report_publicrecord_summary
      */

    val report_publicrecord_summary = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("publicRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("publicRecord.summary.tax"),"\\`","_").alias("tax"),
      regexp_replace(col("publicRecord.summary.judgment"),"\\`","_").alias("judgment"),
      regexp_replace(col("publicRecord.summary.enforcement"),"\\`","_").alias("enforcement"),
      regexp_replace(col("publicRecord.summary.punishment"),"\\`","_").alias("punishment"),
      regexp_replace(col("publicRecord.summary.telecom"),"\\`","_").alias("telecom")
    )
    val report_publicrecord_summary_expr = concat_ws("`", report_publicrecord_summary.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_publicrecord_summary_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_publicrecord_summary_tmp/"),true)

    report_publicrecord_summary.na.fill("NULL").select(report_publicrecord_summary_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_publicrecord_summary_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_publicrecord_summary_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_publicrecord_summary_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_publicrecord_summary_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_publicrecord_summary_tmp/part-00000.snappy"))

    println("report_publicrecord_summary done===============================================================================================================================")

    /**
      * report_publicrecord_detail
      */
    val report_publicrecord_detail = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("publicRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("publicRecord.detail.tax"),"\\`","_").alias("tax"),
      regexp_replace(col("publicRecord.detail.judgment"),"\\`","_").alias("judgment"),
      regexp_replace(col("publicRecord.detail.enforcement"),"\\`","_").alias("enforcement"),
      regexp_replace(col("publicRecord.detail.punishment"),"\\`","_").alias("punishment"),
      regexp_replace(col("publicRecord.detail.telecom"),"\\`","_").alias("telecom")
    )
    val report_publicrecord_detail_expr = concat_ws("`", report_publicrecord_detail.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_publicrecord_detail_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_publicrecord_detail_tmp/"),true)

    report_publicrecord_detail.na.fill("NULL").select(report_publicrecord_detail_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_publicrecord_detail_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_publicrecord_detail_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_publicrecord_detail_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_publicrecord_detail_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_publicrecord_detail_tmp/part-00000.snappy"))

    println("report_publicrecord_detail done===============================================================================================================================")

    /**
      * report_queryecord_summary
      */
    val report_queryecord_summary = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("queryRecord.intro"),"\\`","_").alias("intro"),
      regexp_replace(col("queryRecord.summary.individual"),"\\`","_").alias("individual"),
      regexp_replace(col("queryRecord.summary.organization"),"\\`","_").alias("organization")
    )

    val report_queryecord_summary_expr = concat_ws("`", report_queryecord_summary.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_queryecord_summary_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_queryecord_summary_tmp/"),true)

    report_queryecord_summary.na.fill("NULL").select(report_queryecord_summary_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_queryecord_summary_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_queryecord_summary_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_queryecord_summary_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_queryecord_summary_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_queryecord_summary_tmp/part-00000.snappy"))

    println("report_queryecord_summary done===============================================================================================================================")

    /**
      * report_queryecord_detail
      */
    val report_queryecord_detail_trans_organization = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("queryRecord.intro"),"\\`","_").alias("intro"),
      explode(col("queryRecord.detail.organization")).as("organization_collection")
    ).withColumn("type",lit("organization"))
      .select("domain","messageId","idCard","name","sellerId","timestamp","success","querytime","IDnumber","intro","organization_collection.*","type")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("querytime"),"\\`","_").alias("querytime"),
        regexp_replace(col("IDnumber"),"\\`","_").alias("IDnumber"),
        regexp_replace(col("intro"),"\\`","_").alias("intro"),
        regexp_replace(col("date"),"\\`","_").alias("date"),
        regexp_replace(col("operator"),"\\`","_").alias("operator"),
        regexp_replace(col("reason"),"\\`","_").alias("reason"),
        regexp_replace(col("type"),"\\`","_").alias("type")
    )

    val report_queryecord_detail_trans_individual = report.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("queryRecord.intro"),"\\`","_").alias("intro"),
      explode(col("queryRecord.detail.individual")).as("individual_collection")
    ).withColumn("type",lit("individual"))
      .select("domain","messageId","idCard","name","sellerId","timestamp","success","querytime","IDnumber","intro","individual_collection.*","type")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("querytime"),"\\`","_").alias("querytime"),
        regexp_replace(col("IDnumber"),"\\`","_").alias("IDnumber"),
        regexp_replace(col("intro"),"\\`","_").alias("intro"),
        regexp_replace(col("date"),"\\`","_").alias("date"),
        regexp_replace(col("operator"),"\\`","_").alias("operator"),
        regexp_replace(col("reason"),"\\`","_").alias("reason"),
        regexp_replace(col("type"),"\\`","_").alias("type")
    )
    val report_queryecord_detail=report_queryecord_detail_trans_organization.unionAll(report_queryecord_detail_trans_individual)
    val report_queryecord_detail_expr = concat_ws("`", report_queryecord_detail.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_queryecord_detail_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_queryecord_detail_tmp/"),true)

    report_queryecord_detail.na.fill("NULL").select(report_queryecord_detail_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_queryecord_detail_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_queryecord_detail_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_queryecord_detail_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_queryecord_detail_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_queryecord_detail_tmp/part-00000.snappy"))

    println("report_queryecord_detail done===============================================================================================================================")


    /**
      *report_extend
      */
    val report_extend_data_trans = report_extend.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("cusName"),"\\`","_").alias("cusName"),
      regexp_replace(col("cusIdNo"),"\\`","_").alias("cusIdNo"),
      regexp_replace(col("binderName"),"\\`","_").alias("binderName"),
      regexp_replace(col("binderIdNo"),"\\`","_").alias("binderIdNo"),
      regexp_replace(col("operatorId"),"\\`","_").alias("operatorId"),
      regexp_replace(col("extend_1"),"\\`","_").alias("extend_1"),
      regexp_replace(col("extend_2"),"\\`","_").alias("extend_2"),
      regexp_replace(col("extend_3"),"\\`","_").alias("extend_3")
    )
    val report_extend_data = report_extend_data_trans
      .select("domain","messageId","idCard","name","sellerId","timestamp","success","cusName","cusIdNo","binderName","binderIdNo","operatorId","extend_1","extend_2","extend_3","querytime")

    val report_extend_data_expr = concat_ws("`", report_extend_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_extend_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_extend_tmp/"),true)

    report_extend_data.na.fill("NULL").select(report_extend_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_extend_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_extend_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_extend_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_extend_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_extend_tmp/part-00000.snappy"))

    println("report_extend done===============================================================================================================================")

    /**
      * report_risk
      */
    val report_risk_data = report_risk.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.reporttime"),"\\`","_").alias("reporttime"),
      regexp_replace(col("reportinfo.reportSN"),"\\`","_").alias("reportSN"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      regexp_replace(col("personalinfo.IDtype"),"\\`","_").alias("IDtype"),
      regexp_replace(col("personalinfo.marital"),"\\`","_").alias("marital"),
      regexp_replace(col("overdues.creditOrgCounts"),"\\`","_").alias("overdues_creditOrgCounts"),
      regexp_replace(col("overdues.creditOrgCounts200"),"\\`","_").alias("overdues_creditOrgCounts200"),
      regexp_replace(col("overdues.creditAmts"),"\\`","_").alias("overdues_creditAmts"),
      regexp_replace(col("overdues.creditAmts200"),"\\`","_").alias("overdues_creditAmts200"),
      regexp_replace(col("overdues.creditCountsM60"),"\\`","_").alias("overdues_creditCountsM60"),
      regexp_replace(col("overdues.creditCountsM60D90"),"\\`","_").alias("overdues_creditCountsM60D90"),
      regexp_replace(col("overdues.loanCounts"),"\\`","_").alias("overdues_loanCounts"),
      regexp_replace(col("overdues.loanAmts"),"\\`","_").alias("overdues_loanAmts"),
      regexp_replace(col("overdues.loanCountsM60"),"\\`","_").alias("overdues_loanCountsM60"),
      regexp_replace(col("overdues.loanCountsM60D90"),"\\`","_").alias("overdues_loanCountsM60D90"),
      regexp_replace(col("overdues.countsM60"),"\\`","_").alias("overdues_countsM60"),
      regexp_replace(col("overdues.countsM60D90"),"\\`","_").alias("overdues_countsM60D90"),
      regexp_replace(col("debts.creditLimitMax"),"\\`","_").alias("debts_creditLimitMax"),
      regexp_replace(col("debts.creditLimitTotal"),"\\`","_").alias("debts_creditLimitTotal"),
      regexp_replace(col("debts.creditOrgCounts"),"\\`","_").alias("debts_creditOrgCounts"),
      regexp_replace(col("debts.creditLimitUsed"),"\\`","_").alias("debts_creditLimitUsed"),
      regexp_replace(col("debts.creditLimitUseRate"),"\\`","_").alias("debts_creditLimitUseRate"),
      regexp_replace(col("debts.loanAmts"),"\\`","_").alias("debts_loanAmts"),
      regexp_replace(col("debts.loanAmtsNoSettle"),"\\`","_").alias("debts_loanAmtsNoSettle"),
      regexp_replace(col("debts.loanCounts"),"\\`","_").alias("debts_loanCounts"),
      regexp_replace(col("debts.loanBalances"),"\\`","_").alias("debts_loanBalances"),
      regexp_replace(col("debts.loanBalanceCounts"),"\\`","_").alias("debts_loanBalanceCounts"),
      regexp_replace(col("debts.loanBalancesMortgage"),"\\`","_").alias("debts_loanBalancesMortgage"),
      regexp_replace(col("debts.loanBalancesCar"),"\\`","_").alias("debts_loanBalancesCar"),
      regexp_replace(col("debts.loanBalancesBiz"),"\\`","_").alias("debts_loanBalancesBiz"),
      regexp_replace(col("debts.loanBalancesOther"),"\\`","_").alias("debts_loanBalancesOther"),
      regexp_replace(col("debts.loanBalancesMonth"),"\\`","_").alias("debts_loanBalancesMonth"),
      regexp_replace(col("debts.loanBalancesMortgageMonth"),"\\`","_").alias("debts_loanBalancesMortgageMonth"),
      regexp_replace(col("debts.loanBalancesCarMonth"),"\\`","_").alias("debts_loanBalancesCarMonth"),
      regexp_replace(col("debts.loanBalancesBizMonth"),"\\`","_").alias("debts_loanBalancesBizMonth"),
      regexp_replace(col("debts.loanBalancesOtherMonth"),"\\`","_").alias("debts_loanBalancesOtherMonth"),
      regexp_replace(col("creditLoanHis.creditMOB"),"\\`","_").alias("creditLoanHis_creditMOB"),
      regexp_replace(col("creditLoanHis.loanMOB"),"\\`","_").alias("creditLoanHis_loanMOB"),
      regexp_replace(col("creditLoanNeeds.creditOrgCountsM3"),"\\`","_").alias("creditLoanNeeds_creditOrgCountsM3"),
      regexp_replace(col("creditLoanNeeds.creditLimitTotalM3"),"\\`","_").alias("creditLoanNeeds_creditLimitTotalM3"),
      regexp_replace(col("creditLoanNeeds.loanCountsM3"),"\\`","_").alias("creditLoanNeeds_loanCountsM3"),
      regexp_replace(col("creditLoanNeeds.loanAmtsM3"),"\\`","_").alias("creditLoanNeeds_loanAmtsM3"),
      regexp_replace(col("creditLoanNeeds.loanQueriesM3"),"\\`","_").alias("creditLoanNeeds_loanQueriesM3"),
      regexp_replace(col("creditLoanNeeds.selfQueriesM3"),"\\`","_").alias("creditLoanNeeds_selfQueriesM3"),
      regexp_replace(col("others.guarantees"),"\\`","_").alias("others_guarantees"),
      regexp_replace(col("others.guaranteeAmts"),"\\`","_").alias("others_guaranteeAmts"),
      regexp_replace(col("others.month6TaxAmts"),"\\`","_").alias("others_month6TaxAmts")
    )
    val report_risk_data_expr = concat_ws("`", report_risk_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_risk_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_risk_tmp/"),true)

    report_risk_data.na.fill("NULL").select(report_risk_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_risk_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_risk_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_risk_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_risk_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_risk_tmp/part-00000.snappy"))

    println("report_risk done===============================================================================================================================")

    /**
      * report_risk_loanBalanceInfos
      */
    val report_risk_loanBalanceInfos_data = report_risk.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("reportinfo.querytime"),"\\`","_").alias("querytime"),
      regexp_replace(col("personalinfo.IDnumber"),"\\`","_").alias("IDnumber"),
      explode(col("debts.loanBalanceInfos")).as("loanBalanceInfos_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","querytime","IDnumber","loanBalanceInfos_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("querytime"),"\\`","_").alias("querytime"),
        regexp_replace(col("IDnumber"),"\\`","_").alias("IDnumber"),
        regexp_replace(col("org"),"\\`","_").alias("org"),
        regexp_replace(col("amts"),"\\`","_").alias("amts"),
        regexp_replace(col("type"),"\\`","_").alias("type"),
        regexp_replace(col("balances"),"\\`","_").alias("balances"),
        regexp_replace(col("debtMonths"),"\\`","_").alias("debtMonths")
    )
    val report_risk_loanBalanceInfos_data_expr = concat_ws("`", report_risk_loanBalanceInfos_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_risk_loanBalanceInfos_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_risk_loanBalanceInfos_tmp/"),true)

    report_risk_loanBalanceInfos_data.na.fill("NULL").select(report_risk_loanBalanceInfos_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_risk_loanBalanceInfos_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_risk_loanbalanceinfos_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_risk_loanbalanceinfos_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_risk_loanBalanceInfos_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_risk_loanbalanceinfos_tmp/part-00000.snappy"))

    println("report_risk_loanBalanceInfos done===============================================================================================================================")


    /**
      * report_structure.basic
      */

    val report_structure_basic_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.orgId"),"\\`","_").alias("orgId"),
      regexp_replace(col("basic.operatorId"),"\\`","_").alias("operatorId"),
      regexp_replace(col("basic.reportSn"),"\\`","_").alias("reportSn"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      regexp_replace(col("basic.reportTime"),"\\`","_").alias("reportTime"),
      regexp_replace(col("basic.idtype"),"\\`","_").alias("idtype"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.idname"),"\\`","_").alias("idname"),
      regexp_replace(col("basic.marital"),"\\`","_").alias("marital"),
      regexp_replace(col("basic.assetFlag"),"\\`","_").alias("assetFlag"),
      regexp_replace(col("basic.compensateFlag"),"\\`","_").alias("compensateFlag"),
      regexp_replace(col("basic.creditFlag"),"\\`","_").alias("creditFlag"),
      regexp_replace(col("basic.loanFlag"),"\\`","_").alias("loanFlag"),
      regexp_replace(col("basic.guaranteeFlag"),"\\`","_").alias("guaranteeFlag"),
      regexp_replace(col("basic.taxFlag"),"\\`","_").alias("taxFlag"),
      regexp_replace(col("basic.judgmentFlag"),"\\`","_").alias("judgmentFlag"),
      regexp_replace(col("basic.enforcementFlag"),"\\`","_").alias("enforcementFlag"),
      regexp_replace(col("basic.punishmentFlag"),"\\`","_").alias("punishmentFlag"),
      regexp_replace(col("basic.telecomFlag"),"\\`","_").alias("telecomFlag"),
      regexp_replace(col("basic.checks"),"\\`","_").alias("checks"),
      regexp_replace(col("basic.isfail"),"\\`","_").alias("isfail")
    )

    val report_structure_basic_data_expr = concat_ws("`", report_structure_basic_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_basic_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_basic_tmp/"),true)

    report_structure_basic_data.na.fill("NULL").select(report_structure_basic_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_basic_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_basic_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_basic_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_basic_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_basic_tmp/part-00000.snappy"))

    println("report_structure_basic done===============================================================================================================================")

    /**
      * report_structure.general
      */
    val report_structure_general_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      regexp_replace(col("general.assetTotal"),"\\`","_").alias("assetTotal"),
      regexp_replace(col("general.compensateTotal"),"\\`","_").alias("compensateTotal"),
      regexp_replace(col("general.creditTotal"),"\\`","_").alias("creditTotal"),
      regexp_replace(col("general.creditActive"),"\\`","_").alias("creditActive"),
      regexp_replace(col("general.creditOverdue"),"\\`","_").alias("creditOverdue"),
      regexp_replace(col("general.creditOverdue90"),"\\`","_").alias("creditOverdue90"),
      regexp_replace(col("general.creditGuarantee"),"\\`","_").alias("creditGuarantee"),
      regexp_replace(col("general.mortgageTotal"),"\\`","_").alias("mortgageTotal"),
      regexp_replace(col("general.mortgageActive"),"\\`","_").alias("mortgageActive"),
      regexp_replace(col("general.mortgageOverdue"),"\\`","_").alias("mortgageOverdue"),
      regexp_replace(col("general.mortgageOverdue90"),"\\`","_").alias("mortgageOverdue90"),
      regexp_replace(col("general.mortgageGuarantee"),"\\`","_").alias("mortgageGuarantee"),
      regexp_replace(col("general.otherloanTotal"),"\\`","_").alias("otherloanTotal"),
      regexp_replace(col("general.otherloanActive"),"\\`","_").alias("otherloanActive"),
      regexp_replace(col("general.otherloanOverdue"),"\\`","_").alias("otherloanOverdue"),
      regexp_replace(col("general.otherloanOverdue90"),"\\`","_").alias("otherloanOverdue90"),
      regexp_replace(col("general.otherloanGuarantee"),"\\`","_").alias("otherloanGuarantee"),
      regexp_replace(col("general.isfail"),"\\`","_").alias("isfail")
    )
    val report_structure_general_data_expr = concat_ws("`", report_structure_general_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_general_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_general_tmp/"),true)

    report_structure_general_data.na.fill("NULL").select(report_structure_general_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_general_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_general_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_general_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_general_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_general_tmp/part-00000.snappy"))

    println("report_structure_general done===============================================================================================================================")


    /**
      * report_structure.assets列表
      */
    val report_structure_assets_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("assets")).as("assets_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","assets_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("company"),"\\`","_").alias("company"),
        regexp_replace(col("debtDate"),"\\`","_").alias("debtDate"),
        regexp_replace(col("debtAmount"),"\\`","_").alias("debtAmount"),
        regexp_replace(col("lastRepayment"),"\\`","_").alias("lastRepayment"),
        regexp_replace(col("balance"),"\\`","_").alias("balance"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_assets_data_expr = concat_ws("`", report_structure_assets_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_assets_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_assets_tmp/"),true)

    report_structure_assets_data.na.fill("NULL").select(report_structure_assets_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_assets_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_assets_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_assets_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_assets_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_assets_tmp/part-00000.snappy"))

    println("report_structure_assets done===============================================================================================================================")

    /**
      * report_structure.compensates列表
      */
    val report_structure_compensates_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("compensates")).as("compensates_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","compensates_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("organization"),"\\`","_").alias("organization"),
        regexp_replace(col("lastCompensate"),"\\`","_").alias("lastCompensate"),
        regexp_replace(col("sumRepayment"),"\\`","_").alias("sumRepayment"),
        regexp_replace(col("lastRepayment"),"\\`","_").alias("lastRepayment"),
        regexp_replace(col("balance"),"\\`","_").alias("balance"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_compensates_data_expr = concat_ws("`", report_structure_compensates_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_compensates_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_compensates_tmp/"),true)

    report_structure_compensates_data.na.fill("NULL").select(report_structure_compensates_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_compensates_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_compensates_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_compensates_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_compensates_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_compensates_tmp/part-00000.snappy"))

    println("report_structure_compensates done===============================================================================================================================")

    /**
      * report_structure.guarantees列表
      */
    val report_structure_guarantees_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("guarantees")).as("guarantees_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","guarantees_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("guaranteeTime"),"\\`","_").alias("guaranteeTime"),
        regexp_replace(col("guaranteeIdtype"),"\\`","_").alias("guaranteeIdtype"),
        regexp_replace(col("guaranteeIdname"),"\\`","_").alias("guaranteeIdname"),
        regexp_replace(col("guaranteeIdno"),"\\`","_").alias("guaranteeIdno"),
        regexp_replace(col("organization"),"\\`","_").alias("organization"),
        regexp_replace(col("contractAmount"),"\\`","_").alias("contractAmount"),
        regexp_replace(col("guaranteeAmount"),"\\`","_").alias("guaranteeAmount"),
        regexp_replace(col("principaAmount"),"\\`","_").alias("principaAmount"),
        regexp_replace(col("recordTime"),"\\`","_").alias("recordTime"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )

    val report_structure_guarantees_data_expr = concat_ws("`", report_structure_guarantees_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_guarantees_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_guarantees_tmp/"),true)

    report_structure_guarantees_data.na.fill("NULL").select(report_structure_guarantees_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_guarantees_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_guarantees_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_guarantees_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_guarantees_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_guarantees_tmp/part-00000.snappy"))

    println("report_structure_guarantees done===============================================================================================================================")

    /**
      * report_structure.credits列表
      */
    val report_structure_credits_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("credits")).as("credits_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","credits_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("creditStatus"),"\\`","_").alias("creditStatus"),
        regexp_replace(col("cardType"),"\\`","_").alias("cardType"),
        regexp_replace(col("accountStatus"),"\\`","_").alias("accountStatus"),
        regexp_replace(col("issueBank"),"\\`","_").alias("issueBank"),
        regexp_replace(col("issueTime"),"\\`","_").alias("issueTime"),
        regexp_replace(col("accountType"),"\\`","_").alias("accountType"),
        regexp_replace(col("recordOrCancellation"),"\\`","_").alias("recordOrCancellation"),
        regexp_replace(col("creditAmount"),"\\`","_").alias("creditAmount"),
        regexp_replace(col("creditUsed"),"\\`","_").alias("creditUsed"),
        regexp_replace(col("overAmount"),"\\`","_").alias("overAmount"),
        regexp_replace(col("overMonth"),"\\`","_").alias("overMonth"),
        regexp_replace(col("overMonth90days"),"\\`","_").alias("overMonth90days"),
        regexp_replace(col("declaration"),"\\`","_").alias("declaration"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_credits_data_expr = concat_ws("`", report_structure_credits_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_credits_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_credits_tmp/"),true)

    report_structure_credits_data.na.fill("NULL").select(report_structure_credits_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_credits_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_credits_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_credits_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_credits_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_credits_tmp/part-00000.snappy"))

    println("report_structure_credits done===============================================================================================================================")

    /**
      * report_structure.loans列表
      */
    val report_structure_loans_data  = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("loans")).as("loans_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","loans_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("creditStatus"),"\\`","_").alias("creditStatus"),
        regexp_replace(col("accountStatus"),"\\`","_").alias("accountStatus"),
        regexp_replace(col("loanBank"),"\\`","_").alias("loanBank"),
        regexp_replace(col("loanType"),"\\`","_").alias("loanType"),
        regexp_replace(col("loanTime"),"\\`","_").alias("loanTime"),
        regexp_replace(col("loanAmount"),"\\`","_").alias("loanAmount"),
        regexp_replace(col("loanBalance"),"\\`","_").alias("loanBalance"),
        regexp_replace(col("loanDeadline"),"\\`","_").alias("loanDeadline"),
        regexp_replace(col("recordOrCancellation"),"\\`","_").alias("recordOrCancellation"),
        regexp_replace(col("overAmount"),"\\`","_").alias("overAmount"),
        regexp_replace(col("overMonth"),"\\`","_").alias("overMonth"),
        regexp_replace(col("overMonth90days"),"\\`","_").alias("overMonth90days"),
        regexp_replace(col("declaration"),"\\`","_").alias("declaration"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_loans_data_expr = concat_ws("`", report_structure_loans_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_loans_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_loans_tmp/"),true)

    report_structure_loans_data.na.fill("NULL").select(report_structure_loans_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_loans_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_loans_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_loans_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_loans_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_loans_tmp/part-00000.snappy"))

    println("report_structure_loans done===============================================================================================================================")

    /**
      * report_structure.taxs列表
      */
    val report_structure_taxs_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("taxs")).as("taxs_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","taxs_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("government"),"\\`","_").alias("government"),
        regexp_replace(col("recordTime"),"\\`","_").alias("recordTime"),
        regexp_replace(col("amount"),"\\`","_").alias("amount"),
        regexp_replace(col("idNo"),"\\`","_").alias("t_idNo"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_taxs_data_expr = concat_ws("`", report_structure_taxs_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_taxs_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_taxs_tmp/"),true)

    report_structure_taxs_data.na.fill("NULL").select(report_structure_taxs_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_taxs_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_taxs_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_taxs_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_taxs_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_taxs_tmp/part-00000.snappy"))

    println("report_structure_taxs done===============================================================================================================================")

    /**
      * report_structure.judgments列表
      */
    val report_structure_judgments_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("judgments")).as("judgments_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","judgments_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("court"),"\\`","_").alias("court"),
        regexp_replace(col("docketNo"),"\\`","_").alias("docketNo"),
        regexp_replace(col("docketCause"),"\\`","_").alias("docketCause"),
        regexp_replace(col("filingWay"),"\\`","_").alias("filingWay"),
        regexp_replace(col("filingTime"),"\\`","_").alias("filingTime"),
        regexp_replace(col("filingResult"),"\\`","_").alias("filingResult"),
        regexp_replace(col("filingEffective"),"\\`","_").alias("filingEffective"),
        regexp_replace(col("litigationSubject"),"\\`","_").alias("litigationSubject"),
        regexp_replace(col("litigationAmount"),"\\`","_").alias("litigationAmount"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_judgments_data_expr = concat_ws("`", report_structure_judgments_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_judgments_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_judgments_tmp/"),true)

    report_structure_judgments_data.na.fill("NULL").select(report_structure_judgments_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_judgments_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_judgments_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_judgments_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_judgments_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_judgments_tmp/part-00000.snappy"))

    println("report_structure_judgments done===============================================================================================================================")

    /**
      * report_structure.enforcements列表
      */
    val report_structure_enforcements_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("enforcements")).as("enforcements_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","enforcements_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("court"),"\\`","_").alias("court"),
        regexp_replace(col("docketNo"),"\\`","_").alias("docketNo"),
        regexp_replace(col("docketCause"),"\\`","_").alias("docketCause"),
        regexp_replace(col("filingWay"),"\\`","_").alias("filingWay"),
        regexp_replace(col("filingTime"),"\\`","_").alias("filingTime"),
        regexp_replace(col("docketStatus"),"\\`","_").alias("docketStatus"),
        regexp_replace(col("applyEnforceSubject"),"\\`","_").alias("applyEnforceSubject"),
        regexp_replace(col("executedEnforceSubject"),"\\`","_").alias("executedEnforceSubject"),
        regexp_replace(col("applyEnforeAmount"),"\\`","_").alias("applyEnforeAmount"),
        regexp_replace(col("executedEnforceAmount"),"\\`","_").alias("executedEnforceAmount"),
        regexp_replace(col("closedTime"),"\\`","_").alias("closedTime"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_enforcements_data_expr = concat_ws("`", report_structure_enforcements_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_enforcements_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_enforcements_tmp/"),true)

    report_structure_enforcements_data.na.fill("NULL").select(report_structure_enforcements_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_enforcements_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_enforcements_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_enforcements_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_enforcements_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_enforcements_tmp/part-00000.snappy"))

    println("report_structure_enforcements done===============================================================================================================================")
    /**
      * report_structure.punishments列表
      */
    val report_structure_punishments_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("punishments")).as("punishments_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","punishments_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("organization"),"\\`","_").alias("organization"),
        regexp_replace(col("docketNo"),"\\`","_").alias("docketNo"),
        regexp_replace(col("reconsiderationFlag"),"\\`","_").alias("reconsiderationFlag"),
        regexp_replace(col("reconsideration"),"\\`","_").alias("reconsideration"),
        regexp_replace(col("punishmentContent"),"\\`","_").alias("punishmentContent"),
        regexp_replace(col("punishmentAmount"),"\\`","_").alias("punishmentAmount"),
        regexp_replace(col("punishmentEffective"),"\\`","_").alias("punishmentEffective"),
        regexp_replace(col("punishmentDeadline"),"\\`","_").alias("punishmentDeadline"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )

    val report_structure_punishments_data_expr = concat_ws("`", report_structure_punishments_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_punishments_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_punishments_tmp/"),true)

    report_structure_punishments_data.na.fill("NULL").select(report_structure_punishments_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_punishments_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_punishments_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_punishments_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_punishments_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_punishments_tmp/part-00000.snappy"))

    println("report_structure_punishments done===============================================================================================================================")


    /**
      * report_structure.telecoms列表
      */
    val report_structure_telecoms_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("queryTime"),
      explode(col("telecoms")).as("telecoms_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","queryTime","idno","telecoms_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("telco"),"\\`","_").alias("telco"),
        regexp_replace(col("business"),"\\`","_").alias("business"),
        regexp_replace(col("recordTime"),"\\`","_").alias("recordTime"),
        regexp_replace(col("businessTime"),"\\`","_").alias("businessTime"),
        regexp_replace(col("amount"),"\\`","_").alias("amount"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_telecoms_data_expr = concat_ws("`", report_structure_telecoms_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_telecoms_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_telecoms_tmp/"),true)

    report_structure_telecoms_data.na.fill("NULL").select(report_structure_telecoms_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_telecoms_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_telecoms_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_telecoms_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_telecoms_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_telecoms_tmp/part-00000.snappy"))

    println("report_structure_telecoms done===============================================================================================================================")

    /**
      * report_structure.traces列表
      */
    val report_structure_traces_data = report_structure.select(
      regexp_replace(col("domain"),"\\`","_").alias("domain"),
      regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
      regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
      regexp_replace(col("name"),"\\`","_").alias("name"),
      regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
      regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
      regexp_replace(col("success"),"\\`","_").alias("success"),
      regexp_replace(col("basic.idno"),"\\`","_").alias("b_idno"),
      regexp_replace(col("basic.queryTime"),"\\`","_").alias("b_queryTime"),
      explode(col("traces")).as("traces_collection")
    ).select("domain","messageId","idCard","name","sellerId","timestamp","success","b_queryTime","b_idno","traces_collection.*")
      .select(
        regexp_replace(col("domain"),"\\`","_").alias("domain"),
        regexp_replace(col("messageId"),"\\`","_").alias("messageId"),
        regexp_replace(col("idCard"),"\\`","_").alias("idCard"),
        regexp_replace(col("name"),"\\`","_").alias("name"),
        regexp_replace(col("sellerId"),"\\`","_").alias("sellerId"),
        regexp_replace(col("timestamp"),"\\`","_").alias("timestamp"),
        regexp_replace(col("success"),"\\`","_").alias("success"),
        regexp_replace(col("b_queryTime"),"\\`","_").alias("queryTime"),
        regexp_replace(col("b_idno"),"\\`","_").alias("idno"),
        regexp_replace(col("original"),"\\`","_").alias("original"),
        regexp_replace(col("queryTime"),"\\`","_").alias("t_queryTime"),
        regexp_replace(col("queryOperator"),"\\`","_").alias("queryOperator"),
        regexp_replace(col("queryReason"),"\\`","_").alias("queryReason"),
        regexp_replace(col("isfail"),"\\`","_").alias("isfail")
      )
    val report_structure_traces_data_expr = concat_ws("`", report_structure_traces_data.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/report_structure_traces_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/report_structure_traces_tmp/"),true)

    report_structure_traces_data.na.fill("NULL").select(report_structure_traces_data_expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/report_structure_traces_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_traces_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_traces_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/report_structure_traces_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/report_structure_traces_tmp/part-00000.snappy"))

    println("report_structure_traces done===============================================================================================================================")


    sc.stop()



  }

}
