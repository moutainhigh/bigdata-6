package lxy


import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime


/**
  * Created by liuxinyuan on 2018/1/26.
  */
object spark_hive_sms_indicator {
  def main(args: Array[String]): Unit = {
    //起始日期
    val format = new SimpleDateFormat("yyyy-MM-dd")
    var start: String = null
    var mn: String = null
    if (args.length == 0) {
      start = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
      mn = DateTime.now().withTimeAtStartOfDay().plusMonths(-1).toString("yyyy-MM")
    } else if (args.length >= 1) {
      start = args(0)
      mn = dateCalculateMonth(start)

    }

    println("start:" + start)

    val d30: String = dateCalculate(start, -30)
    val d7: String = dateCalculate(start, -7)
    val mnArray = "('" + start.substring(0, 7) + "','" + mn + "')"

    val conf = new SparkConf().setAppName("sms_indicator")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val statement =
      s"""
        with 30droc as(
        select userid,count(org) as 30d_register_org_cnt from (select userid,org from ee.structured_result where substring(sms_calltime,0,10)>='${d30}'  and dt in ${mnArray} group by userid,org) t
        where  org is not null
        and org != ''
        group by userid
        ),
        30dloc as (
        select userid,count(org) as 30d_loan_org_cnt from (select userid,org from ee.structured_result where   substring(sms_calltime,0,10) >= '${d30}' and dt in ${mnArray} and  loan_amount is not null and loan_amount!='' group by userid,org) t
        where  org is not null
        and org != ''
        group by userid
        ),
        30dooc as (
        select userid,count(org) as 30d_overdue_org_cnt from (select userid,org from ee.structured_result where  substring(sms_calltime,0,10) >= '${d30}' and dt in ${mnArray} and  classifier_business in('已逾期','严重逾期','催收','逾期公布') group by userid,org) t
        where  org is not null
        and org != ''
        group by userid
        ),
        7dooc as (
        select userid,count(org) as 7d_overdue_org_cnt from (select userid,org from ee.structured_result where  substring(sms_calltime,0,10) >= '${d7}' and   dt in ${mnArray} and  classifier_business in('已逾期','严重逾期','催收','逾期公布') group by userid,org) t
        where  org is not null
        and org != ''
        group by userid
        ),
        30dmia as (
        select
        userid,30d_max_income_date,30d_max_income_amount
        from
        (
        select userid,
        row_number() over(partition by userid order by Extract_Amount(income_amount) desc) as rn,
        Extract_Amount(income_amount) as 30d_max_income_amount,
        sms_calltime 30d_max_income_date
        from  ee.structured_result where   substring(sms_calltime,0,10) >= '${d30}'
        and dt in ${mnArray}
        ) t where rn = 1
        ),
        30dmca as (
        select
        userid,30d_max_consume_amount,30d_max_consume_date
        from
        (
        select userid,
        row_number() over(partition by userid order by Extract_Amount(consume_amount) desc) as rn,
        Extract_Amount(consume_amount) as 30d_max_consume_amount,
        sms_calltime 30d_max_consume_date
        from  ee.structured_result where  substring(sms_calltime,0,10) >= '${d30}'
        and dt in ${mnArray}
        ) t where rn = 1
        ),
        t1 as(
        select userid from (
        select userid from 30droc
        union all
        select userid from 30dloc
        union all
        select userid from 30dooc
        union all
        select userid from 7dooc
        union all
        select userid from 30dmia
        union all
        select userid from 30dmca) tt group by userid
        )
        select
        t1.userid,
        case when 30droc.30d_register_org_cnt is null then 0 else 30droc.30d_register_org_cnt end 30d_register_org_cnt,
        case when 30dloc.30d_loan_org_cnt is null then 0 else 30dloc.30d_loan_org_cnt end 30d_loan_org_cnt,
        case when 30dooc.30d_overdue_org_cnt is null then 0 else 30dooc.30d_overdue_org_cnt end 30d_overdue_org_cnt,
        case when 7dooc.7d_overdue_org_cnt is null then 0 else 7dooc.7d_overdue_org_cnt end 7d_overdue_org_cnt,
        case when 30dmia.30d_max_income_amount is null or 30dmia.30d_max_income_date is null then '' else 30dmia.30d_max_income_date end 30d_max_income_date,
        case when 30dmia.30d_max_income_amount is null then 0 else 30dmia.30d_max_income_amount end 30d_max_income_amount,
        case when 30dmca.30d_max_consume_amount is null then 0 else 30dmca.30d_max_consume_amount end 30d_max_consume_amount,
        case when 30dmca.30d_max_consume_amount is null or 30dmca.30d_max_consume_date is null then '' else 30dmca.30d_max_consume_date end 30d_max_consume_date,
        '${start}' as end_date
        from
        t1
        left join  30droc
        on t1.userid = 30droc.userid
        left join  30dloc
        on t1.userid = 30dloc.userid
        left join  30dooc
        on t1.userid = 30dooc.userid
        left join  7dooc
        on t1.userid = 7dooc.userid
        left join  30dmia
        on t1.userid = 30dmia.userid
        left join  30dmca
        on t1.userid = 30dmca.userid
      """
    println("sql is " + statement)

    val df = sqlContext.sql(
      statement
    )

    println("df is " + df.first().toString())
    //持久化bigdata_crawler_renthouse_info
    val expr1 = concat_ws("`", df.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/sms_indicator_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/sms_indicator_tmp/"), true)
    df.na.fill("NULL").select(expr1).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/sms_indicator_tmp/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_sms_indicator/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_sms_indicator/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/sms_indicator_tmp/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_sms_indicator/part-00000.snappy"))
    sc.stop()

  }

  def dateCalculate(dat: String, reduce: Integer) = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    var rsDate: String = null
    try {
      rsDate = format.format(new Date(format.parse(dat).getTime + reduce * 24 * 60 * 60 * 1000L))
    } catch {
      case e: Exception => println("日期格式有误" + e.getMessage)
    }
    rsDate
  }

  //某一日期上个月例如2017-12
  def dateCalculateMonth(dat: String) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val df = new SimpleDateFormat("yyyy-MM")
    var date: Date = null
    try {
      date = sdf.parse(dat)
    } catch {
      case e: Exception => e.printStackTrace()
    }

    val c = Calendar.getInstance()
    c.setTime(date)
    c.add(Calendar.MONTH, -1)

    val lastDate = df.format(c.getTime)

    lastDate
  }
}