package lxy

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime
import com.alibaba.fastjson.JSONObject
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions.{col, concat_ws}


/**
  * Created by liuxinyuan on 2018/2/2.
  */
object spark_hive_sms_indicator_model {
  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //起始日期
    val modelDt = "06"
    var start: String = null
    var mn: String = null
    if (args.length == 0) {
      start = DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyy-MM-dd")
      mn = DateTime.now().withTimeAtStartOfDay().plusMonths(-1).toString("yyyy-MM")
    } else if (args.length >= 1) {
      start = args(0)
      mn = dateCalculateMonth(start)
    }
    val d30: String = dateCalculate(start, -30)
    val d7: String = dateCalculate(start, -7)

    val conf = new SparkConf().setAppName("sms_indicator_model")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    //需要预测的数据
    val mnArray = "('" + start.substring(0, 7) + "','" + mn + "')"

    val target_df = sqlContext.sql(
      s"""
        select t.userid, concat_ws('；',collect_set(parsedtext)) parsedtext
        from
        (select userid,parsedtext,row_number() over(partition by id order by sms_calltime) rn from ee.structured_result where  dt in ${mnArray}
        and id is not null
        and substring(sms_calltime,0,10) between '${d30}' and '${start}') as  t
        where t.rn =1 group by userid
      """.stripMargin).toDF("userid", "sentence")


    val start_date = dateCalculate(start, -60) //申请前模型结束申请时间
    val end_date = dateCalculate(start, -30) //申请前模型开始申请时间
    val start_datetime = start_date + " 00:00:00"
    val end_datetime = end_date + " 23:59:59"
    val start_stamp = (format.parse(start_datetime).getTime / 1000).toInt
    val end_stamp = (format.parse(end_datetime).getTime / 1000).toInt

    val start_dt = start_date.substring(0, 7) //申请前模型结束短信时间
    val end_dt = end_date.substring(0, 7) //申请前模型开始短信时间
    //创建还款后训练集
    val after_test_df = sqlContext.sql(
      s"""
        SELECT  cast(y as double) as y,
                sr.parsedtext content
        FROM
          (SELECT trs.user_id,
                 max(case
              WHEN trs.late_days > 3 THEN
              1
              ELSE 0 end) AS y
          FROM cw_mobp2p.t_repay_schedule trs
           where  repay_yestime between '${start_datetime}' and   '${end_datetime}'   group by  trs.user_id) a
         inner JOIN
          (SELECT concat_ws(' ',collect_set(parsedtext)) parsedtext,userid
          FROM ee.structured_result
          WHERE dt IN ('${start_dt}','${end_dt}')
          GROUP BY  userid) sr
            ON a.user_id = sr.userid
      """).toDF("label", "sentence").randomSplit(Array(0.7, 0.3), seed = 11L)

    //创建申请前训练集
    val apply_test_df = sqlContext.sql(
      s"""
        SELECT  cast(y as double) as y,
                sr.parsedtext content
        FROM
          (SELECT trs.user_id,
                 max(case
              WHEN trs.late_days > 3 THEN
              1
              ELSE 0 end) AS y
          FROM cw_mobp2p.t_repay_schedule trs
          inner join (select borrow_nid from audit_mobp2p.yyd_order_info where  addtime between ${start_stamp} and   ${end_stamp} group by borrow_nid) yoi
          on yoi.borrow_nid=trs.borrow_nid
          WHERE
            (trs.late_days > 3 OR trs.late_days = 0)
          GROUP BY  trs.user_id) a
         inner JOIN
          (SELECT concat_ws(' ',collect_set(parsedtext)) parsedtext,userid
          FROM ee.structured_result
          WHERE dt IN ('${start_dt}','${end_dt}')
          GROUP BY  userid) sr
            ON a.user_id = sr.userid
      """).toDF("label", "sentence").randomSplit(Array(0.7, 0.3), seed = 11L)


    //创建管道
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("rawFeatures").setNumFeatures(1000)
    val idf = new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01) //.setInputCol(idf.getOutputCol).setOutputCol("regression")
    //   .setFeaturesCol("sentence").setPredictionCol("predictValue")
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, lr))


    //申请前模型评价
    if (format.format(new Date()).substring(8, 10).equals(modelDt)) {
      //训练申请前模型
      val apply_model = pipeline.fit(apply_test_df(0))
      //训练申请后的模型
      val after_model = pipeline.fit(after_test_df(0))

      val pre_model_date = dateCalculateMonth(start) + "-" + modelDt
      val save_model_date = format.format(new Date).substring(0, 10)
      var pre_applyLoadModel = PipelineModel.load(s"/user/model/sms_model/apply_sms_model_${pre_model_date}")
      var pre_afterLoadModel = PipelineModel.load(s"/user/model/sms_model/after_sms_model_${pre_model_date}")
      pre_applyLoadModel = compareModel(apply_model, pre_applyLoadModel, apply_test_df(1))
      pre_afterLoadModel = compareModel(after_model, pre_afterLoadModel, apply_test_df(1))
      pre_applyLoadModel.save(s"/user/model/sms_model/apply_sms_model_${save_model_date}")
      pre_afterLoadModel.save(s"/user/model/sms_model/after_sms_model_${save_model_date}")
    }

    //加载模型并应用
    val modelDate = DateTime.now().withTimeAtStartOfDay().toString("yyyy-MM-dd").substring(0, 8) + modelDt
    case class Person(userid: String, probability: Double)
    sc.getConf.registerKryoClasses(Array(classOf[Person]))
    import sqlContext.implicits._
    val applydf = PipelineModel.load(s"/user/model/sms_model/apply_sms_model_${modelDate}")
      .transform(target_df)
      .select("userid", "probability")
      .map(
        row =>
          Row(row.get(0).toString, round(row.get(1).toString.split(",")(1).substring(0, row.get(1).toString.split(",")(1).length - 1).toDouble))
      ).map(x => Person(x.getAs[String](1), x.getAs[Double](2))).toDF().registerTempTable("applydf")

    PipelineModel.load(s"/user/model/sms_model/after_sms_model_${modelDate}")
      .transform(target_df)
      .select("userid", "probability")
      .map(
        row =>
          (row.get(0).toString, round(row.get(1).toString.split(",")(1).substring(0, row.get(1).toString.split(",")(1).length - 1).toDouble))
      ).map(x => Person(x._1, x._2)).toDF().registerTempTable("afterdf")


    val expr1 = concat_ws("`", df.columns.map(col): _*)
    if (fs.exists(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/sms_indicator_model_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/sms_indicator_model_tmp/"), true)
    df.na.fill("NULL").select(expr1).repartition(1).map(_.getString(0)).saveAsTextFile("/datahouse/ods/mongo/sms_indicator_model_tmp/")
    if (fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_sms_indicator_model/part-00000.snappy"))) {
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_sms_indicator_model/part-00000.snappy"), true)
    }
    fs.rename(new org.apache.hadoop.fs.Path("/datahouse/ods/mongo/sms_indicator_model_tmp/part-00000.snappy"), new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/tmp_sms_indicator_model/part-00000.snappy"))
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

  def evaluateModel(df: DataFrame, pre_loadModel: PipelineModel) = {
    val json = new JSONObject()

    val prediction = pre_loadModel.transform(df).select("label", "probability", "prediction").map(
      row =>
        (row.get(0).toString.toDouble, row.get(1).toString.split(",")(1).substring(0, row.get(1).toString.split(",")(1).length - 1).toDouble, row.get(2).toString.toDouble)
    ).map(x => (x._2, x._1))

    val metrics = new BinaryClassificationMetrics(prediction)
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
    auROC
  }

  def round(dou: Double) = {
    val ddf = NumberFormat.getNumberInstance()
    ddf.setMaximumFractionDigits(2)
    ddf.format(dou).toDouble
  }

  def compareModel(currentModel: PipelineModel, preModel: PipelineModel, df: DataFrame) = {
    val preRoc = evaluateModel(df, preModel)
    val currentRoc = evaluateModel(df, currentModel)
    if (currentRoc > preRoc) {
      currentModel
    } else {
      preModel
    }
  }

}
