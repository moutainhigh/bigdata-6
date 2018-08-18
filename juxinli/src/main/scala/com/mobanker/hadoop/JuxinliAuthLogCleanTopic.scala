package com.mobanker.hadoop

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.shazhx.juxinli.api.JuxinliClean
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{Map => mMap}

/**
  * 清洗聚信立原始日志数据
  * Created by shazhenghui on 2017/5/25.
  */
object JuxinliAuthLogCleanTopic extends Serializable {

  case class Record(table:String,time:String,data:String,triggerStrategy:String,provider:String)

  def main(args: Array[String]): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currentTime = dateFormat.format(new Date())
    val currentDay = currentTime.substring(0, 10)
    val dateBegin = if (args.size >= 1) args(0) else currentDay
    val dateEnd = if (args.size >= 2) args(1) else dateBegin
    val fromDir = if (args.size >= 3) args(2) else "/datahouse/ods/topic/biz_crawler_query_success"
    val toDir = if (args.size >= 4) args(3) else "/tmp/qlcrawer/clean"
    val validTables = if (args.size >= 5) args(4) else ""
    var datePointer = dateBegin


    val appname = "JuxinliAuthLogCleanTopic#" + currentTime.replaceAll("[^0-9]", "")
    // 类型+"#"+时间
    val sparkConf = new SparkConf().setAppName(appname)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[Record]))
    val sc = new SparkContext(sparkConf)
    val utilshare = new UtilShare()
    val juxinliClean = new JuxinliClean()

    // topic格式解析到mongo格式,同时获取触发方式(triggerStrategy)及渠道来源(provider):
    // data -> AccessReportData;
    // properties.idCard -> idCard;
    // properties.data_type -> channel;
    // properties.name -> name;
    // properties.mobile -> phone;
    // messageId -> billno
    // timestamp -> addtime
    val convertTopic2Mongo = (originText:String) => {
      var result = ("", "", "")
      try {
        val jsonData = JSON.parseObject(originText)
        val AccessReportData = jsonData.getString("data")
        // 外层信息
        val billno = jsonData.getString("messageId")
        val addtime = jsonData.getString("timestamp")
        // 请求信息
        val properties = JSON.parseObject(jsonData.getString("properties"))
        val idCard = properties.getString("idCard")
        val channel = properties.getString("data_type")
        val name = properties.getString("name")
        val phone = properties.getString("mobile")
        val triggerStrategy = properties.getString("triggerStrategy")
        val provider = properties.getString("provider")
        // 按照mongo格式重新组装消息体并返回元组(info,triggerStrategy,provider)
        val req = new JSONObject()
        req.put("idCard", idCard)
        req.put("channel", channel)
        req.put("name", name)
        req.put("phone", phone)

        val _id = new JSONObject()
        _id.put("$oid", billno)

        val data = new JSONObject()
        data.put("AccessReportData", AccessReportData)
        data.put("idCard", idCard)
        data.put("channel", channel)
        data.put("name", name)
        data.put("phone", phone)
        data.put("addtime", addtime)
        data.put("req", req.toJSONString)
        data.put("_id", _id)
        // 返回
        result = (data.toJSONString, triggerStrategy, provider)
      } catch {
        case _ : Throwable =>
      }
      result
    }


    // hdfs 操作句柄
    @transient val hconf = new Configuration()
    val hdfsCoreSitePath = new Path("core-site.xml")
    val hdfsHDFSSitePath = new Path("hdfs-site.xml")
    hconf.addResource(hdfsCoreSitePath)
    hconf.addResource(hdfsHDFSSitePath)
    @transient val fileSystem = FileSystem.get(hconf)

    while (datePointer <= dateEnd) {
      val tagdir = fromDir + "/" + datePointer
      val data = sc.textFile(tagdir).
        map(x=> convertTopic2Mongo(x)).//json平铺
        filter(x=> x._1 != "" && x._2 != "" && x._3 != "").
        map(x=>juxinliClean.parseDetails(x._1, validTables).toArray().map(t=>t.toString + "\t" + x._2 + "\t" + x._3)).
        flatMap(x=>x).
        map(x=>x.split("\t")).
        filter(x=>x.size == 5).
        map(x=>Record(x(0), x(1),  x(2),  x(3),  x(4))); // output: (table,time,data, triggerStrategy, provider)

      data.persist(StorageLevel.MEMORY_AND_DISK)


      val tableList = data.map(x=>x.table).distinct().collect()
      tableList.foreach(table => {
        val tableToDir = toDir + "/" + table + "/" + datePointer
        fileSystem.delete(new Path(tableToDir), true)
        data.filter(x=>x.table == table).map(x=>x.data  + "\t" + x.time  + "\t" + x.provider  + "\t" + x.triggerStrategy).saveAsTextFile(tableToDir);
      })
      data.unpersist()
      datePointer = utilshare.dateMove(datePointer, 1)
    }
  }
}

