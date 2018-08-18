package com.mobanker.hadoop

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.shazhx.juxinli.api.JuxinliClean
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{Map => mMap}

/**
  * Created by shazhenghui on 2017/6/1.
  */
object Json2Table {
  def main(args: Array[String]): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currentTime = dateFormat.format(new Date())
    val currentDay = currentTime.substring(0, 10)
    val dateBegin = if (args.size >= 1) args(0) else currentDay
    val dateEnd = if (args.size >= 2) args(1) else dateBegin
    val fromDir = if (args.size >= 3) args(2) else "/tmp/juxinli/clean"
    val toDir = if (args.size >= 4) args(3) else "/tmp/juxinli/table"
    val jsonSchemaPath =  if (args.size >= 5) args(4) else "/tmp/juxinli/schema"
    var datePointer = dateBegin

    val appname = "Json2Table#" + currentTime.replaceAll("[^0-9]", "")
    // 类型+"#"+时间
    val sparkConf = new SparkConf().setAppName(appname)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val utilshare = new UtilShare();
    val juxinliClean = new JuxinliClean();


    // hdfs 操作句柄
    @transient val hconf = new Configuration()
    val hdfsCoreSitePath = new Path("core-site.xml")
    val hdfsHDFSSitePath = new Path("hdfs-site.xml")
    hconf.addResource(hdfsCoreSitePath)
    hconf.addResource(hdfsHDFSSitePath)
    @transient val fileSystem = FileSystem.get(hconf)

    // 获取schema:Map(table,Array(cln))
    val schemaConf = sc.textFile(jsonSchemaPath).distinct().map(x=>x.split(":")).map(x=>(x(0), x(1).split(","))).collect().toMap
    val convert2table =(json:String, schema:Array[String]) =>{
      val jsonData = JSON.parseObject(json)
      var result = schema.map(sch => {if (jsonData.getString(sch) == null) "" else jsonData.getString(sch).toString}).mkString("\t")
      result
    }

    while (datePointer <= dateEnd) {
      schemaConf.foreach(x => {
        val table = x._1
        val schema = x._2
        val fromPath = fromDir + "/" + table + "/" + datePointer
        val toPath = toDir + "/" + table + "/" + datePointer
        if(fileSystem.exists(new Path(toPath))) fileSystem.delete(new Path(toPath), true)
        if(fileSystem.exists(new Path(fromPath))) sc.textFile(fromPath).map(x=>x.split("\t")).filter(x=>x.size > 0 && x(0).size > 0).map(x=>convert2table(x(0), schema) + "\t" + x.tail.mkString("\t")).distinct(3).saveAsTextFile(toPath)
      })
      datePointer = utilshare.dateMove(datePointer, 1)
    }
  }
}
