package com.mobanker.hadoop

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{Map => mMap}

/**
  * Created by shazhenghui on 2017/6/1.
  */
object Json2Column {
  def main(args: Array[String]): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currentTime = dateFormat.format(new Date())
    val currentDay = currentTime.substring(0, 10)
    val dateBegin = if (args.size >= 1) args(0) else currentDay
    val dateEnd = if (args.size >= 2) args(1) else dateBegin
    val jsonSchema =  args(2).split(',')
    val fromDir = args(3).trim
    val toDir =  args(4).trim
    var datePointer = dateBegin;

    val appname = "Json2Column#" + currentTime.replaceAll("[^0-9]", "")
    // 类型+"#"+时间
    val sparkConf = new SparkConf().setAppName(appname)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val utilshare = new UtilShare();

    // hdfs 操作句柄
    @transient val hconf = new Configuration()
    val hdfsCoreSitePath = new Path("core-site.xml")
    val hdfsHDFSSitePath = new Path("hdfs-site.xml")
    hconf.addResource(hdfsCoreSitePath)
    hconf.addResource(hdfsHDFSSitePath)
    @transient val fileSystem = FileSystem.get(hconf)

    while (datePointer <= dateEnd) {
        val fromPath = fromDir + "/"  + datePointer
        val toPath = toDir + "/"  + datePointer
        if(fileSystem.exists(new Path(toPath))) fileSystem.delete(new Path(toPath), true)
        if(fileSystem.exists(new Path(fromPath))) sc.textFile(fromPath).filter(x=>x.size > 0).map(x=>utilshare.convert2table(x, jsonSchema)).filter(x=>x.toString != """()""").saveAsTextFile(toPath)
      datePointer = utilshare.dateMove(datePointer, 1)
    }
  }
}
