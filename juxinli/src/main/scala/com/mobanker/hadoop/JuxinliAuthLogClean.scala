package com.mobanker.hadoop

import java.text.SimpleDateFormat
import java.util.Date

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
object JuxinliAuthLogClean extends Serializable {

  case class Record(table:String,time:String,data:String)

  def main(args: Array[String]): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currentTime = dateFormat.format(new Date())
    val currentDay = currentTime.substring(0, 10)
    val dateBegin = if (args.size >= 1) args(0) else currentDay
    val dateEnd = if (args.size >= 2) args(1) else dateBegin
    val fromDir = if (args.size >= 3) args(2) else "/tmp/juxinli2"
    val toDir = if (args.size >= 4) args(3) else "/tmp/juxinli/clean"
    val validTables = if (args.size >= 5) args(4) else ""
    var datePointer = dateBegin;

    val appname = "JuxinliAuthLogClean#" + currentTime.replaceAll("[^0-9]", "")
    // 类型+"#"+时间
    val sparkConf = new SparkConf().setAppName(appname)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[Record]))
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

    while (datePointer <= dateEnd) {
      val tagdir = fromDir + "/" + datePointer;
      val data = sc.textFile(tagdir).
        map(x=>juxinliClean.parseDetails(x, validTables).toArray().map(t=>t.toString)).
        flatMap(x=>x).
        map(x=>x.split("\t")).
        filter(x=>x.size == 3).
        map(x=>Record(x(0), x(1),  x(2))); // output: (table,time,data)

      data.persist(StorageLevel.MEMORY_AND_DISK)

      val tableList = data.map(x=>x.table).distinct().collect()
      tableList.foreach(table => {
        val tableToDir = toDir + "/" + table + "/" + datePointer
        fileSystem.delete(new Path(tableToDir), true)
        data.filter(x=>x.table == table).map(x=>x.data  + "\t" + x.time ).saveAsTextFile(tableToDir);
      })
      data.unpersist()
      datePointer = utilshare.dateMove(datePointer, 1)
    }
  }
}

