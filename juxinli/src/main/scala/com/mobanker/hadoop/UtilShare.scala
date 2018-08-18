package com.mobanker.hadoop

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import com.alibaba.fastjson.JSON

/**
  * Created by 2046 on 2016/5/10 0010.
  */
class UtilShare extends  java.io.Serializable{

  /*
  * 天日期计算
  * 输入参数
  * dates: 日期,字符串格式,如 2016-01-01
  * mvValue: 移动长度,数字型
  * 返回参数:
  * 日期: 格式如 2016-01-01
  * */
  def dateMove(dates: String, mvValue: Int): String = {
    val datefmt = new SimpleDateFormat("yyyy-MM-dd");
    val dt = datefmt.parse(dates);
    val mvDate = Calendar.getInstance();
    mvDate.setTime(dt);
    mvDate.add(Calendar.DAY_OF_YEAR, mvValue);
    //日期移动
    val tagDate = mvDate.getTime();
    datefmt.format(tagDate);
  }

  /*
  * 计算周数及所在周日期列表
  * 输入参数:
  * tagDate: 目标日期,字符串,格式如 2016-01-01
  * 返回参数:
  * (目标日期所在年周,周日期数组), 如 ("2016-01", Array("2016-01-01", "2016-01-01", "2016-01-01", "2016-01-01"...))
  * */
  def weekDates(tagDate: String): (String, Array[String]) = {
    val dateFormats = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormats.parse(tagDate)
    val gc = new GregorianCalendar()
    gc.setFirstDayOfWeek(Calendar.MONDAY)
    gc.setTime(date)
    gc.set(Calendar.DAY_OF_WEEK, gc.getFirstDayOfWeek())
    val firstDayOfWeek = gc.getTime()
    val weekDays = (0 to 6).map(x => dateFormats.format(firstDayOfWeek.getTime + x * 24 * 60 * 60 * 1000)).toArray
    val weekYear = gc.getWeekYear
    val weekYearNum = gc.get(Calendar.WEEK_OF_YEAR)
    val yearWeek = weekYear + "-" + weekYearNum.toString
    (yearWeek, weekDays)
  }

  /*
* 日期转换,日期和时间戳字符串互转
* 输入: 时间格式字符串 或 long型字符串精确到秒,如(2016-01-01 11:11:11, 14299000123)
* 输出: 时间格式字符串 或 long型字符串精确到秒,如(2016-01-01 11:11:11, 14299000123)
* */
  val timeStamp = (xdate: String) => {
    val xFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (xdate.contains("-")) xFormat.parse(xdate.substring(0, 19)).getTime.toString
    else xFormat.format(new Date(xdate.toLong))
  }

  /*
* json格式字符串规格化输出,按tab分割
* 输入-json格式字符串: {"t1":t1,"t2":{"t21":21},"t3":{"t31":{"t311":"311"},"t32":"t32"}}
* 输入-json字段排列顺序: t1,t2.t21,t3.t31.t311,t3.t32
* 输出: 属性值
* */
  val convert2table = (json: String, schema: Array[String]) => {
    try {
      val jsonData = JSON.parseObject(json)
      var result = schema.map(sch => {
        val level = sch.split('.');
        val value = level.size match {
          case 1 => jsonData.getString(sch)
          case 2 => {
            val k1 = level(0)
            val k2 = level(1)
            val value = if (jsonData.getString(k1) == null) null
            else {
              JSON.parseObject(jsonData.getString(k1)).getString(k2)
            }
            value
          }
          case 3 => {
            val k1 = level(0)
            val k2 = level(1)
            val k3 = level(2)
            val value = if (jsonData.getString(k1) == null || JSON.parseObject(jsonData.getString(k1)).getString(k2) == null) null
            else {
              JSON.parseObject(JSON.parseObject(jsonData.getString(k1)).getString(k2)).getString(k3)
            }
            value
          }
        }
        if (value == null) "" else value
      }).mkString("\t")
      result
    } catch {
      case _ : Throwable =>
    }
  }
}
