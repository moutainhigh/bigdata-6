package lxy

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * Created by liuxinyuan on 2018/1/17
  */
object Scalatest {
  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    val dt = "2017-12-12"
    var rsDate: String = null
    try {
      rsDate = format.format(new Date(format.parse(s"$dt").getTime + -1 * 24 * 60 * 60 * 1000))
    } catch {
      case e: Exception => println("日期格式有误" + e.getMessage)
    }
    println(rsDate)
  }

}
