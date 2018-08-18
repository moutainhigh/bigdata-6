package zfs

import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/20.
  */
object test {

  def main(args: Array[String]) {
    val begin = DateTime.now().withTimeAtStartOfDay().plusDays(-1)
    val end =  DateTime.now().withTimeAtStartOfDay().plusDays(0)

    val date_str = begin.year().getAsString+""+begin.monthOfYear().getAsString+""+begin.dayOfMonth().getAsString
    println("begin:"+begin.toString("yyyyMMdd"))
    println("end:"+end)
    println("date_str:"+date_str)

  }

}
