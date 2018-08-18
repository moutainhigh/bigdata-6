import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/20.
  */
object test {

  def main(args: Array[String]) {
    val s=
      """
        2018-04-25 15:21:40|IHR|DEBUG|172.31.11.27 | uat-web-deploy-360493796-zh0bh|[http-nio-8080-exec-5]| org.mybatis.spring.SqlSessionUtils| Closing non transactional SqlSession [org.apache.ibatis.session.defaults.DefaultSqlSession@81cd347]
      """.stripMargin
  }
}
