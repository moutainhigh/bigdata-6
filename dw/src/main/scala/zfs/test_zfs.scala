package zfs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gongshaojie on 2017/6/20.
  */
object test_zfs {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("spark_hive_lbs").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = Seq((1,201708, 0.0, 0.6), (1,201709, 0.6, 0.7)).toDF("A","B", "col_1", "col_2")

    def toLong(df: DataFrame, by: Seq[String]): DataFrame = {

      val (cols, types) = df.dtypes.filter{ case (c, _) => !by.contains(c)}.unzip
      require(types.distinct.size == 1)

      val kvs = explode(array(
        cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))): _*
      ))

      val byExprs = by.map(col(_))

      df
        .select(byExprs :+ kvs.alias("_kvs"): _*)
        .select(byExprs ++ Seq($"_kvs.key", $"_kvs.val"): _*)
    }

    val df_trans=toLong(df, Seq("A","B"))
    df.dtypes.foreach(println)

    df.show()//转化前
    df_trans.show()//转化后

  }

}
