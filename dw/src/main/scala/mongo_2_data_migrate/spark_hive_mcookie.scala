package mongo_2_data_migrate

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by gongshaojie on 2017/6/29.
  *
  * spark-submit --class mongo_2_data_migrate.spark_hive_mcookie --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl.jar
  */
object spark_hive_mcookie {
  def main(args: Array[String]) {
    if (args.length<1){
      System.err.println("please give the correct params")
      System.exit(1)
    }

    var date_etl=DateTime.now().withTimeAtStartOfDay().plusDays(-1).toString("yyyyMMdd")
    //参数:mode date  ==> mode=1 && date=201707  代表跑以前历史数据 ; mode=2 && date=1 代表每天定时跑 date=1代表跑上一天数据  什么都不传 默认跑上一天数据
    if (args.length==2 && args(0).toString=="1")
    {
      date_etl = args(1).toString
    }else if(args.length==2 && args(0).toString=="2"){
      date_etl=DateTime.now().withTimeAtStartOfDay().plusDays(-args(1).toInt).toString("yyyyMMdd")
    }

    println("mcookie=========================================================date_etl:"+date_etl)

    val conf = new SparkConf().setAppName("spark_hive_mcookie")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val hdfsconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsconf)

    val stringToJson = (originText:String) => {
      var result = ("")
      try {
        val jsonData = JSON.parseObject(originText)
        val userHead = JSON.parseObject(jsonData.getString("userHead"))

        val data = new JSONObject();
        data.put("userId", jsonData.getString("userId"))
        data.put("addtime", jsonData.getString("addtime"))
        data.put("userCookie", jsonData.getString("userCookie"))
        data.put("ip", jsonData.getString("ip"))
        data.put("type", jsonData.getString("type"))
        data.put("addProudct", jsonData.getString("addProudct"))
        data.put("addChannel", jsonData.getString("addChannel"))

        data.put("userHead_REDIRECT_UNIQUE_ID", userHead.getString("REDIRECT_UNIQUE_ID"))
        data.put("userHead_REDIRECT_STATUS", userHead.getString("REDIRECT_STATUS"))
        data.put("userHead_UNIQUE_ID", userHead.getString("UNIQUE_ID"))
        data.put("userHead_HTTP_HOST", userHead.getString("HTTP_HOST"))
        data.put("userHead_HTTP_X_REAL_IP", userHead.getString("HTTP_X_REAL_IP"))
        data.put("userHead_HTTP_X_FORWARDED_FOR", userHead.getString("HTTP_X_FORWARDED_FOR"))
        data.put("userHead_HTTP_CONNECTION", userHead.getString("HTTP_CONNECTION"))
        data.put("userHead_HTTP_USER_AGENT", userHead.getString("HTTP_USER_AGENT"))
        data.put("userHead_HTTP_WEB_SERVER_TYPE", userHead.getString("HTTP_WEB_SERVER_TYPE"))
        data.put("userHead_HTTP_WL_PROXY_CLIENT_IP", userHead.getString("HTTP_WL_PROXY_CLIENT_IP"))
        data.put("userHead_HTTP_X_FORWARDED_PROTO", userHead.getString("HTTP_X_FORWARDED_PROTO"))
        data.put("userHead_HTTP_X_FORWARDED_CLUSTER", userHead.getString("HTTP_X_FORWARDED_CLUSTER"))
        data.put("userHead_HTTP_VIA", userHead.getString("HTTP_VIA"))
        data.put("userHead_HTTP_EAGLEEYE_TRACEID", userHead.getString("HTTP_EAGLEEYE_TRACEID"))
        data.put("userHead_HTTP_ALI_SWIFT_LOG_HOST", userHead.getString("HTTP_ALI_SWIFT_LOG_HOST"))
        data.put("userHead_HTTP_X_CLIENT_SCHEME", userHead.getString("HTTP_X_CLIENT_SCHEME"))
        data.put("userHead_HTTP_ALI_CDN_REAL_IP", userHead.getString("HTTP_ALI_CDN_REAL_IP"))
        data.put("userHead_HTTP_ACCEPT", userHead.getString("HTTP_ACCEPT"))
        data.put("userHead_HTTP_UPGRADE_INSECURE_REQUESTS", userHead.getString("HTTP_UPGRADE_INSECURE_REQUESTS"))
        data.put("userHead_HTTP_COOKIE", userHead.getString("HTTP_COOKIE"))
        data.put("userHead_HTTP_ACCEPT_LANGUAGE", userHead.getString("HTTP_ACCEPT_LANGUAGE"))
        data.put("userHead_HTTP_ACCEPT_ENCODING", userHead.getString("HTTP_ACCEPT_ENCODING"))
        data.put("userHead_PATH", userHead.getString("PATH"))
        data.put("userHead_SERVER_SIGNATURE", userHead.getString("SERVER_SIGNATURE"))
        data.put("userHead_SERVER_SOFTWARE", userHead.getString("SERVER_SOFTWARE"))
        data.put("userHead_SERVER_NAME", userHead.getString("SERVER_NAME"))
        data.put("userHead_SERVER_ADDR", userHead.getString("SERVER_ADDR"))
        data.put("userHead_SERVER_PORT", userHead.getString("SERVER_PORT"))
        data.put("userHead_REMOTE_ADDR", userHead.getString("REMOTE_ADDR"))
        data.put("userHead_DOCUMENT_ROOT", userHead.getString("DOCUMENT_ROOT"))
        data.put("userHead_SERVER_ADMIN", userHead.getString("SERVER_ADMIN"))
        data.put("userHead_SCRIPT_FILENAME", userHead.getString("SCRIPT_FILENAME"))
        data.put("userHead_REMOTE_PORT", userHead.getString("REMOTE_PORT"))
        data.put("userHead_REDIRECT_QUERY_STRING", userHead.getString("REDIRECT_QUERY_STRING"))
        data.put("userHead_REDIRECT_URL", userHead.getString("REDIRECT_URL"))
        data.put("userHead_GATEWAY_INTERFACE", userHead.getString("GATEWAY_INTERFACE"))
        data.put("userHead_SERVER_PROTOCOL", userHead.getString("SERVER_PROTOCOL"))
        data.put("userHead_REQUEST_METHOD", userHead.getString("REQUEST_METHOD"))
        data.put("userHead_QUERY_STRING", userHead.getString("QUERY_STRING"))
        data.put("userHead_REQUEST_URI", userHead.getString("REQUEST_URI"))
        data.put("userHead_SCRIPT_NAME", userHead.getString("SCRIPT_NAME"))
        data.put("userHead_PATH_INFO", userHead.getString("PATH_INFO"))
        data.put("userHead_PATH_TRANSLATED", userHead.getString("PATH_TRANSLATED"))
        data.put("userHead_PHP_SELF", userHead.getString("PHP_SELF"))
        data.put("userHead_REQUEST_TIME", userHead.getString("REQUEST_TIME"))

        // 返回
        result = (data.toJSONString)
      } catch {
        case _ : Throwable =>
      }
      result
    }

    val table_schema_string = "mcookie:userId,addtime,userCookie,ip,type,addProudct,addChannel,userHead_REDIRECT_UNIQUE_ID,userHead_REDIRECT_STATUS,userHead_UNIQUE_ID,userHead_HTTP_HOST,userHead_HTTP_X_REAL_IP,userHead_HTTP_X_FORWARDED_FOR,userHead_HTTP_CONNECTION,userHead_HTTP_USER_AGENT,userHead_HTTP_WEB_SERVER_TYPE,userHead_HTTP_WL_PROXY_CLIENT_IP,userHead_HTTP_X_FORWARDED_PROTO,userHead_HTTP_X_FORWARDED_CLUSTER,userHead_HTTP_VIA,userHead_HTTP_EAGLEEYE_TRACEID,userHead_HTTP_ALI_SWIFT_LOG_HOST,userHead_HTTP_X_CLIENT_SCHEME,userHead_HTTP_ALI_CDN_REAL_IP,userHead_HTTP_ACCEPT,userHead_HTTP_UPGRADE_INSECURE_REQUESTS,userHead_HTTP_COOKIE,userHead_HTTP_ACCEPT_LANGUAGE,userHead_HTTP_ACCEPT_ENCODING,userHead_PATH,userHead_SERVER_SIGNATURE,userHead_SERVER_SOFTWARE,userHead_SERVER_NAME,userHead_SERVER_ADDR,userHead_SERVER_PORT,userHead_REMOTE_ADDR,userHead_DOCUMENT_ROOT,userHead_SERVER_ADMIN,userHead_SCRIPT_FILENAME,userHead_REMOTE_PORT,userHead_REDIRECT_QUERY_STRING,userHead_REDIRECT_URL,userHead_GATEWAY_INTERFACE,userHead_SERVER_PROTOCOL,userHead_REQUEST_METHOD,userHead_QUERY_STRING,userHead_REQUEST_URI,userHead_SCRIPT_NAME,userHead_PATH_INFO,userHead_PATH_TRANSLATED,userHead_PHP_SELF,userHead_REQUEST_TIME"

    val table_name = table_schema_string.split(":")(0)
    val schemaString = table_schema_string.split(":")(1)
    val schema =StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    ///datahouse/ods/mongo/mcookie/mcookie-201707.json
    val json_rdd = sc.textFile("/datahouse/ods/mongo/mcookie/mcookie-"+date_etl+".json").map(x=>x.replaceAll("\\\\r\\\\n"," ").replaceAll("\\\\n"," ").replaceAll("\\\\r"," "))
      .map(x=>stringToJson(x))
    val mcookie = sqlContext.read.schema(schema).json(json_rdd)


    val mcookie_select = mcookie.select(mcookie.columns.map(c => regexp_replace(col(c),"\\`","_").alias(c)):_*)


    val expr = concat_ws("`", mcookie_select.columns.map(col): _*)

    if(fs.exists(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/")))
      fs.delete(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/"),true)

    mcookie_select.na.fill("NULL").select(expr).repartition(1).map(_.getString(0)).saveAsTextFile("/tmp/"+table_name+"_tmp/")

    if(fs.exists(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))){
      fs.delete(new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"),true)
    }

    fs.rename(new org.apache.hadoop.fs.Path("/tmp/"+table_name+"_tmp/part-00000.snappy"),new org.apache.hadoop.fs.Path("/user/hive/warehouse/tmp.db/"+table_name+"_tmp/part-00000.snappy"))

    println("table has done:"+table_name+" date:"+date_etl)
    sc.stop()
  }

}
