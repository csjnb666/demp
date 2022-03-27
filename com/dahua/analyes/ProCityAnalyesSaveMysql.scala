package com.dahua.analyes

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ProCityAnalyesSaveMysql {
  /**
   * 将数据写入mysql
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //判断参数是否正确
    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath
          |""".stripMargin
      )
      sys.exit()
    }

    //创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName("ProCityAnalyes")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext


    //接收参数
    var Array(inputpath) = args
    //读取文件
    val df: DataFrame = spark.read.parquet(inputpath)
    //创建视图
    df.createTempView("log")
    //写sql语句
    var sql = "select provincename,cityname,count(*) num from log group by provincename,cityname"
    val resdf: DataFrame = spark.sql(sql)

    //读取到所有的资源文件
    val load: Config = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user",load.getString("jdbc.user"))
    pro.setProperty("driver",load.getString("jdbc.driver"))
    pro.setProperty("password",load.getString("jdbc.password"))

    resdf.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),pro)

    spark.stop()
    sc.stop()
  }
}
