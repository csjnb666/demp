package com.dahua.dim

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.codehaus.jackson.map.MapperConfig.ConfigFeature

import java.util.Properties

object ZoneDim {

  /**
   * 地域报表
   */
  def main(args: Array[String]): Unit = {
    /*
    判断参数
     */
    if (args.length != 2) {
      println(
        """
          |inputpath outputpath
          |缺少参数
          |""".stripMargin)
      sys.exit()
    }

    //创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).appName("ZoneDim").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //隐式转换
    import spark.implicits._
    //读取参数
    var Array(inputpath, outputpath) = args
    //读取数据
    val df: DataFrame = spark.read.parquet(inputpath)

    //创建表
    df.createTempView("dim")

    //写sql语句
    var sql =
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end) as ysqqs,
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) as yxqqs,
        |sum(case when requestmode =1 and processnode =3 then 1 else 0 end) as ggqqs,
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) as cyjjs,
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling=1 and iswin=1 then 1 else 0 end) as cycgs,
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as ggzsh,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as ggdjs,
        |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
        |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as ggxf,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as ggcb
        |from dim
        |group by
        |provincename,cityname
        |""".stripMargin

    //读取到所有的资源文件
    val df1: DataFrame = spark.sql(sql)

    val load: Config = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user", load.getString("jdbc.user"))
    pro.setProperty("driver", load.getString("jdbc.driver"))
    pro.setProperty("password", load.getString("jdbc.password"))

    df1.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"), load.getString("jdbc.tableName2"), pro)
    spark.stop()
    sc.stop()
  }
}
