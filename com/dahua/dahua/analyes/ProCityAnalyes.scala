package com.dahua.dahua.analyes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityAnalyes {

  def main(args: Array[String]): Unit = {
    //判断参数是否正确
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath outputpath
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

    import spark.implicits._

    //接收参数
    var Array(inputpath, outputpath) = args
    //读取文件
    val df: DataFrame = spark.read.parquet(inputpath)
    //创建视图
    df.createTempView("log")
    //写sql语句
    var sql = "select provincename,cityname,count(*) num from log group by provincename,cityname"
    val resdf: DataFrame = spark.sql(sql)
    val configuration: Configuration = sc.hadoopConfiguration

    //文件系统对象
    val fs: FileSystem = FileSystem.get(configuration)
    var path = new Path(outputpath)
    //如果路径已经存在 则删除
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    //coalesce缩减分区 partitionby让打出来的数据根据省市一阶一阶档次分明
    resdf.coalesce(1).write.partitionBy("provincename", "cityname").json(outputpath)
    spark.stop()
    sc.stop()
  }
}
