package com.dahua.tools

import com.dahua.bean.Logbean
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LogParquet2 {

  def main(args: Array[String]): Unit = {
    //判断参数是否正确
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath outputpath
          |""".stripMargin)
      sys.exit()
    }
    //创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //将自定义对象进行kryo序列化
    conf.registerKryoClasses(Array(classOf[Logbean]))
    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("LogParquet2")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    //接收参数
    var Array(inputpath, outputpath) = args

    //-1读取到最后一位 过滤出来数据长度大于85的
    val rdd: RDD[Array[String]] = sc.textFile(inputpath).map(_.split(",", -1)).filter(_.length >= 85)
    val rdd1: RDD[Logbean] = rdd.map(Logbean(_))
    val df: DataFrame = spark.createDataFrame(rdd1)
    df.write.parquet(outputpath)

    spark.stop()
    sc.stop()
  }
}
