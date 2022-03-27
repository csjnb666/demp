package com.dahua.task

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Task02 {

  def main(args: Array[String]): Unit = {
    //判断参数
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
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .appName("ProCityAnalyesForRDD")
      .master("local[1]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    var Array(inputpath, outputpath) = args

    val line: RDD[String] = sc.textFile(inputpath)
    val filed: RDD[Array[String]] = line.map(_.split(",", -1))
    val rdd1: RDD[((String, String), Int)] = filed.filter(_.length >= 85).map(arr => {
      ((arr(24), arr(25)), 1)
    })

    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_ + _).sortByKey()
    rdd2.saveAsTextFile(outputpath)

    spark.stop()
    sc.stop()
  }
}
