package com.dahua.dahua.analyes

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object ProCityAnalyesForRDD {

  def main(args: Array[String]): Unit = {
    //判断参数
    if(args.length !=2){
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
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    var Array(inputpath,outputpath) =args

    val line: RDD[String] = sc.textFile(inputpath)
    val filed: RDD[Array[String]] = line.map(_.split(",", -1))
    val procityRdd: RDD[((String, String), Int)] = filed.filter(_.length >= 85).map(arr => {
      ((arr(24), arr(25)), 1)
    })

    //降维
    val reduceRdd: RDD[((String, String), Int)] = procityRdd.reduceByKey(_ + _)
    val rdd1: RDD[(String, (String, Int))] = reduceRdd.map(arr => {
      (arr._1._1, (arr._1._2, arr._2))
    })
    val num: Long = rdd1.map(x => {
      (x._1, 1)
    }).reduceByKey(_ + _).count()

    rdd1.partitionBy(new HashPartitioner(num.toInt)).saveAsTextFile(outputpath)

    spark.stop()
    sc.stop()
  }
}
