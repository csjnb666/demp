package com.dahua.tools

import com.dahua.utils.{LogSchema, NumFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object LogParquet1 {

  def main(args: Array[String]): Unit = {
    //判断参数是否正确
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath outputpath
      """.stripMargin)
      sys.exit()
    }

    //创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[*]").getOrCreate()
    var sc = spark.sparkContext

    import spark.implicits._

    //接收参数
    var Array(inputpath, outputpath) = args

    //读取参数
    val line: RDD[String] = sc.textFile(inputpath)
    val logdata: RDD[Array[String]] = line.map(_.split(",", -1)).filter(_.length >= 85) //-1表示读取到最后一位
    val row: RDD[Row] = logdata.map(arr => {
      Row(
        arr(0),
        NumFormat.toint(arr(1)),
        NumFormat.toint(arr(2)),
        NumFormat.toint(arr(3)),
        NumFormat.toint(arr(4)),
        arr(5),
        arr(6),
        NumFormat.toint(arr(7)),
        NumFormat.toint(arr(8)),
        NumFormat.todouble(arr(9)),
        NumFormat.todouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        NumFormat.toint(arr(17)),
        arr(18),
        arr(19),
        NumFormat.toint(arr(20)),
        NumFormat.toint(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        NumFormat.toint(arr(26)),
        arr(27),
        NumFormat.toint(arr(28)),
        arr(29),
        NumFormat.toint(arr(30)),
        NumFormat.toint(arr(31)),
        NumFormat.toint(arr(32)),
        arr(33),
        NumFormat.toint(arr(34)),
        NumFormat.toint(arr(35)),
        NumFormat.toint(arr(36)),
        arr(37),
        NumFormat.toint(arr(38)),
        NumFormat.toint(arr(39)),
        NumFormat.todouble(arr(40)),
        NumFormat.todouble(arr(41)),
        NumFormat.toint(arr(42)),
        arr(43),
        NumFormat.todouble(arr(44)),
        NumFormat.todouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        NumFormat.toint(arr(57)),
        NumFormat.todouble(arr(58)),
        NumFormat.toint(arr(59)),
        NumFormat.toint(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        NumFormat.toint(arr(73)),
        NumFormat.todouble(arr(74)),
        NumFormat.todouble(arr(75)),
        NumFormat.todouble(arr(76)),
        NumFormat.todouble(arr(77)),
        NumFormat.todouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        NumFormat.toint(arr(84))
      )
    })

    val df: DataFrame = spark.createDataFrame(row, LogSchema.structType)
    df.write.parquet(outputpath)
    spark.stop()
    sc.stop()
  }
}
