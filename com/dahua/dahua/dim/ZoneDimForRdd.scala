package com.dahua.dahua.dim

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ZoneDimForRdd {

  def main(args: Array[String]): Unit = {
    if (args.length != 2){
      println(
        """
          |inputpath outputpath
          |缺少参数
          |""".stripMargin)
      sys.exit()
    }

    //创建sparksession对象
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .master("local[1]")
      .appName("ProCityAnalyes")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import  spark.implicits._

    //接收参数
    var Array(inputPath, outputPath) = args
    val df: DataFrame = spark.read.parquet(inputPath)

    //获取字段
    val DimRdd: Dataset[((String, String), List[Double])] = df.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val appname: String = row.getAs[String]("appname")

      //将维度写到方法里边
      val ysqqs: List[Double] = DimZB.qqsRtp(requestmode, processNode)
      val cyjjs: List[Double] = DimZB.jingjiaRtp(iseffective, isbilling, isbid, iswin, adorderid)
      val ggzss: List[Double] = DimZB.ggzjRtp(requestmode, iseffective)
      val mjzss: List[Double] = DimZB.mjjRtp(requestmode, iseffective, isbilling)
      val ggxf: List[Double] = DimZB.ggcbRtp(iseffective, isbilling, iswin, winprice, adpayment)

      //将list集合里边的数据连接起来
      ((province, cityname), ysqqs ++ cyjjs ++ ggzss ++ mjzss ++ ggxf)
    })

    DimRdd.rdd.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreach(println)
  }
}
