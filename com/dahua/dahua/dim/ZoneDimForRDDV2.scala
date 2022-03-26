package com.dahua.dahua.dim


import com.dahua.dahua.bean.Logbean
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ZoneDimForRDDV2 {

  def main(args: Array[String]): Unit = {
    if (args.length != 3){
      println(
        """
          |inputpath appMapping outputpath
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
      .appName("ZoneDimForRDDV2")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import  spark.implicits._

    //接收参数
    var Array(inputPath,appMapping, outputPath) = args
  //读取映射文件 appmapping
  val appmap: Map[String, String] = sc.textFile(appMapping).map(line => {
    val arr: Array[String] = line.split("[:]", -1)
    (arr(0), arr(1))
  }).collect().toMap

    //广播变量 后续计算可以重复使用
    val appBro: Broadcast[Map[String, String]] = sc.broadcast(appmap)

    val log: RDD[String] = sc.textFile(inputPath)
    //过滤出不为空的appid
    val logrdd: RDD[Logbean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(Logbean(_)).filter(t => {
      !t.appid.isEmpty
    })

    logrdd.map(log=>{
      var appname: String = log.appname
      //如果appname等于空或者appname为空
      if (appname=="" ||appname.isEmpty){
        appname=appBro.value.getOrElse(log.appid,"不明确")
      }

      val ysqqs: List[Double] = DimZB.qqsRtp(log.requestmode, log.processnode)

      (appname,ysqqs)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreach(println)





  }
}
