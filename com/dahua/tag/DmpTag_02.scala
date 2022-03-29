package com.dahua.tag

import com.dahua.utils.TagUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DmpTag_02 {
  /**
   * 写入hbase
   * @param args
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """
          |缺少参数
          |inputPath appmapping stopword outputPath
        """.stripMargin)
      sys.exit()
    }

    val Array(inputPath, appmapping,stopword, outputPath) = args
    // Sparkconf对象。
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // SparkSession对象。
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    //连接Hbase，并创建表。
    val load: Config = ConfigFactory.load()
    val hbaseTableName: String = load.getString("hbase.table.name")
    val configuration: Configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
    val hbConn: Connection = ConnectionFactory.createConnection(configuration)
    val hbAdmin: Admin = hbConn.getAdmin
    if(!hbAdmin.tableExists(TableName.valueOf(hbaseTableName))){
      println("表不存在，创建中****")
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val columnFamily = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(columnFamily)
      hbAdmin.createTable(tableDescriptor)
      hbAdmin.close()
      hbConn.close()
    }

    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    // 读取 app名称。
    val appMap: RDD[String] = sc.textFile(appmapping)
    val appMap1: Map[String, String] = appMap.map(line => {
      val words: Array[String] = line.split("[:]", -1)
      (words(0), words(1))
    }).collect().toMap

    // 读取停用词。
    val stopWrodRDD: RDD[String] = sc.textFile(stopword)
    val stopWord2Map: Map[String, Int] = stopWrodRDD.map(line => {
      val field: Array[String] = line.split(",")
      (field(0), 1)
    }).collect().toMap

    // 使用广播变量。将appMapping进行广播。
    val broadCastAppMap: Broadcast[Map[String, String]] = sc.broadcast(appMap1)
    // 将停用词使用广播变量进行广播。
    val stopWordVal: Broadcast[Map[String, Int]] = sc.broadcast(stopWord2Map)

    // 最终的效果。
    // imei：3472934723472934，LC01->1,LNbanner->1,APP爱奇艺->1`CNxxx->1,D00010001->1,D00020001>1
    // key ->Id：   value-> List(map)

    // key如何获得。
    val df: DataFrame = spark.read.parquet(inputPath)
    val tagFilter: Dataset[Row] = df.where(TagUtils.tagUserIdFilterParam)
    val tag: Dataset[(String, List[(String, Int)])] = tagFilter.map(r => {
      // 广告位标签。
      val asdTag: Map[String, Int] = AgsTags.makeTags(r)
      //app标签。
      val appTag: Map[String, Int] = AppTags.makeTags(r, broadCastAppMap.value)
      // 设备标签
      val driverTag: Map[String, Int] = DriverTags.makeTags(r)
      // 关键字标签
      val keywordTag: Map[String, Int] = KeyTags.makeTags(r, stopWordVal.value)

      (TagUtils.getUserId(r)(0), (asdTag ++ appTag ++ driverTag ++ keywordTag).toList)

    })
    // imei：3472934723472934  List((LC1 ,1)(lN"插图"，1)，(app小鳄鱼，1))
    // imei：3472934723444446  List((LC1 ,1)(lN"插图"，1)，(app小鳄鱼，1))
    // imei：3472934723472934  List((LC1 ,1)(lN"插图"，1)，(app小鳄鱼，1))
    // 结果：
    // imei：3472934723472934  List((LC1 ,2)(lN"插图"，2)，(app小鳄鱼，2))
    tag.rdd.reduceByKey((list1,list2)=>{
      // List ((LC1 ,1)(lN"插图"，1)，(app小鳄鱼，1)(LC1 ,1)(lN"插图"，1)，(app小鳄鱼，1))
      // Map[(LC1,List(LC1,1),(LC1,1)),]
      // Map[lN"插图,List(lN"插图,1)(lN"插图,1)]
      (list1++list2).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }).map{
      case(userId,userTags)=>{
        val put = new Put(Bytes.toBytes(userId))// 使用userId，当做rowkey
        val tags: String = userTags.map(t=>t._1+","+t._2).mkString(",")
        var day = "2020-09-30"
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"day${day}"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)

  }
}
