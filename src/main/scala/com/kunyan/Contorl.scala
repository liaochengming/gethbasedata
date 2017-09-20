package com.kunyan

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Get, Connection}

import scala.collection.mutable.ListBuffer
import scala.xml.XML

/**
 * Created by Administrator on 2017/7/25.
 *
 */
object Contorl {

  /**
   *
   * @param args
   *             1:配置文件路径
   *             2:表名
   *             3:rowkey
   *             4:列族=列名
   *             5:~
   *             .:.
   *             .:.
   *             .:.
   */
  def main(args: Array[String]) {

    val list = new ListBuffer[String]
    println("参数长度 "+ args.length)
    for(a <- args){
      println(a)
    }

    if(args.length < 4){

      println("输入条件有误，参数数量小于4个，格式如下：")
      println("path tableName rokey columnFamily=columnName ->后面跟多个需要查询的列族和列名")

    }else{

      val path = args(0)
      val tableName = args(1)
      val rowKey = args(2)

      val hbaseConn = initHbaseConn(path)
      val table = hbaseConn.getTable(TableName.valueOf(tableName))
      val get = new Get(rowKey.getBytes)

      for(index <- 3 until args.length){

        val arr = args(index).split("=")
        val columnFamily = arr(0)
        val columnName = arr(1)
        val result = table.get(get).getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))
        val resString = new String(result, "UTF-8")
        list.+=(s"列族 $columnFamily , 列名 $columnName , 查询结果是: $resString")
      }

      for(s <- list){
        println(s)
      }

    }


  }

  def initHbaseConn(path: String):Connection= {

    val xml = XML.loadFile(path)

    val hbaseConf = HBaseConfiguration.create

    hbaseConf.set("hbase.rootdir", (xml \ "hbase" \ "rootDir").text)
    hbaseConf.set("hbase.zookeeper.quorum", (xml \ "hbase" \ "ip").text)
    println("create connection")

    val connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)

    sys.addShutdownHook {
      connection.close()
    }

    println("Hbase connection created.")
    connection

  }

}
