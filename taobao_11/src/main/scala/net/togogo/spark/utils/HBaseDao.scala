package net.togogo.spark.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import net.togogo.spark.config.ApplicationConfig

object HBaseDao {
  private lazy val connection: Connection = createHBaseConn()

  def createHBaseConn(): Connection = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ApplicationConfig.HBASE_ZK_HOSTS)
    conf.set("hbase.zookeeper.property.clientPort", ApplicationConfig.HBASE_ZK_PORT)
    conf.set("zookeeper.znode.parent", ApplicationConfig.HBASE_ZK_ZNODE)
    ConnectionFactory.createConnection(conf)
  }

  def getHTable(tableName: String): HTable = {
    connection.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
  }

  def insert(tableName: String, columnFamily: String, columns: Array[String], datas: Iterator[String]): Boolean = {
    var htable: HTable = null
    try {
      htable = getHTable(tableName)
      import java.util
      val puts = new util.ArrayList[Put]()
      val cfBytes = Bytes.toBytes(columnFamily)
      datas.foreach { data =>
        try {
          val jsonObj: JSONObject = JSON.parseObject(data)
          val userId = jsonObj.getString("userId")
          val orderTime = jsonObj.getString("orderTime")
          // 确保 rowkey 生成正确（userId 逆序 + orderTime）
          val rowKey = Bytes.toBytes(s"${if(userId!=null) userId.reverse else "unknown"}_$orderTime")
          val put = new Put(rowKey)
          columns.foreach { column =>
            val value = jsonObj.getString(column)
            // 防止 null 值导致 Bytes.toBytes 报错
            val bytesValue = if (value != null) Bytes.toBytes(value) else Bytes.toBytes("")
            put.addColumn(cfBytes, Bytes.toBytes(column), bytesValue)
          }
          puts.add(put)
        } catch {
          case e: Exception => e.printStackTrace() // 单条数据错误不影响整体
        }
      }
      if (!puts.isEmpty) {
        htable.put(puts)
      }
      true
    } catch {
      case e: Exception => e.printStackTrace(); false
    } finally {
      if (htable != null) htable.close()
    }
  }
}
