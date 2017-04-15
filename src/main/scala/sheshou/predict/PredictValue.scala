package sheshou.predict

import java.sql.{DriverManager, ResultSet}
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Created by suyu on 17-4-14.
  */
object PredictValue {

  //define class
  case class HourPredict(id:Int,busiess_sys:String,time_hour:String,attack_type:String,real_count:Int,predict_count:Int)

  case class HourStatus(id:Int, vulnerability:String)
  //define method
  def compareInputs(input: Array[HourPredict]): HourPredict = {
    var result:HourPredict =HourPredict(0,"","","",0,0)

    //初始化变量
    var id = 0
    var business = ""
    var hour = ""
    var attack = ""
    var current = 0
    var next = 0
    //increase percentage
    var increase:Double = 0

    //打印变量长度
    println("****"+input.length)

    if(input.length >= 2){
      val st = input.takeRight(2)
      //println(st(0).category+st(1).category+st(2).category)
      //有效数据

      if((st(1).real_count!=0)&&(st(1).real_count!= 0)){

        //id
        id = st(1).id+1
        //business
        business = st(1).busiess_sys

        //time_hour
        hour = st(1).time_hour

        //attack_type
        attack = st(1).attack_type

        //计算增长率
        increase = (st(1).real_count-st(0).real_count).toDouble/st(0).real_count.toDouble


        //预测下一个
        next = (st(1).real_count.toDouble *(1.0+increase)).toInt
      }

    }

    return HourPredict(id,business,hour,attack,current,next)

  }

  def main(args: Array[String]) {
    val logFile = "/usr/local/share/spark-2.1.0-bin-hadoop2.6/README.md" // Should be some file on your system
    val filepath = "/Users/b/Documents/andlinks/sheshou/log/0401log3(1).txt"
    val middlewarepath = "hdfs://192.168.1.21:8020/user/root/test/webmiddle/20170413/web.json"
    val hdfspath = "hdfs://192.168.1.21:8020/user/root/test/windowslogin/20170413/windowslogin"
    val conf = new SparkConf().setAppName("Offline Doc Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //create hive context
    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    val conn = DriverManager.getConnection("jdbc:hive2://192.168.1.23:10000/default", "admin", "123456")

    //get input table
    //get input table
    val source: ResultSet = conn.createStatement
      .executeQuery("SELECT * FROM hourly_stat")
    //fetch all the data
    val fetchedSrc = mutable.MutableList[HourPredict]()
    fetchedSrc += source.last()
    //get the target table
    val res: ResultSet = conn.createStatement
      .executeQuery("SELECT * FROM prediction_hourly_stat")

    //fetch all the data
    val fetchedRes = mutable.MutableList[HourPredict]()

    while(res.next()) {
      var rec = HourPredict(
        res.getInt("id"),
        res.getString("busiess_sys"),
        res.getString("time_hour"),
        res.getString("attack_type"),
        res.getInt("real_count"),
        res.getInt("predict_count")
      )
      fetchedRes += rec
    }
  }

}
