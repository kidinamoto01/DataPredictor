package sheshou.predict

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Created by suyu on 17-4-18.
  */
object DailyPrediction {
  //define class
  case class HourPredict(id:Int,busiess_sys:String,time_hour:String,attack_type:String,real_count:Int,predict_count:Int)

  case class MidData(hour:String, vulnerability:Int,predict:Int)
  case class HourStatus(hour:String, vulnerability:Int)
  //define method
  def compareInputs(input: Array[HourStatus]): MidData = {
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

    var current_time = ""
    //打印变量长度
    println("****"+input.length)

    if(input.length >= 2){
      val st = input.takeRight(2)

      //有效数据
      if((st(1).vulnerability!=0)&&(st(1).vulnerability!= 0)){

        println("st1"+st(1).vulnerability+"st(0)"+st(0).vulnerability)
        //计算增长率
        increase = (st(1).vulnerability-st(0).vulnerability).toDouble/st(0).vulnerability.toDouble
        current_time = st(1).hour
        current = st(1).vulnerability
        //预测下一个
        next = (st(1).vulnerability.toDouble *(1.0+increase)).toInt
      }

    }

    return MidData(current_time, current,next)

  }

  def main(args: Array[String]) {
    val logFile = "/usr/local/share/spark-2.1.0-bin-hadoop2.6/README.md" // Should be some file on your system
    val filepath = "/Users/b/Documents/andlinks/sheshou/log/0401log3(1).txt"
    val middlewarepath = "hdfs://192.168.1.21:8020/user/root/test/webmiddle/20170413/web.json"
    val hdfspath = "hdfs://192.168.1.21:8020/user/root/test/windowslogin/20170413/windowslogin"

    val conf = new SparkConf().setAppName("Daily Prediction Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //create hive context
    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    val conn = DriverManager.getConnection("jdbc:hive2://192.168.1.23:10000/sheshou", "admin", "123456")

    //get input table
    val source: ResultSet = conn.createStatement
      .executeQuery("SELECT time_day,mal_operation FROM dayly_stat ")
    //fetch all the data
    val fetchedSrc = mutable.MutableList[HourStatus]()
    while(source.next()) {
      var rec = HourStatus(
        source.getString("time_hour"),
        source.getInt("weak_password")
      )
      fetchedSrc += rec
    }

    val predict = compareInputs(fetchedSrc.toArray)

    println("predict: "+ predict.vulnerability)
    //get the target table
    val res: ResultSet = conn.createStatement
      .executeQuery("SELECT id FROM prediction_dayly_stat")

    //fetch all the data
    val fetchedRes = mutable.MutableList[HourPredict]()


    val insertSQL = "Insert into table sheshou.prediction_dayly_stat values ( "+predict.predict+",\"N/A\","+predict.hour+",\"mal_operation\","+predict.vulnerability+","+predict.predict+")"

    println(insertSQL)

    conn.createStatement.execute(insertSQL)

  }
}
