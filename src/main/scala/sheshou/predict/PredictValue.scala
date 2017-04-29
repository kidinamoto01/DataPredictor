package sheshou.predict

import java.sql.{DriverManager, ResultSet}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Created by suyu on 17-4-14.
  */
object PredictValue {

  //define class
  case class HourPredict(id:Int,busiess_sys:String,time_hour:String,attack_type:String,real_count:Int,predict_count:Int)

  case class MidData(hour:String, vulnerability:Int,predict:Int)
  case class HourStatus(hour:String, vulnerability:Int)
  //define method
  def compareInputs(input: Array[HourStatus]): MidData = {
    var result:MidData =MidData("",0,0)

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
    else{

      val st = input.take(1)
      current = st(0).vulnerability
      current_time = st(0).hour
      next = st(0).vulnerability
    }

    return MidData(current_time, current,next)

  }

  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <databaseurl> is a list of one or more kafka topics to consume from
                            |    <username> is a list of one or more kafka topics to consume from
                            |  <password> is a list of one or more kafka topics to consume from
                            |  <tablename1>
                            |  <col_name>
                            |  <tablename2>
        """.stripMargin)
      System.exit(1)
    }


    val Array(url,username,password,tablename1,col_name,tablename2) = args
    println(url)
    println(username)
    println(password)

    println(tablename1)
    println(col_name)
    println(tablename2)

    val conf = new SparkConf().setAppName("Hourly Prediction Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //create hive context
    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    Class.forName("com.mysql.jdbc.Driver")
    //val connectionString = "jdbc:mysql://192.168.1.22:3306/log_info?user=root&password=andlinks"
    val connectionString = "jdbc:mysql://"+url+"?user="+username+"&password="+password
    val conn = DriverManager.getConnection(connectionString)

    //get input table
    val sqlQuery = "SELECT time_hour,"+col_name+" FROM "+tablename1
    println(sqlQuery)
    val source: ResultSet = conn.createStatement.executeQuery(sqlQuery)
    //fetch all the data
    val fetchedSrc = mutable.MutableList[HourStatus]()
    while(source.next()) {
      var rec = HourStatus(
        source.getString("time_hour"),
        source.getInt(col_name)
      )
      fetchedSrc += rec
    }

    val predict = compareInputs(fetchedSrc.toArray)

    println("predict: "+ predict.vulnerability)

    val insertSQL = "Insert into "+tablename2+" values( 0,\"0\",\""+predict.hour+"\",\""+col_name+"\","+predict.vulnerability+","+predict.predict+")"

    println(insertSQL)

    conn.createStatement.execute(insertSQL)

  }

}
