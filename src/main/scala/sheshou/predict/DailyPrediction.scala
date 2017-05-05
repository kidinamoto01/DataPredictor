package sheshou.predict

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by suyu on 17-4-18.
  */
object DailyPrediction {
  //define class
  case class HourPredict(id:Int,busiess_sys:String,time_hour:String,attack_type:String,real_count:Int,predict_count:Int)

  case class MidData(hour:String, vulnerability:Int,predict:Int)
  case class HourStatus(hour:String, vulnerability:Int)
  //define method
  def compareInputs(input: Array[HourStatus]): ArrayBuffer[MidData] =  {

    var resultList=  ArrayBuffer[MidData]()

    //初始化变量
    var current = 0
    var next = 0
    //increase percentage
    var increase:Double = 0

    var current_time = ""
    //打印变量长度
    println("****"+input.length)

    if(input.length >= 2){

      for (i <- 0 until input.length-1){
        val firstElt = input(i)
        if( i+1 < input.length){
          val secondElt = input(i+1)
          //有效数据
          if(firstElt.vulnerability!=0){
            println("second  "+secondElt.vulnerability+" first  "+firstElt.vulnerability)
            //计算增长率
            increase = (secondElt.vulnerability-firstElt.vulnerability).toDouble/firstElt.vulnerability.toDouble
            current_time = secondElt.hour
            current = secondElt.vulnerability
            //预测下一个
            next = (secondElt.vulnerability.toDouble *(1.0+increase)).toInt
            println("next "+ next)
            val newInstance= MidData(current_time, current,next)
            //insert into array
            resultList.append(newInstance)
          }

        }/*else{
          // odd
          resultList.append( MidData(input(i).hour,input(i).vulnerability,input(i).vulnerability) )
        }*/

      }
    }
    else{

      //when there is only one line, we presume the data will remain the same
      val st = input.take(1)
      current = st(0).vulnerability
      current_time = st(0).hour
      next = st(0).vulnerability

      //add to result list
      val newInstance= MidData(current_time, current,next)
      resultList.append(newInstance)
    }

    return resultList
  }

  def main(args: Array[String]) {

    if (args.length < 6) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <databaseurl>  192.168.1.22:3306/log_info
                            |  <username>  root
                            |  <password> andlinks
                            |  <tablename1> hourly_stat
                            |  <col_name> attack
                            |  <tablename2> prediction_hourly_stat
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


    val conf = new SparkConf().setAppName("Daily Prediction Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    Class.forName("com.mysql.jdbc.Driver")
    //val connectionString = "jdbc:mysql://192.168.1.22:3306/log_info?user=root&password=andlinks"
    val connectionString = "jdbc:mysql://"+url+"?user="+username+"&password="+password

    val conn = DriverManager.getConnection(connectionString)

    //truncate prediction table
    val truncateSQL = "truncate table "+ tablename2
    println(truncateSQL)
    conn.createStatement.execute(truncateSQL)

    //get input table
    val source: ResultSet = conn.createStatement
      .executeQuery("SELECT time_day,"+col_name+"  FROM "+tablename1)
    //fetch all the data
    val fetchedSrc = mutable.MutableList[HourStatus]()
    while(source.next()) {
      var rec = HourStatus(
        source.getString("time_day"),
        source.getInt(col_name)
      )
      fetchedSrc += rec
    }


    // get prediction results
    val predictList = compareInputs(fetchedSrc.toArray)

    println("predict: "+ predictList.length)
    predictList.foreach{
      x=>
        //insert into prediction table
        val insertSQL = "Insert into "+tablename2+" values( 0,\"0\",\""+x.hour+"\",\""+col_name+"\","+x.vulnerability+","+x.predict+")"

        println(insertSQL)

        conn.createStatement.execute(insertSQL)
    }

  }
}
