package sheshou.predict

import java.sql.DriverManager

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import sheshou.predict.WeeklyPrediction.{MakeWeeklyPrediction, PredictValue}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by suyu on 17-5-16.
  */
object MonthlyPrediction {

  case class MonthPredict(time_month:String,attack_type:String,real_count:Int,predict_count:Int)

  def PredictValue(inputs: Array[Row]): ArrayBuffer[MonthPredict]  ={
    //init
    var result = 0
    var resultList=  ArrayBuffer[MonthPredict]()
    var percent:Double = 0.0

    //we need 4 records to make a prediction
    if(inputs.length >= 1) {
      if(inputs.length>3) {
        for (j <- 3 until inputs.length) {
          for (i <- 0 until 3) {
            percent += inputs(j - i).getDouble(2) / inputs(j - i - 1).getDouble(2)

            println("j =" + j + "  get input :" + i + "input " + inputs(j - i).getDouble(2) + " " + inputs(j - i - 1).getDouble(2))

          }

          println("  get percentage : " + percent)
          result = (inputs(j).getDouble(2) * percent / 3.0).toInt

          val newInstance = MonthPredict(inputs(j).getString(4), inputs(j).getString(0), inputs(j).getDouble(2).toInt, result) //insert into array
          resultList.append(newInstance)
          //reset the change percentages
          percent = 0.0
        }
      }
      else{
      //calculate prediction for the first three inputs
      for (j <- 1 until inputs.length) {
        for (i <- 0 until j) {
          percent += inputs(j - i).getDouble(2) / inputs(j - i - 1).getDouble(2)

          println("j =" + j + "  get input :" + i + "input " + inputs(j - i).getDouble(2) + " " + inputs(j - i - 1).getDouble(2))

        }

        println("  get percentage : " + percent)
        result = (inputs(j).getDouble(2) * percent / j).toInt

        val newInstance = MonthPredict(inputs(j).getString(4), inputs(j).getString(0), inputs(j).getDouble(2).toInt, result)
        //insert into array
        resultList.append(newInstance)
        //reset the change percentages
        percent = 0.0
      }

      }
    }
    return resultList
  }

  def MakeMonthlyPrediction(hiveContext:HiveContext,col_name:String,url:String,username:String,password:String,tablename2:String): Unit ={


    //get mysql connection class
    Class.forName("com.mysql.jdbc.Driver")
    val mysqlurl = "jdbc:mysql://"+url+"?user="+username+"&password="+password
    //val mysqlurl ="jdbc:mysql://192.168.1.22:3306/log_info?"+"user="+username+"&password="+password//+"&useUnicode=true&amp&characterEncoding=UTF-8"

    println(mysqlurl)
    val conn = DriverManager.getConnection(mysqlurl)


    //get input table from hive
    val selectSQL = "select attack_type, year,sum(sum) as acc,month ,concat(year,month) as month_time from sheshou.attacktypestat where trim(attack_type) = '"+col_name+"'"+"group by year,month,attack_type "
    println(selectSQL)
    //get selected result
    val selectDF = hiveContext.sql(selectSQL).coalesce(1).orderBy("year","month")
    println("**************"+selectDF.count())

    selectDF.registerTempTable("temp")

    selectDF.foreach{line=>
      println(line)
    }


//    // get prediction results
    if(selectDF.count()>0)
    {
      val predictList = PredictValue(selectDF.collect())

      println("predict: "+ predictList.length)
      predictList.foreach{
        x=>
          //insert into prediction table
          val insertSQL = "Insert into "+tablename2+" values(0, \"0\",\""+x.time_month+"\",\""+col_name+"\","+x.real_count+","+x.predict_count+")"

          println(insertSQL)

          conn.createStatement.execute(insertSQL)
      }
    }

    conn.close()
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

    val conf = new SparkConf().setAppName("Hourly Prediction Application").setMaster("local[*]")
    conf.set("hive.metastore.uris", "thrift://192.168.1.23:9083")
    val sc = new SparkContext(conf)
    val hiveContext = new  HiveContext(sc)
    //get mysql connection class
    Class.forName("com.mysql.jdbc.Driver")
    val mysqlurl = "jdbc:mysql://"+url+"?user="+username+"&password="+password
    //val mysqlurl ="jdbc:mysql://192.168.1.22:3306/log_info?"+"user="+username+"&password="+password//+"useUnicode=true&amp;characterEncoding=UTF-8"

    println(mysqlurl)
    val conn = DriverManager.getConnection(mysqlurl)

    //truncate prediction table
    val truncateSQL = "truncate table "+ tablename2
    println(truncateSQL)
    conn.createStatement.execute(truncateSQL)

    //get input table from hive
    val typeSQL = "select attack_type from sheshou.attacktypestat group by attack_type"
    println(typeSQL)
    //get selected result
    val typeDF = hiveContext.sql(typeSQL)
    println("**************"+typeDF.count())
    typeDF.collect().foreach{
      x=>
        val colname = x.getString(0)
        val hc = new  HiveContext(sc)
        MakeMonthlyPrediction(hc,colname,url,username,password,tablename2)
    }

  }
}
