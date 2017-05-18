package sheshou.predict

import java.sql.DriverManager

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer
;

/**
  * Created by suyu on 17-5-16.
  */
object WeeklyPrediction {

  case class WeekPredict(time_week:String,attack_type:String,real_count:Int,predict_count:Int,year:String)
  def GetNextWeek(inputYear:String,inputMonth:String,inputDay:String): String ={

    val strDate = inputYear+"-"+inputMonth+"-"+inputDay+"00:00:00"

    val strMon= StringUtils.stripStart(inputMonth, "0")
    println("left pad :"+strMon.toInt)
    val date =  (new DateTime).withYear(inputYear.toInt)
      .withMonthOfYear(strMon.toInt)
      .withDayOfMonth(inputDay.toInt).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)

    val result = date.plusWeeks(1).toString("yyyy-MM-dd")
    println(result)
    return result
  }

  def PredictValue(inputs: Array[Row]): ArrayBuffer[WeekPredict]  ={
    //init
    var result = 0
    var resultList=  ArrayBuffer[WeekPredict]()
    var percent:Double = 0.0

    //we need 4 records to make a prediction
    if(inputs.length >= 4) {
      for (j <- 3 until inputs.length ) {
        for (i <- 1 until 3) {
          percent += inputs(j - i).getDouble(2)/ inputs(j - i -1).getDouble(2)

          println("j ="+j+"  get input :"+i+"input "+inputs(j - i).getDouble(2)+" "+inputs(j - i-1).getDouble(2))

        }

        println("  get percentage : "+percent)
        result = (inputs(j-1).getDouble(2)* percent / 2.0).toInt

        val newWeek:String = ((inputs(j).getInt(1)+1)%52).toString
        val newInstance = WeekPredict(newWeek, inputs(j).getString(0),0, result,inputs(j).getString(3))
        //insert into array
        resultList.append(newInstance)
        //reset the change percentages
        percent = 0.0
      }

      //calculate prediction for the first three inputs
      //first row
      val firstInstance =  WeekPredict(inputs(0).getInt(1).toString, inputs(0).getString(0),0, 0,inputs(0).getString(3))
      //second row
      val secondInstance =   WeekPredict(inputs(1).getInt(1).toString, inputs(1).getString(0),0, 0,inputs(0).getString(3))
      //third row
      val thirdInstance =   WeekPredict(inputs(2).getInt(1).toString, inputs(2).getString(0),0, inputs(0).getDouble(2).toInt,inputs(0).getString(3))
      val thirdprediction =  (inputs(1).getDouble(2))*(inputs(1).getDouble(2))/ (inputs(0).getDouble(2))
      //forth row
      val forthInstance =   WeekPredict(inputs(3).getInt(1).toString, inputs(2).getString(0),0, thirdprediction.toInt,inputs(0).getString(3))

      //insert into array
      resultList.append(firstInstance)
      resultList.append(secondInstance)
      resultList.append(thirdInstance)
      resultList.append(forthInstance)

    }

    if(inputs.length == 3) {
      //first row
      val firstInstance =  WeekPredict(inputs(0).getInt(1).toString, inputs(0).getString(0),0, 0,inputs(0).getString(3))
      //second row
      val secondInstance =   WeekPredict(inputs(1).getInt(1).toString, inputs(1).getString(0),0, 0,inputs(0).getString(3))
      //third row

      val thirdInstance =   WeekPredict(inputs(2).getInt(1).toString, inputs(2).getString(0),0, inputs(0).getDouble(2).toInt,inputs(0).getString(3))

      //insert into array
      resultList.append(firstInstance)
      resultList.append(secondInstance)
      resultList.append(thirdInstance)

    }
    if(inputs.length == 2) {
      //first row
      val firstInstance =  WeekPredict(inputs(0).getInt(1).toString, inputs(0).getString(0),0, 0,inputs(0).getString(3))
      //second row
      val secondInstance =   WeekPredict(inputs(1).getInt(1).toString, inputs(1).getString(0),0, 0,inputs(0).getString(3))

      //insert into array
      resultList.append(firstInstance)
      resultList.append(secondInstance)

    }
    if(inputs.length == 1) {

      //first row
      val firstInstance =  WeekPredict(inputs(0).getInt(1).toString, inputs(0).getString(0),0, 0,inputs(0).getString(3))

      //insert into array
      resultList.append(firstInstance)

    }
    return resultList
  }

  def MakeWeeklyPrediction(hiveContext:HiveContext,col_name:String,url:String,username:String,password:String,tablename2:String): Unit ={


    //get mysql connection class
    Class.forName("com.mysql.jdbc.Driver")
    val mysqlurl = "jdbc:mysql://"+url+"?user="+username+"&password="+password
    //val mysqlurl ="jdbc:mysql://192.168.1.22:3306/log_info?"+"user="+username+"&password="+password//+"&useUnicode=true&amp&characterEncoding=UTF-8"

    println(mysqlurl)
   // val conn = DriverManager.getConnection(mysqlurl)
   val conn =  DriverManager.getConnection("jdbc:mysql://" + url + "?useUnicode=true&characterEncoding=UTF-8",username,password);

    //get input table from hive
    val selectSQL = "select t.attack_type,t.week_time, sum(t.acc) ,t.year from (select attack_type, sum as acc ,year,weekofyear(concat(year, '-',month, '-',day,' ',hour, \":00:00\" )) as week_time from sheshou.attacktypestat)t  where trim(t.attack_type) = \'"+col_name+"\' group by t.year,t.week_time,t.attack_type "
    println(selectSQL)
    //get selected result
    val selectDF = hiveContext.sql(selectSQL).coalesce(1).orderBy("year","week_time")
    println("**************"+selectDF.count())

    selectDF.registerTempTable("temp")

    selectDF.foreach{line=>
      println(line)
    }


    // get prediction results
    if(selectDF.count()>0)
    {
      val predictList = PredictValue(selectDF.collect())

      println("predict: "+ predictList.length)
      predictList.foreach{
        x=>
          //insert into prediction table
          val insertSQL = "Insert into "+tablename2+" values(0, \"0\",\""+x.time_week+"\",\""+col_name+"\","+x.real_count+","+x.predict_count+","+x.year +")"

          println(insertSQL)

          conn.createStatement.execute(insertSQL)
      }
    }

    conn.close()
  }


  def UpdateWeeklyStat(hiveContext:HiveContext,col_name:String,url:String,username:String,password:String,tablename2:String): Unit ={


    //get mysql connection class
    Class.forName("com.mysql.jdbc.Driver")
    val mysqlurl = "jdbc:mysql://"+url+"?user="+username+"&password="+password+"&useUnicode=true&characterEncoding=UTF-8"
    //val mysqlurl ="jdbc:mysql://192.168.1.22:3306/log_info?"+"user="+username+"&password="+password//
    println(mysqlurl)
   // val conn = DriverManager.getConnection(mysqlurl)
    val conn =  DriverManager.getConnection("jdbc:mysql://" + url + "?useUnicode=true&characterEncoding=UTF-8",username,password);

    //get input table from hive
    val selectSQL = "select t.attack_type,t.week_time, sum(t.acc) ,t.year from (select attack_type, sum as acc ,year,weekofyear(concat(year, '-',month, '-',day,' ',hour, \":00:00\" )) as week_time from sheshou.attacktypestat)t  where trim(t.attack_type) = \'"+col_name+"\' group by t.year,t.week_time,t.attack_type "
    println(selectSQL)
    //get selected result
    val selectDF = hiveContext.sql(selectSQL).coalesce(1).orderBy("year","week_time")
    println("**************"+selectDF.count())

    // get prediction results
    if(selectDF.count()>0)
    {

      selectDF.collect().foreach{
        x=>
          //insert into prediction table
          val insertSQL = "update "+tablename2+" set real_count="+x.getDouble(2).toInt+" where time_week = \'"+x.getInt(1)+" \'and attack_type =\'"+col_name+"\'"

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
      MakeWeeklyPrediction(hc,colname,url,username,password,tablename2)
      UpdateWeeklyStat(hc,colname,url,username,password,tablename2)
  }

}

}
