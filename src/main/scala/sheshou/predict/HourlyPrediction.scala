package sheshou.predict

import java.sql.{DriverManager, ResultSet}

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

/**
  * Created by suyu on 17-4-14.
  * predict value of hourly_stat
  * insert a new line in prediction hourly stat
  */
object HourlyPrediction {

  //define class
  case class HourPredict(id:Int,busiess_sys:String,time_hour:String,attack_type:String,real_count:Int,predict_count:Int)

  case class MidData(hour:String, vulnerability:Int,predict:Int)
  case class HourStatus(hour:String, vulnerability:Int)

  //get the next hour in string
  def GetNextHour(inputYear:String,inputMonth:String,inputDay:String,inputHour:String): String ={

    val strDate = inputYear+"-"+inputMonth+"-"+inputDay+" "+inputHour+":00:00"

    val strMon= StringUtils.stripStart(inputMonth, "0")
    println("left pad :"+strMon.toInt)
    val date =  (new DateTime).withYear(inputYear.toInt)
      .withMonthOfYear(strMon.toInt)
      .withDayOfMonth(inputDay.toInt).withHourOfDay( inputHour.toInt).withMinuteOfHour(0).withSecondOfMinute(0)

    val result = date.plusHours(1).toString("yyyy-MM-dd HH:mm:ss")
    println(result)
    return result
  }

  def PredictValue(inputs: Array[Row]): ArrayBuffer[MidData]  ={
    //init
    var result = 0
    var resultList=  ArrayBuffer[MidData]()
    var percent:Double = 0.0
    if(inputs.length >= 1) {
      //we need 4 records to make a prediction
      if (inputs.length >= 4) {
        for (j <- 3 until inputs.length) {
          for (i <- 1 until 3) {
            percent += inputs(j - i- 1).getDouble(1) / inputs(j - i ).getDouble(1)

            println("j =" + j + "  get input :" + i + "input " + inputs(j - i).getDouble(1) + " " + inputs(j - i - 1).getDouble(1))

          }

          println("  get percentage : " + percent)
          result = (inputs(j - 1).getDouble(1).toInt * percent / 2.0).toInt
          println("  get year: " + inputs(j - 1).getString(2) + " get month " + inputs(j - 1).getString(3) + " get day " + inputs(j - 1).getString(4) + " get hour " + inputs(j - 1).getString(5))

          val nextHour = GetNextHour(inputs(j).getString(2), inputs(j).getString(3), inputs(j).getString(4), inputs(j).getString(5))
          println("next hour: " + nextHour)
          val newInstance = MidData(nextHour, 0, result)
          //insert into array
          resultList.append(newInstance)
          //reset the change percentages
          percent = 0.0
        }

        //calculate prediction for the first three inputs
        //first row
        val firstInstance =  MidData(inputs(0).getString(6), 0,0)
        //second row
        val secondInstance =   MidData(inputs(1).getString(6), 0,0)
        //third row
        val thirdInstance =   MidData(inputs(2).getString(6), 0,inputs(0).getDouble(1).toInt)

        //forth row
        val forthVal = inputs(1).getDouble(1)*inputs(1).getDouble(1)/inputs(0).getDouble(1)
        val forthInstance =  MidData(inputs(3).getString(6), 0,forthVal.toInt)
        //insert into array
        resultList.append(firstInstance)
        resultList.append(secondInstance)
        resultList.append(thirdInstance)
        resultList.append(forthInstance)

      }

      if(inputs.length == 3) {
        //first row
        val firstInstance =  MidData(inputs(0).getString(6), 0,0)
        //second row
        val secondInstance =   MidData(inputs(1).getString(6), 0,0)
        //third row
        val thirdInstance =   MidData(inputs(2).getString(6), 0,inputs(0).getDouble(1).toInt)

        //insert into array
        resultList.append(firstInstance)
        resultList.append(secondInstance)
        resultList.append(thirdInstance)

      }
      if(inputs.length == 2) {
        //first row
        val firstInstance =  MidData(inputs(0).getInt(1).toString, 0,0)
        //second row
        val secondInstance =   MidData(inputs(1).getInt(1).toString, 0,0)

        //insert into array
        resultList.append(firstInstance)
        resultList.append(secondInstance)

      }
      if(inputs.length == 1) {

        //first row
        val firstInstance =  MidData(inputs(0).getInt(1).toString, 0, 0)

        //insert into array
        resultList.append(firstInstance)

      }
    }
    return resultList
  }
  def MakeHourlyPrediction(hiveContext:HiveContext,col_name:String,url:String,username:String,password:String,tablename2:String): Unit ={


    //get mysql connection class
    Class.forName("com.mysql.jdbc.Driver")
    val mysqlurl = "jdbc:mysql://"+url+"?user="+username+"&password="+password
    //val mysqlurl ="jdbc:mysql://192.168.1.22:3306/log_info?"+"user="+username+"&password="+password//+"&useUnicode=true&amp&characterEncoding=UTF-8"

    println(mysqlurl)
   // val conn = DriverManager.getConnection(mysqlurl)
   val conn =  DriverManager.getConnection("jdbc:mysql://" + url + "?useUnicode=true&characterEncoding=UTF-8",username,password);

    //get input table from hive
    val selectSQL = "select attack_type, sum(sum) as acc,year,month,day,hour,concat(year,'-',month,'-',day,' ',hour, \":00:00\") as hour_time from sheshou.attacktypestat where trim(attack_type) = '"+col_name+"'"+"group by year,month, day,hour,attack_type "
    println(selectSQL)
    //get selected result
    val selectDF = hiveContext.sql(selectSQL).coalesce(1).orderBy("year","month","day","hour")
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
          val insertSQL = "Insert into "+tablename2+" values(0, \"0\",\""+x.hour+"\",\""+col_name+"\","+x.vulnerability+","+x.predict+")"

          println(insertSQL)

         conn.createStatement.execute(insertSQL)
      }
    }

    conn.close()
  }

  //update the sum of attacks in mysql
  def UpdateHourlyStat(hiveContext:HiveContext,col_name:String,url:String,username:String,password:String,tablename2:String): Unit ={


    //get mysql connection class
    Class.forName("com.mysql.jdbc.Driver")
    val mysqlurl = "jdbc:mysql://"+url+"?user="+username+"&password="+password
    //val mysqlurl ="jdbc:mysql://192.168.1.22:3306/log_info?"+"user="+username+"&password="+password//+"&useUnicode=true&amp&characterEncoding=UTF-8"

    println(mysqlurl)
    //val conn = DriverManager.getConnection(mysqlurl)
    val conn =  DriverManager.getConnection("jdbc:mysql://" + url + "?useUnicode=true&characterEncoding=UTF-8",username,password);

    //get input table from hive
    val selectSQL = "select attack_type, sum(sum) as acc,year,month,day,hour,concat(year,'-',month,'-',day,' ',hour, \":00:00\") as hour_time from sheshou.attacktypestat where trim(attack_type) = '"+col_name+"'"+"group by year,month, day,hour,attack_type "
    println(selectSQL)
    //get selected result
    val selectDF = hiveContext.sql(selectSQL)
    println("**************"+selectDF.count())

    // get total count results
    if(selectDF.count()>0)
    {

      println("predict: "+ selectDF.count())
      selectDF.collect().foreach{
        x=>
          //insert into prediction table
          val insertSQL = "update "+tablename2+" set real_count = " +x.getDouble(1).toInt+" where time_hour =\'"+x.getString(6)+"\' and attack_type =\'"+col_name+"\'"

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
        MakeHourlyPrediction(hc,colname,url,username,password,tablename2)
        UpdateHourlyStat(hc,colname,url,username,password,tablename2)
    }

  }

}
