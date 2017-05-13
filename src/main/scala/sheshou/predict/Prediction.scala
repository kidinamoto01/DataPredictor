package sheshou.predict

import java.sql.DriverManager

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import sheshou.predict.DailyPrediction.{MidData, compareInputs}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by suyu on 17-5-13.
  */
object Prediction {
  def PredictValue(inputs: Array[Row]): ArrayBuffer[MidData]  ={
    //init
    var result = 0
    var resultList=  ArrayBuffer[MidData]()
    var percent:Double = 0.0

    //we need 4 records to make a prediction
    if(inputs.length >= 4) {
      for (j <- 4 until inputs.length ) {
        for (i <- 1 until 4) {
          percent += (inputs(j - i).getLong(1).toInt - inputs(j - i-1).getLong(1).toInt).toDouble / inputs(j - i -1).getLong(1).toInt.toDouble

          println("j ="+j+"  get input :"+i+"date"+inputs(j).getString(2) )
        }
        result = (inputs(j).getLong(1).toInt * percent / 3.0).toInt
        val newInstance = MidData(inputs(j).getString(2), inputs(j).getLong(1).toInt, result)
        //insert into array
        resultList.append(newInstance)
      }
    }
    return resultList
  }
  def ComputeInputs(input: Array[Row]): ArrayBuffer[MidData] =  {

    var resultList=  ArrayBuffer[MidData]()

    //初始化变量
    var current = 0
    var next = 0
    //increase percentage
    var increase:Double = 0

    var current_time = ""
    //打印变量长度
    println("****"+input.length)
//make prediction based on 3 previous records

    if(input.length >= 4){

      for (i <- 0 until input.length-1){
        val firstElt = input(i)
        if( i+1 < input.length){
          val secondElt = input(i+1)
          //有效数据
          if(firstElt.getLong(1).toInt!=0){
            println("second  "+secondElt.getLong(1).toInt+" first  "+firstElt.getLong(1).toInt)
            //计算增长率
            increase = (secondElt.getLong(1).toInt-firstElt.getLong(1).toInt).toDouble/firstElt.getLong(1).toDouble
            current_time = secondElt.getString(0)
            current = secondElt.getLong(1).toInt
            //预测下一个
            next = (secondElt.getLong(1).toDouble *(1.0+increase)).toInt
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
      current = st(0).getLong(1).toInt
      current_time = st(0).getString(0)
      next = st(0).getLong(1).toInt

      //add to result list
      val newInstance= MidData(current_time, current,next)
      resultList.append(newInstance)
    }

    return resultList
  }

  def MakeDaylyPrediction(hiveContext:HiveContext,col_name:String,url:String,username:String,password:String,tablename2:String): Unit ={

    //get mysql connection class
    Class.forName("com.mysql.jdbc.Driver")
    val connectionString = "jdbc:mysql://"+url+"?user="+username+"&password="+password
    val mysqlurl ="jdbc:mysql://192.168.1.22:3306/log_info?"+"user="+username+"&password="+password//+"&useUnicode=true&amp&characterEncoding=UTF-8"

    println(mysqlurl)
    val conn = DriverManager.getConnection(mysqlurl)


    //get input data from Hive
//    val selectSQL = "select attack_type, count(sum) as acc ,year,month,day from sheshou.attacktypestat where trim(attack_type) = '"+col_name+"'"+
//      " group by year,month, day,attack_type  SORT BY year asc, month asc,day asc"
    val selectSQL = " select attack_type, count(sum) as acc ,concat(year, '-',month, '-',day,' ', \"00:00:00\" ) as hourly_time from sheshou.attacktypestat where trim(attack_type) = '"+col_name+"'"+
      " group by year,month, day,attack_type  SORT BY hourly_time  asc"
    println(selectSQL)
    //get selected result

    val selectDF = hiveContext.sql(selectSQL)
    println("**************"+selectDF.count())
    selectDF.foreach{line=>
      println(line)
    }
//
   selectDF.registerTempTable("temp")
//    val transSQL = "select concat(year,'-',month,'-',day,' ', \"00:00:00\") as hour,acc from temp "
//    val transDF = hiveContext.sql(transSQL)
//    transDF.foreach{line=>
//      println(line)
//    }
    // get prediction results
    if(selectDF.count()>0)
    {
      val predictList =PredictValue(selectDF.coalesce(1).collect())

      println("predict: "+ predictList.length)
      predictList.foreach{
        x=>
//          //insert into prediction table
          val insertSQL = "Insert into "+tablename2+" values( 0,\"0\",\""+x.hour+"\",\""+col_name+"\","+x.vulnerability+","+x.predict+")"
//
          println(insertSQL)
//
//          conn.createStatement.execute(insertSQL)
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
    val connectionString = "jdbc:mysql://"+url+"?user="+username+"&password="+password
    val mysqlurl ="jdbc:mysql://192.168.1.22:3306/log_info?"+"user="+username+"&password="+password//+"useUnicode=true&amp;characterEncoding=UTF-8"

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
       MakeDaylyPrediction(hc,colname,url,username,password,tablename2)
    }

  }

}
