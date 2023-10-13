package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.{Database, Table}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SaveMode, SparkSession, catalog}

import java.util.Properties
import scala.beans.BeanProperty

class Person extends Serializable {
  @BeanProperty
  var name: String = ""

  @BeanProperty
  var age: Int = 0

}

// an UDF
class MyAggFun extends   UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    // ooxx(score)
    StructType.apply(Array(StructField.apply("score",IntegerType,false)))
  }

  override def bufferSchema: StructType = {
    //avg  sum / count = avg
    StructType.apply(Array(
      StructField.apply("sum",IntegerType,false),
      StructField.apply("count",IntegerType,false)
    ))
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //组内，一条记录调用一次
    buffer(0) = buffer.getInt(0) +  input.getInt(0)  //sum
    buffer(1) = buffer.getInt(1) + 1  //count

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  override def evaluate(buffer: Row): Double = {  buffer.getInt(0) / buffer.getInt(1)  }
}




object sqlBasic {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test")
    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      //      .appName("test")
      //      .master("local")
      .enableHiveSupport()   // catalog: a kind of data structure which is used to save table by spark itself
      //具体实现有InMemoryCatalog和HiveExternalCatalog两种。
      .getOrCreate()
    //show some information about the database(information in catalog)
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    val databases: Dataset[Database] = session.catalog.listDatabases()
    databases.show()

    val tables: Dataset[Table] = session.catalog.listTables()
    tables.show()

    val functions: Dataset[catalog.Function] = session.catalog.listFunctions()
    functions.show(999, true)


    //read data from json, and schema is generated automatically
    val df: DataFrame = session.read.json("src/main/resources/spark_data/json")
    df.show()
    df.printSchema()

    df.createTempView("ooxx")



    //A REPL SYSTEM
    import scala.io.StdIn._
    while (true) {
      val sql:String = readLine("input your sql:")
      session.sql(sql).show()
    }

    import session.implicits._

    //read data from an array
    val dataDF: DataFrame = List(
      "hello world",
    "hello world",
    "hello msb",
    "hello world",
    "hello world",
    "hello spark",
    "hello world",
    "hello spark",
    ).toDF("line")

    dataDF.createTempView("ooxx")

    val df_from_array: DataFrame = session.sql("select * from ooxx")
    df_from_array.show()
    df.printSchema()

    println("--------------------")
    session.sql("select word, count(*) from (select explode(split(line, ' ')) as word from ooxx) as tt group by tt.word").show()

    val res: DataFrame = dataDF.selectExpr("select explode(split(line, ' ')) as word").groupBy("word").count()

    println("-----------------------------")

    res.write.mode(SaveMode.Append).parquet("")

    println("-----------------------------")

    val frame: DataFrame = session.read.parquet("")

    frame.show()
    frame.printSchema()

    /*
基于文件的行式：
session.read.parquet()
session.read.textFile()
session.read.json()
session.read.csv()
读取任何格式的数据源都要转换成DF
res.write.parquet()
res.write.orc()
res.write.text()
*/
    session.read.textFile("")


    //dataframe is made of data and metadata. dataset can be manipulated as collection and sql.
    // relationship: dataset provide a off-heap memory optimization.A DataFrame is a Dataset organized into named columns.
    //The first way
    val ds1: Dataset[String] = session.read.textFile("") //data
    val person: Dataset[(String, Int)] = ds1.map(line => {
      val strs: Array[String] = line.split(" ")
      (strs(0), strs(1).toInt)
    })(Encoders.tuple(Encoders.STRING, Encoders.scalaInt))

    val cperson: DataFrame = person.toDF("name", "age")
    cperson.show()
    cperson.printSchema()

    // the second way: bean rdd + javabean
    val rdd: RDD[String] = sc.textFile("src/main/resources/spark_data")
    val rddBean: RDD[Person] = rdd.map(_.split(" "))
      .map(arr => {
        val p = new Person //no arguments
        p.setName(arr(0))
        p.setAge(arr(1).toInt)
        p
      })
    val df2: DataFrame = session.createDataFrame(rddBean, classOf[Person])
    df2.show()
    df2.printSchema()









  }

  def sql_JDBC(): Unit ={
    val ss: SparkSession = SparkSession
       .builder()
       .appName("JDBCTest")
       .master("local")
       .config("spark.sql.shuffle.partitions", "1")
       .getOrCreate()
    val sc: SparkContext = ss.sparkContext
    sc.setLogLevel("INFO")

    val pro = new Properties()
    pro.put("url","jdbc:mysql://192.168.150.99/spark")
    pro.put("user","root")
    pro.put("password","hadoop")
    pro.put("driver","com.mysql.jdbc.Driver")
    //get data from designated databases and tables
    val usersDF: DataFrame = ss.read.jdbc(pro.get("url").toString,"users",pro)
    val scoreDF: DataFrame = ss.read.jdbc(pro.get("url").toString,"score",pro)
    usersDF.select("*").where("xx > 10")

    val resDF: DataFrame = ss.sql("select  userstab.id,userstab.name,userstab.age, scoretab.score   from    userstab join scoretab on userstab.id = scoretab.id")
    resDF.show()
    resDF.printSchema()

    println(resDF.rdd.partitions.length)
    val resDF01: Dataset[Row] = resDF.coalesce(1) //narrow down the DataFrame partitions
    println(resDF01.rdd.partitions.length)

    resDF.write.jdbc(pro.get("url").toString,"bbbb",pro)

    //什么数据源拿到的都是DS/DF
    //    val jdbcDF: DataFrame = ss.read.jdbc(pro.get("url").toString,"ooxx",pro)
    //    jdbcDF.show()
    //
    //    jdbcDF.createTempView("ooxx")
    //
    //    ss.sql(" select * from ooxx  ").show()

    //    jdbcDF.write.jdbc(pro.get("url").toString,"xxxx",pro)

  }

  def hive_standalone(): Unit ={
    val ss: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("sdsfd")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.sql.warehouse.dir", "d:/spark/warehouse") //if no hive exist spark will create a metadata file in the designated directory
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = ss.sparkContext
    import ss.sql



    ss.catalog.listTables().show()
    sql("create database msb")
    sql("create table msb.xxxx (name string,age int)")
    sql("create table  xxxx (name string,age int)")

    sql("insert into msb.xxxx values ('zhangsan',18),('lisi',22)")
    sql("insert into  xxxx values ('zhangsan',18),('lisi',22)")
  }


  def sql_onhive(): Unit ={
    val ss: SparkSession = SparkSession
      .builder()
      .appName("test on hive ")
      .master("local")
      .config("hive.metastore.uris", "thrift://node01:9083") //connect to metastore service
      .enableHiveSupport()
      .getOrCreate()
    val sc: SparkContext = ss.sparkContext

    sc.setLogLevel("ERROR")

    import ss.implicits._

    val df01: DataFrame = List(
      "zhangsan",
      "lisi"
    ).toDF("name")
    df01.createTempView("ooxx")

    //    ss.sql("create table xxoo ( id int)")  //DDL

    ss.sql("insert into xxoo values (3),(6),(7)")  //DML  数据是通过spark自己和hdfs进行访问
    df01.write.saveAsTable("oxox")

    ss.catalog.listTables().show()  //能不能看到表？

    //如果没有hive的时候，表最开始一定是  DataSet/DataFrame
    //    ss.sql("use default")
    val df: DataFrame = ss.sql("show tables")
    df.show()

  }


  def UDF_demo(): Unit ={
    val ss: SparkSession = SparkSession
      .builder()
      .appName("functions")
      .master("local")
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val dataDF: DataFrame = List(
      ("A", 1, 90),
      ("B", 1, 90),
      ("A", 2, 50),
      ("C", 1, 80),
      ("B", 2, 60)
    ).toDF("name", "class", "score")

    dataDF.createTempView("users")


    ss.sql("select *, " +
      "count(score) over (partition by class) as num " +
      "from users").show()

    ss.sql("select  class , count(score) as num   from users   group by class ").show()


    ss.sql("select * ," +
      " rank()  over(partition by class order by score desc  )  as rank ,   " +
      " row_number()  over(partition by class order by score desc  )  as number    " +
      "from users ").show()

    val res: DataFrame = ss.sql("  select ta.name  , ta.class  ,tb.score + 20 + 80   " +
      "from    " +
      "( select name , class  from users ) as ta   " +
      "join" +
      "( select name , score from users where score > 10) as tb  " +
      "on ta.name  =  tb.name  " +
      "where  tb.score > 60  ")
    //class student score          class
                          //-----> student
                                  //score
    ss.sql("select name, explode (split (concat case when class = 1 then 'AA' else 'BB' end, ' ', score), ' ') as ox from users ").show()

    //count how many people in the same rank.
    ss.sql("select case when score <= 100 and score >= 90 then 'good' when score < 90 and score >=80 then 'you' else 'bad' end" +
      "as ox, count(*) from users group by case when score <= 100 and score >= 90 then 'good' when score < 90 and score >=80 then 'you' else 'bad' end").show()


    ss.sql("select * ," +
      "case " +
      "  when score <= 100 and score >=90  then 'good'  " +
      "  when score < 90  and score >= 80 then 'you' " +
      "  else 'cha' " +
      "end as ox " +
      "from users").show()


    ss.udf.register("ooxx",new MyAggFun)  // avg
    ss.sql("select name,    " +
      " ooxx(score)    " +
      "from users  " +
      "group by name  ")
      .show()

    ss.udf.register("ooxx",(x:Int)=>{x*10})
    ss.sql("select * ,ooxx(score) as ox from users").show()











  }


}
