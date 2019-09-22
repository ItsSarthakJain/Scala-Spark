var new_batch=java.time.LocalDate.now.toString
new_batch=new_batch.replaceAll("-","")

var DF =Seq((null,null,null)).toDF("Table","Count","Update_timestamp")
DF=DF.select(DF.col("*")).where(DF.col("Table").leq(10))
var table_list = Seq(
"DATABASE|TABLE|TimestampCol",
).toDF("table_name")
table_list.select("table_name").collect.map(i=>
  {
      try
       {
      val line=i(0).toString()
      val table_map=line.split('.').map(_.trim)
      val Database=table_map(0)
      val Table=table_map(1)
      val Incremental_col=table_map(2)
      println(s"""select "$Table",count(*),max($Incremental_col) from $Database.$Table""")
      var temp_df=spark.sql(s"""select "$Table",count(*),cast(max($Incremental_col) as String) from $Database.$Table""").toDF()
      //temp_df.show()
      DF=DF.union(temp_df)
       }
    catch
      {
          case e: Exception => e.printStackTrace()
          None
      }
  }
)
//display(DF)
DF.coalesce(1).write.format("com.databricks.spark.csv").save(s"path/$new_batch")
