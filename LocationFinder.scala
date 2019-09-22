var table_list=Seq("","","").toDF("table_name")
table_list.select("table_name").collect.map(i=>
  {
      val table=i(0)
      spark.sql("use DATABASE_NAME")
      if (spark.sql(s"SHOW TABLES LIKE '$table'").count() == 1) 
        {
            println(s"$table")
            var create_statement=spark.sql(s"show create table temp_$table").collect()
            val regex_Pattern= "(abfs).+?(?=\\')".r //----Pattern to find replace abfs with any common word in locations of tables
            var location=regex_Pattern.findAllIn(create_statement(0).toString()).mkString
            println(location)
        }
//       else
//         println(s"$table not found")
  }
)
