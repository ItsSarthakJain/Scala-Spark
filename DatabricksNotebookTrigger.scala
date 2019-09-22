var table_list = 
Seq("","","").toDF("table_name")
table_list.select("table_name").collect.map(i=>
 {
     val table=i(0)
     var table_path=table.toString().replaceAll("_","-")
     println(s"$table-reconsile")
     dbutils.notebook.run(s"path_of_notebook",0)
 }
)
