dbutils.widgets.text("table","default");
val table=(dbutils.widgets.get("table"))
//spark.sql(s"msck repair table $table")
val partition=spark.sql(s"select max(batch_id) from $table").collect()
var partition_list:String=partition(0).toString()
//  for (partition <- partitions_df)
// { 
//    partition_list=partition(0).toString()
//     //     partition_list+=","
//  }
partition_list=partition_list.slice(1,partition_list.length - 1)

dbutils.notebook.exit(partition_list)
