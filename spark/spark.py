from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sqlContext.sql("select * from customer_transactions").collect()
ct = sqlContext.sql("select * from customer_transactions")
ct.printSchema()
ct_sum = ct.groupBy('cust_name').sum('total_cost')
sqlContext.sql("create table customer_spend(cust_name string, spend float)")
ct_sum.write.mode("append").saveAsTable("customer_spend")
ct_sum.write.save("customer_spend.json", format="json")
