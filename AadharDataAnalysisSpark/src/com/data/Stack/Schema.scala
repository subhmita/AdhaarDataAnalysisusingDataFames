package com.data.Stack


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField,StructType}
import org.apache.spark.sql.types.{StringType,IntegerType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType

class Schema {
def createSchema(sqlContext:SQLContext):DataFrame={
   val colName = List(("date",StringType),("registrar",StringType),("private_agency",StringType),("state",StringType),("district",StringType),
                      ("sub_district",StringType),("pincode",StringType),("gender",StringType),("age",IntegerType),("aadhaar_generated",IntegerType),
                      ("rejected",IntegerType),("mobile_number",IntegerType),("email_id",IntegerType))
    val colStruct = colName.map(x=> StructField(x._1,x._2,true))
    val schema= StructType(colStruct)
    var dataframe = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], schema)

    val data = sqlContext.sparkContext.textFile("D:/Xebia/aadhaar_data.csv", 4)
    val rdd = data.map { x => {
                       val rowValue=Seq(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4),
                          x.split(",")(5),x.split(",")(6),x.split(",")(7),Integer.parseInt(x.split(",")(8)),Integer.parseInt(x.split(",")(9)),
                          Integer.parseInt(x.split(",")(10)),Integer.parseInt(x.split(",")(11)),Integer.parseInt(x.split(",")(12)))
          Row.fromSeq(rowValue)}             
              }
 
     dataframe = sqlContext.createDataFrame(rdd, schema)
     dataframe
  }
}