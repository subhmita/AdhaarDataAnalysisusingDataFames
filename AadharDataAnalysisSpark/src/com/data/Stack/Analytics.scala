package com.data.Stack



import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.FloatType

object Analytics {
  def main(args: Array[String]) {
    var schema = new Schema()
    var sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Aadhaar"))
    sc.setLogLevel("WARN")
    var sqlContext = new SQLContext(sc)
    var dataframe = schema.createSchema(sqlContext)
    dataframe.persist(StorageLevel.MEMORY_AND_DISK)

    //Checkpoint 2: SparkSQL Skills
    /*1*/ var schemaDispaly = dataframe.schema.foreach(x => print(x.name + "," + x.dataType + "  "))
    print("Registrar")
    /*2*/ var registrar = dataframe.select("registrar").distinct();
        print("Registrar"+registrar)

    /*3*/ var districtsStateWise = dataframe.select("state", "district").groupBy("state").
    agg(count(dataframe("district")).as("stateWiseDistrict"))
        print("districtsStateWise"+districtsStateWise)
    /*3*/ var subDistrictWise = dataframe.select("district", "sub_district").
    groupBy("district").agg(count(dataframe("sub_district")).as("sub_districtDistrictWise"))
            print("subDistrictWise"+subDistrictWise)

    var groupedData = dataframe.select("state", "gender").groupBy("state")
    /*4*/ var genderCount = groupedData.agg(count((when(col("gender") === "M", 0))).as("MaleCount"),
        count((when(col("gender") === "F", 0))).as("FemaleCount"))
                    print("genderCount"+genderCount)

    /*5*/ var agenciesCountState = dataframe.select("state", "private_agency").groupBy("state").
    agg(countDistinct("private_agency").as("agencyCount"))
       print("agenciesCountState"+agenciesCountState)

  
    //Checkpoint 3: Hive Skills
    /*1*/ var top3State = dataframe.select("aadhaar_generated", "state").groupBy("state").agg(sum("aadhaar_generated").as("sum")).sort(col("sum").desc).limit(3)
    /*2*/ var top3Agencies = dataframe.select("private_agency", "aadhaar_generated").groupBy("private_agency").agg(sum("aadhaar_generated").as("sum")).sort(col("sum").desc).limit(3)
    /*3*/ var emailMobileResidentCount = dataframe.select("email_id", "mobile_number", "aadhaar_generated").filter(col("email_id").!==(0).and(col("mobile_number").!==(0)))
      .agg(sum("aadhaar_generated").as("totalCount"))
    /*4*/ var top3District = dataframe.select("district", "aadhaar_generated", "rejected").groupBy("district").agg((sum("aadhaar_generated") + sum("rejected")).as("total")).sort(col("total").desc).limit(3)

    /*5*/ var genratedStateWise = dataframe.select("aadhaar_generated", "state").groupBy("state").agg(sum("aadhaar_generated").as("sum")).sort(col("sum").desc)

    //Checkpoint 4: PySpark Skills
    /*1*/ var summary = dataframe.describe().show()
    var totalRegisterPerson = dataframe.select("age", "aadhaar_generated", "rejected").filter(col("age") !== ("0"))
      .agg((sum("aadhaar_generated") + sum("rejected")).alias("totalRegister")).collectAsList().get(0).get(0).toString()

    /*2*/ var mobileNumberRelation = dataframe.select("age", "aadhaar_generated", "rejected", "mobile_number")
      .filter(col("age") !== ("0"))
      .groupBy("age")
      .agg(sum("mobile_number").as("total"))
      .withColumn("percentage", ((col("total") / Integer.parseInt(totalRegisterPerson)) * 100))
      .withColumn("percentage", col("percentage").cast(FloatType))
      .sort(col("total").desc)
    /*3*/ var uniquePincodes = dataframe.select("pincode").distinct()
    /*4*/ var registrationsRejected = dataframe.select("rejected", "state").filter(col("state").===("Uttar Pradesh").or(col("state").===("Maharashtra")))
      .agg(sum("rejected").as("totalRejected"))
    //Checkpoint 5: Analysis
    /*1*/ var topStatesMale = dataframe.select("state", "aadhaar_generated", "gender").filter(col("gender").===("M"))
      .groupBy("state")
      .agg(sum("aadhaar_generated").as("total")).orderBy(col("total").desc).limit(3)

    /*3*/ var topFemaleCountPercentage = dataframe.select("state", "aadhaar_generated", "gender").filter(col("gender").===("F"))
      .groupBy("state")
      .agg(sum("aadhaar_generated").as("total"))
      .withColumn("percentage", ((col("total") / Integer.parseInt(totalRegisterPerson)) * 100))
      .orderBy(col("percentage").desc).limit(3)

    /*5*/ var acceptancePercentageBuckted = dataframe.select("age", "aadhaar_generated").randomSplit(Array(1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
      .foreach(
        x => x.groupBy("age")
          .agg(sum("aadhaar_generated").as("sum"))
          .withColumn("acceptancePercentage", ((col("sum") / Integer.parseInt(totalRegisterPerson)) * 100))
          .sort(col("acceptancePercentage").desc).show())

    mobileNumberRelation.show(mobileNumberRelation.count().toInt, false)
  
    
  }
}



