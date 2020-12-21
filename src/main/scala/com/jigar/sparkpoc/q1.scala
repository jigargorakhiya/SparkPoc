package com.jigar.sparkpoc

import org.apache.spark.sql.{DataFrame,SaveMode,SparkSession}
import org.apache.log4j._

object q1
{

  def main (args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val inputPath = "/Users/jigar/Documents/Jigar/Spark-Interviews/Practicals/Sapient-BigData-Interview/test_pack/data_for_probs/ecom"

    val spark = SparkSession
      .builder()
      .appName("Q1- Spark/MapReduce")
      .master("local")
      .getOrCreate()

    val competitor_df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema",true)
      .load(inputPath+"/ecom_competitor_data.txt")

    competitor_df.show

    val internal_product_df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema",true)
      .load(inputPath+"/internal_product_data.txt")

    val seller_df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema",true)
      .load(inputPath+"/seller_data.txt")

    competitor_df.createOrReplaceTempView("competitor")
    internal_product_df.createOrReplaceTempView("int_prod")
    seller_df.createOrReplaceTempView("seller")


    val all_data_set=spark.sql("""
                                 							| select comp.productId,comp.price,comp.saleEvent,comp.rivalName,comp.fetchTS,
                                 							| prod.procuredValue,prod.minMargin,prod.maxMargin,
                                 							| sal.SellerID,sal.netValue
                                 							| from competitor comp
                                 							| left outer join int_prod prod
                                 							| On comp.productId=prod.productId
                                 							| left outer join seller sal
                                 							| On sal.SellerID=prod.SellerID
                               							""".stripMargin)
    all_data_set.createOrReplaceTempView("all_data")

    val final_data = spark.sql(
      """
        |select distinct a.productId,a.final_Price,a.Cheapest_Price_amongst_all_Rivals,c.rivalName as Rival_name from (
        |select distinct all.productId,
        |case when procuredValue+maxMargin < min(price) over (partition by ProductId) then procuredValue+maxMargin
        |     when procuredValue+minMargin < min(price) over (partition by ProductId) then min(price) over (partition by ProductId)
        |     when procuredValue < min(price) over (partition by ProductId) and saleEvent='Special' then min(price) over (partition by ProductId)
        |     when min(price) over (partition by ProductId) < procuredValue and saleEvent='Special' and netValue='VeryHigh' then ( procuredValue * 0.9 )
        |else procuredValue
        |end as  final_Price,
        |min(price) over (partition by ProductId) as Cheapest_Price_amongst_all_Rivals
        |from  all_data all
        |) a
        |left outer join competitor c
        |on a.productId=c.productId and a.Cheapest_Price_amongst_all_Rivals=c.price
      """.stripMargin)

    final_data.repartition(1).write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(inputPath+"/result/q1.csv")

  }
}
