from pyspark.sql import SparkSession
import json
import time

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("toys_sales")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_sales = "dataset.csv"
    df_sales = spark.read.csv(path_sales, header=True, inferSchema=True)
    df_sales.createOrReplaceTempView("sales")

    # Query to get the day with the most sales
    query_most_sales = """
    SELECT CAST(Date AS STRING) AS DateWithMostSales, SUM(Units) as total_sales
    FROM sales
    GROUP BY Date
    ORDER BY total_sales DESC
    LIMIT 1
    """
    df_most_sales = spark.sql(query_most_sales)
    most_sales = df_most_sales.collect()[0].asDict()

    # Query to get the top 10 toys that were sold the most
    query_top_sellers = """
    SELECT Product_ID AS ToyID, SUM(Units) as total_quantity
    FROM sales
    GROUP BY Product_ID
    ORDER BY total_quantity DESC
    LIMIT 10
    """
    df_top_sellers = spark.sql(query_top_sellers)
    top_sellers = [row.asDict() for row in df_top_sellers.collect()]

    # Combine results into a single JSON object


    results = {
        "MostSales": most_sales,
        "TopSellers": top_sellers,
        "Updated": str(time.time())
    }


    # Write results to a JSON file
    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    df_top_sellers.show()
    df_most_sales.show()

    spark.stop()