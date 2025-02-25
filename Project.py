'''
import pymongo
import json
conn_str=f"mongodb+srv://ramansidhuu7:Canada123@cluster0.ljvukjx.mongodb.net/?retryWrites=true&w=majority"
client=pymongo.MongoClient(conn_str)

myDB=client["Project_2023"]
Healthcare=myDB["Healthcare"]

# Read the JSON file
with open(r"C:/Users/91628/Desktop/NoSQL PROJECT/cleaned_dataset.json") as file:
    data = json.load(file)

# Insert the data into the MongoDB collection
Healthcare.insert_many(data)

cursor = Healthcare.find()

# Print each document
for document in cursor:
    print(document)



cursor = Healthcare.find({"Age": {"$gte": 40, "$lte": 80}})

# Print a message before the loop
print("Patients whose age is between 40 and 80:")

for document in cursor:
    print(document)

'''
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.getOrCreate()

# Assuming 'healthcare_data.csv' is your CSV file
csv_file_path = "C:/Users/91628/Desktop/NoSQL PROJECT'/cleaned_dataset.csv"

# Read CSV data into a PySpark DataFrame
csv_data_frame = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Register the DataFrame as a temporary SQL table
csv_data_frame.createOrReplaceTempView("healthcare_table")

query="""
SELECT
    `Medical Condition`,
    COUNT(*) AS total_patients,
    AVG(`Billing Amount`) AS avg_billing_amount,
    MAX(`Billing Amount`) AS max_billing_amount,
    MIN(`Billing Amount`) AS min_billing_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY `Billing Amount`) AS median_billing_amount
FROM
    healthcare_table
WHERE
    `Medical Condition` IS NOT NULL -- Filter out rows without medical condition information
GROUP BY
    `Medical Condition`
ORDER BY
    avg_billing_amount DESC; -- Order by average billing amount in descending order

"""
Result = spark.sql(query)
Result.show()

