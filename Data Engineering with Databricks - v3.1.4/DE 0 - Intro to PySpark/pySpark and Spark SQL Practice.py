# Databricks notebook source
from pyspark.sql.functions import approx_count_distinct

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Dataset

# COMMAND ----------

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Number of Distinct rows

# COMMAND ----------

#PySpark
#Count number of employees in each department w 
df_distinct_by_department = df.groupBy("department").count()
display(df_distinct_by_department)

# COMMAND ----------

#Spark SQL
#spark sql queries myst be made from a table. Thus, we need to turn the df into a table

df.createOrReplaceTempView("table")

distinct_employees_department_sql = spark.sql("""SELECT department,  COUNT("employee_name")
                                            AS `Number Employees`
                                            FROM table
                                            GROUP BY department""")

display(distinct_employees_department_sql)


# COMMAND ----------

from pyspark.sql.functions import col, sum

department_salary = (df
.groupBy("department")
.agg(sum(col("salary").cast("int")))
.show()
)

# COMMAND ----------

#Average salary by department
department_salary_sql = spark.sql("""SELECT department, avg(salary) AS avg_salary
                                  FROM df
                                  GROUP BY department""")

display(department_salary_sql)

# COMMAND ----------

avg_df = spark.sql("""SELECT *, avg("salary") AS avg salary
          FROM df
          GROUP BY department""")



# COMMAND ----------


