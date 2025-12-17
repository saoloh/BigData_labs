# Lab 1: Introduction to Spark DataFrames

**Objective:**
This lab will introduce you to the fundamental concepts of Spark DataFrames. You will learn how to create a DataFrame from a JSON file, perform basic actions to inspect the data, and manipulate its structure by selecting, filtering, adding, and removing columns.

**Prerequisites:**
- A running Spark environment.
- The sample dataset `data/flight-data/json/2015-summary.json`.

**Steps:**

**1. Initialize SparkSession**
The entry point to any Spark functionality is the `SparkSession`. Create one like this:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IntroToDataFrames").getOrCreate()
```

**2. Reading Data**
Let's read the 2015 flight summary data into a DataFrame.

```python
# Create a DataFrame from a JSON file
df = spark.read.json("data/flight-data/json/2015-summary.json")

# Alternative way
# df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
```

**3. Inspecting the Data**
There are several ways to see what's inside your DataFrame.

-   `show()`: Displays the top 20 rows in a tabular format. You can specify the number of rows to show, e.g., `df.show(5)`.

    ```python
    df.show(5)
    ```
    *Expected Output:*
    ```
    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|   15|
    |    United States|            Croatia|    1|
    |    United States|            Ireland|  344|
    |            Egypt|      United States|   15|
    |    United States|              India|   62|
    +-----------------+-------------------+-----+
    only showing top 5 rows
    ```

-   `collect()`: Retrieves all data from the DataFrame to the driver node as a list of `Row` objects. **Warning:** Be very careful with this on large datasets, as it can cause your driver to run out of memory.

    ```python
    df.limit(5).collect()
    ```

-   `take(n)`: Returns the first `n` rows as a list of `Row` objects.

    ```python
    df.take(2)
    ```

-   `printSchema()`: Prints the schema of the DataFrame, showing column names and their data types.

    ```python
    df.printSchema()
    ```
    *Expected Output:*
    ```
    root
     |-- DEST_COUNTRY_NAME: string (nullable = true)
     |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
     |-- count: long (nullable = true)
    ```

**4. Column Manipulation**

Spark provides powerful ways to select and manipulate columns.

-   `select()` and `selectExpr()`:

    ```python
    from pyspark.sql.functions import col, expr

    # Select a single column
    df.select("DEST_COUNTRY_NAME").show(3)

    # Select multiple columns
    df.select("DEST_COUNTRY_NAME", "count").show(3)

    # Use expressions with selectExpr
    df.selectExpr("DEST_COUNTRY_NAME as destination", "count").show(3)
    ```

-   Renaming columns:
    -   `withColumnRenamed()`: This is the standard way to rename a column.
    -   Using aliases: You can use `.alias()` on a column expression or `AS` within `selectExpr`.

    ```python
    # Using alias
    df.select(col("DEST_COUNTRY_NAME").alias("destination")).show(3)

    # Using withColumnRenamed
    df_renamed = df.withColumnRenamed("DEST_COUNTRY_NAME", "dest")
    df_renamed.printSchema()
    ```

**5. Filtering Rows**

You can filter rows based on conditions using the `where()` or `filter()` methods.

```python
# Show flights with a count greater than 3000
df.where("count > 3000").show()

# Chain multiple filters
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia").show()
```

**6. Adding and Dropping Columns**

-   `withColumn()`: Add a new column or replace an existing one.

    ```python
    # Add a column to check if the flight was domestic
    df_with_country_check = df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    df_with_country_check.show(5)
    ```

-   `drop()`: Remove one or more columns.

    ```python
    df_dropped = df_with_country_check.drop("withinCountry", "count")
    df_dropped.show(5)
    ```

**Exercise:**

1.  Read the `2015-summary.json` dataset.
2.  Select only the `ORIGIN_COUNTRY_NAME` and `DEST_COUNTRY_NAME` columns.
3.  Rename `ORIGIN_COUNTRY_NAME` to `origin` and `DEST_COUNTRY_NAME` to `destination`.
4.  Filter the data to show only flights where the origin is "United States" and the count is greater than 1000.
5.  Show the resulting DataFrame.

---

# Lab 2: Advanced DataFrame Operations

**Objective:**
In this lab, you'll dive deeper into DataFrame manipulations. You will work with different data types, perform numerical calculations, and apply a variety of string functions to clean and transform data.

**Prerequisites:**
- A running Spark environment.
- The sample dataset `data/retail-data/by-day/2010-12-01.csv`.

**Steps:**

**1. Reading the Data**
This time, we'll work with a CSV file containing retail data. We'll let Spark infer the schema and use the first line as the header.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("AdvancedOps").getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/retail-data/by-day/2010-12-01.csv")

df.printSchema()
df.show(5)
```

**2. Working with Booleans and Literals**

-   **The `lit()` function**: `lit()` is used to add a new column with a constant, literal value.

    ```python
    df.select(lit(5), lit("five"), lit(5.0)).show(2)
    ```

-   **Boolean Filtering**: You can build complex conditions for filtering. Spark optimizes chained `where` clauses, treating them as a single `AND` expression. `OR` conditions must be specified within the same `where` clause using the `|` operator.

    ```python
    # An example of a complex filter
    priceFilter = col("UnitPrice") > 5
    descripFilter = instr(df.Description, "KNITTED") >= 1
    
    df.where(df.StockCode.isin("84029G", "84029E")).where(priceFilter | descripFilter).show()
    ```

-   **Adding Boolean Flags**: You can add a boolean column to your DataFrame to flag rows that meet a certain condition.

    ```python
    isExpensiveFilter = (col("UnitPrice") > 5) | (instr(col("Description"), "HAND WARMER") >= 1)

    df.withColumn("isExpensive", isExpensiveFilter).where("isExpensive").select("Description", "UnitPrice", "isExpensive").show(5)
    ```

**3. Numeric Operations**

-   **Calculations**: Perform arithmetic operations on numeric columns. Here, we calculate a hypothetical "total price".

    ```python
    # The pow() function raises a column to the specified power
    fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(col("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

    # The same can be done with selectExpr for SQL-like syntax
    df.selectExpr("CustomerId", "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
    ```

-   **Rounding**: Use `round()` for standard rounding.

    ```python
    df.select(round(lit("2.5")), round(col("UnitPrice"), 1)).show(5)
    ```

-   **Summary Statistics**: The `describe()` action provides count, mean, standard deviation, min, and max for all numeric columns.

    ```python
    df.describe().show()
    ```
-   **Correlation**: Calculate the Pearson correlation coefficient between two columns.
    ```python
    # As an action
    print(df.stat.corr("Quantity", "UnitPrice"))
    
    # As a transformation
    df.select(corr("Quantity", "UnitPrice")).show()
    ```

**4. String Manipulation**

-   **Case and Spacing**: Use `initcap`, `lower`, `upper`, and `trim` to format strings.

    ```python
    df.select(initcap(col("Description"))).show(3, False)
    
    from pyspark.sql.functions import ltrim, rtrim, trim
df.select(
    ltrim(lit(" HELLO ")).alias("ltrim"),
    rtrim(lit(" HELLO ")).alias("rtrim"),
    trim(lit(" HELLO ")).alias("trim")).show()
    ```

-   **Regular Expressions**:
    -   `regexp_replace`: Replace parts of a string that match a regular expression.
    -   `regexp_extract`: Extract a value from a string based on a regular expression group.

    ```python
    # Replace color names with "COLOR"
    regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
    df.select(
        regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
        col("Description")).show(3, False)

    # Extract the first color name found
    extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
    df.select(
        regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
        col("Description")).show(3, False)
    ```

-   **Substring Search**: The `instr` function returns the 1-based index of the first occurrence of a substring. It's useful for filtering.

    ```python
    containsKnitted = instr(col("Description"), "KNITTED") >= 1
df.withColumn("hasKnitted", containsKnitted).where("hasKnitted").select("Description").show(3, False)
    ```

**Exercise:**

1.  From the retail dataset, find all items where the `Description` contains the word "HEART".
2.  Create a new column called `TotalPrice` which is the `Quantity` multiplied by the `UnitPrice`.
3.  Add a new column `HighValue` that is `true` if `TotalPrice` is greater than 15 and `false` otherwise.
4.  Show the `Description`, `TotalPrice`, and `HighValue` columns for the first 10 "HEART" items.
