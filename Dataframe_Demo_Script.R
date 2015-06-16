library(SparkR)
sc <- sparkR.init("local[*]")
sqlCtx <- sparkRSQL.init(sc)

# Creating Data Frames ----
# From a local data.frame
irisDF <- createDataFrame(sqlCtx, iris)
printSchema(irisDF)

# Or from raw data using a schema
listOfLists <- list(list(1, 2, 3),
                    list("a","b","c"),
                    list(1.2, 3.4, 5.6))
schema <- structType(structField("col1", "integer"),
                     structField("col2", "string"),
                     structField("col3", "numeric"))
head(newDF <- createDataFrame(sqlCtx, listOfLists, schema))

# Or from an existing data source
demo <- read.df(sqlCtx, "/Users/cfreeman/SparkR/SparkR_DataFrame_Demo/Customer_Demographics.json", "json")
printSchema(demo)
head(demo)

# Basic Operations ----
# Reference a specific column with `$`
head(select(irisDF,irisDF$Species))

# Or multiple columns with `[, c("foo", "bar")]`
head(irisDF[,c("Petal_Length", "Sepal_Length")])

# Create new columns with `$` or `mutate`
# Create Petal_Area
irisDF$Petal_Area <- irisDF$Petal_Length * irisDF$Petal_Width
# Create Sepal_Area
head(irisDF <- mutate(irisDF, Sepal_Area = irisDF$Sepal_Length * irisDF$Sepal_Width))

# And drop them just as easily!
irisDF$Petal_Area <- NULL
head(irisDF)

# Filter on a pattern with `like`
head(filter(demo, like(demo$age, "40to44")))

# Count records of a certain type with `where`
head(count(where(demo, demo$region == "Pacific")))

# Aggregations ----
#First, let's load up some transaction data
txnsRaw <- read.df(sqlCtx, "/Users/cfreeman/SparkR/SparkR_DataFrame_Demo/Customer_Transactions.json", "json")
printSchema(txnsRaw)
head(txnsRaw)

# Aggregate data using `groupBy` and `summarize`
head(perCustomer <- summarize(groupBy(txnsRaw, txnsRaw$cust_id),
                              txns = countDistinct(txnsRaw$day_num),
                              spend = sum(txnsRaw$extended_price)))

# Joins ----
# Basic join
head(limit(select(
            join(perCustomer, demo, perCustomer$cust_id == demo$cust_id),
            demo$"*",
            perCustomer$txns,
            perCustomer$spend),
            10))

# But let's say we want to combine a few steps into one process
head(limit(mutate(
            select(
              join(perCustomer, demo, perCustomer$cust_id == demo$cust_id),
              demo$"*",
              perCustomer$txns,
              perCustomer$spend),
            txnsAboveAverage = perCustomer$txns > as.numeric(first(summarize(perCustomer, mean(perCustomer$txns))))),               
          10))

# All of these nested functions are kind of a drag... ----
library(magrittr)

# Let's try that again
customerData <- perCustomer %>%
                  join(demo, .$cust_id == demo$cust_id) %>%
                  select(demo$"*", perCustomer$txns, perCustomer$spend) %>%
                  {
                    txnsAvg <- summarize(., mean(.$txns)) %>% first %>% as.numeric
                    mutate(., txnsAboveAverage = .$txns > txnsAvg)
                  }
head(customerData)
  

# Bonus: DataFrame Pipelines! ----
sample <- read.df(sqlCtx, "/Users/cfreeman/SparkR/SparkR_DataFrame_Demo/DM_Sample.json/", "json")
printSchema(sample)
count(sample)

# Use a single pipeline to perform a series of transformations and create multiple new DataFrames
customerData %>%
  join(sample, .$cust_id == sample$cust_id, "left_outer") %T>% {
    # Create training data
    filter(., isNull(sample$cust_id)) %>%
      select(customerData$"*") ->> trainDF
    # Create test data
    filter(., isNotNull(sample$cust_id)) %>%
      select(customerData$"*", sample$respondYes) ->> testDF
  }

printSchema(trainDF)
printSchema(testDF)


