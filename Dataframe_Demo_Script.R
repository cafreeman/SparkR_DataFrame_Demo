library(SparkR)
sc <- sparkR.init("local[*]")
sqlCtx <- sparkRSQL.init(sc)

# Creating Data Frames ----
head(iris)
irisDF <- createDataFrame(sqlCtx, iris)
printSchema(irisDF)

irisDF$Petal_Area <- irisDF$Petal_Length * irisDF$Petal_Width
head(irisDF)

irisDF$Petal_Area <- NULL
head(irisDF)

# Working with external data types ----
demo <- read.df(sqlCtx, "/Users/cfreeman/SparkR/SparkSummit2015_Demo/Customer_Demographics.json", "json")
printSchema(demo)
head(demo)

# Basic Operations ----
head(select(demo, demo$age))

head(filter(demo, like(demo$age, "40to44")))

head(count(where(demo, demo$region == "Pacific")))

# Aggregations ----
txnsRaw <- read.df(sqlCtx, "/Users/cfreeman/SparkR/SparkSummit2015_Demo/Customer_Transactions.parquet")

printSchema(txnsRaw)
head(txnsRaw)

# Grouping
groupByCustomer <- groupBy(txnsRaw, txnsRaw$cust_id)
class(groupByCustomer)

# Summarize
head(perCustomer <- summarize(groupByCustomer,
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

# But we'd also like to calculate a new field based on one of our current fields
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
  
# DataFrame Pipelines ----
sample <- loadDF(sqlCtx, "/Users/cfreeman/SparkR/SparkR_DataFrame_Demo/DM_Sample.parquet/")
printSchema(sample)
count(sample)

# Use a single pipeline to perform a series of transformations and create multiple new DataFrames
customerData %>%
  join(sample, .$cust_id == sample$cust_id, "left_outer") %>% {
    # Create training data
    filter(., isNull(sample$cust_id)) %>%
      select(customerData$"*") ->> trainDF
    # Create test data
    filter(., isNotNull(sample$cust_id)) %>%
      select(customerData$"*", sample$respondYes) ->> testDF
  }

head(trainDF)


