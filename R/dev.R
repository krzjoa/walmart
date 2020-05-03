# Data 
list.files("../data/")

# Read from csv
calendar <- read.csv("../data/calendar.csv")
prices   <- read.csv("../data/sell_prices.csv")
sales    <- read.csv("../data/sales_train_validation.csv")

# Read from fst
calendar   <- fst::read_fst("../data/calendar.fst")
prices     <- fst::read_fst("../data/prices.fst")
sales      <- fst::read_fst("../data/sales_train_validation.fst")
sales.long <- fst::read_fst("../data/sales_long.fst")

# As disk.frame
library(disk.frame)

sales.long <- as.disk.frame(sales.long, outdir = "../data/sales_long_disk", overwrite = TRUE)
prices     <- as.disk.frame(prices, outdir = "../data/prices_disk")
calendar   <- as.disk.frame(calendar, outdir = "../data/calendar_disk")

# From disk
sales.long <- disk.frame(path = "../data/sales_long_disk/")
calendar   <- disk.frame(path = "../data/calendar_disk/")
prices     <- disk.frame(path = "../data/prices_disk//")

sales.long <- sales.long %>% 
  left_join(calendar, by = "d")

sales.long <- sales.long %>% 
  left_join(prices)

write_disk.frame(sales.long, "../data/sales_long.df")

# Preparing data
View(prices)

# Stores
unique(prices$store_id)

# Items
unique(prices$item_id)

View(head(sales))

# Transforming sales data 
library(dplyr)
library(data.table)

sales.long <- sales %>% 
  tidyr::pivot_longer(cols = starts_with("d_")) %>% 
  setDT()

setnames(sales.long, "name", "d")
# fst::write_fst(sales.long, "../data/sales_long.fst")
sales.long <- merge(sales.long, calendar, by = "d")

# Database

# https://clickhouse.tech/#quick-start
# sudo service clickhouse-server start
# https://polatramazan.wordpress.com/2019/06/11/clickhouse-password-issue/
# https://github.com/mkearney/kaggler

devtools::install_github("mkearney/kaggler")

install.packages("RClickhouse")

# Test
existing.items.query <- dbSendQuery(con, 
                                    "SELECT DISTINCT item_id, store_id FROM sales"
)

dbListTables(con)
dbRemoveTable(con, 'sales')
dbFetch(existing.items.query)
dbListTables(con)


library(tsibble)
library(fable)
library(ggplot2)



# Sudo password to run a service
# https://stackoverflow.com/questions/23186960/run-a-system-command-as-sudo-from-r
# system("sudo service clickhouse-server start", input = rstudioapi::askForPassword("sudo password"))
# https://blog.cerebralab.com/Clickhouse,_an_analytics_database_for_the_21st_century


# https://programmer.help/blogs/how-to-choose-clickhouse-table-engine.html
dbSendQuery(con, "CREATE TABLE test_tbl (
  id UInt16,
  create_time Date,
  comment Nullable(String)
) ENGINE = MergeTree()
   PARTITION BY create_time
     ORDER BY  (id, create_time)
     PRIMARY KEY (id, create_time)
     TTL create_time + INTERVAL 1 MONTH
     SETTINGS index_granularity=8192") -> query

sql <- glue("ALTER TABLE test_tbl ADD COLUMN {col} Float32") %>% 
  as.character()
query <- dbSendQuery(con, sql)
    




ggplot(raw.data) +
  geom_histogram(aes(x = sold), bins = 100) -> p

plotly::ggplotly(p)
