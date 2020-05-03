DB.INSERT.CHUNKS <- 2000

# Sudo password to run a service
# https://stackoverflow.com/questions/23186960/run-a-system-command-as-sudo-from-r
# system("sudo service clickhouse-server start", input = rstudioapi::askForPassword("sudo password"))
# https://blog.cerebralab.com/Clickhouse,_an_analytics_database_for_the_21st_century

# dbSendQuery(con, "SET max_query_size = 250000000")
# dbSendQuery(con, "SET max_ast_elements = 50000000")

# data preprocessing ------------------------------------------------------
prepare_calendar <- function(calendar){
  calendar <- mutate_if(calendar, is.factor, as.character)
  setDT(calendar)
  calendar[, date := lubridate::as_date(date)]
  return(calendar)
}

prepare_prices <- function(prices){
  setDT(prices)
  prices <- mutate_if(prices, is.factor, as.character)
  return(prices)
}

prepare_sales_longer <- function(sales, calendar){
  setDT(sales)
  setDT(calendar)
  dates <- calendar[, .(d, date, wm_yr_wk)]
  dates[, `:=`(d = as.character(d),
               date = lubridate::as_date(date))]
  
  sales <- sales %>% 
    tidyr::pivot_longer(cols = starts_with("d_")) %>% 
    setDT() %>% 
    setnames("name", "d") %>% 
    .[, !c("id")] %>% 
    .[, d := as.character(d)]
  
  sales <- left_join(sales, dates)
  sales <- mutate_if(sales, is.factor, as.character)
  sales <- select(sales, -d)
  return(sales)
}

# Db save if not exists
# Replace with dplyr::copy_to (or dbplyr)
db_save <- function(con, df, name){
  if (dbExistsTable(con, name))
    return(TRUE)
  else
    dbWriteTable(con, name, df)
}

db_batch_save <- function(con, df, name, chunks){
  names <- colnames(df)
  print(names)
  # dbCreateTable(con, name, names)
  # Split data.frame to into smaller chunks

  for (idx in parallel::splitIndices(nrow(df), chunks)) {
    print(length(idx))
    dbWriteTable(con, name, df[idx,], 
                append = TRUE, row.names = TRUE)
  }
  return(TRUE)
}


create_store_item_forecast_table <- function(con, sales, calendar, name){
  
  max.date.sales <- max(sales$date)
  date <- calendar %>% 
    filter(date > max.date.sales) %>% 
    .$date
  
  setDT(sales)
  all.products <- sales %>% 
    .[, .(n = .N), by = .(store_id, item_id)] %>% 
    .[, !c("n")]
  
  forecast.item.days <-  tidyr::crossing(all.products, date)
  
  # Create table
  if (!dbExistsTable(con, "forecast_item_store"))
    dbSendQuery(con, 
      "CREATE TABLE forecast_item_store (
         store_id String,
         item_id String,
         date Date
      ) ENGINE = MergeTree
        PRIMARY KEY (store_id, item_id, date)
        ORDER BY (store_id, item_id, date)"
    )
  
  # Split on batches
  for (idx in parallel::splitIndices(nrow(forecast.item.days), 10)) {
    print(length(idx))
    dbWriteTable(con, "forecast_item_store", forecast.item.days[idx,], 
                 append = TRUE)
  }
  return(TRUE)
}

# Prepare data
data.plan <- drake_plan(
  # Files
  calendar = target(read.csv("../data/calendar.csv"), format = 'fst'),
  prices   = target(read.csv("../data/sell_prices.csv"), format = 'fst'),
  sales    = target(read.csv("../data/sales_train_validation.csv"), format = 'fst'),
  # Transfroming data
  calendar.prepared = prepare_calendar(calendar),
  prices.prepared = prepare_prices(prices),
  sales.long = target(prepare_sales_longer(sales, calendar.prepared), format = "fst"),
  # Feature store
  db.calendar = db_save(con, calendar.prepared, 'calendar'),
  db.prices   = db_save(con, prices.prepared, "prices"),
  # db.sales.long = db_batch_save(con, sales.long, "sales", DB.INSERT.CHUNKS),
  # Create table for forecasts
  db.forecast.items.store = create_store_item_forecast_table(con, sales.long, 
                                                             calendar.prepared, FORECAST.ITEM.STORE.TABLE)
)

make(data.plan, garbage_collection = TRUE,
     memory_strategy = 'preclean')
  