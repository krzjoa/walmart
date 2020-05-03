# PARAMS ------------------------------------------------------------------
FCT.COL   <- "xgboost_v1"
VARIABLES <- "sales.store_id as store_id,
              sales.item_id as item_id,
              sales.date as date, 
              sales.value as sold, 
              prices.sell_price as price"

# Get list of items without forecast
add_forecast_column <- function(con, col){
  if (!(col %in% dbListFields(con, FORECAST.ITEM.STORE.TABLE))){
    sql <- glue("ALTER TABLE {FORECAST.ITEM.STORE.TABLE} 
                 ADD COLUMN {col} Nullable(Float32)") %>% 
      as.character()
    query <- dbSendQuery(con, sql)
  }
}

item_list <- function(con, col){
  sql <- glue(
    "SELECT store_id, item_id, cat_id, dept_id, date
     FROM {FORECAST.ITEM.STORE.TABLE}
     WHERE {col} IS NULL"
  )
  query <- dbSendQuery(con, sql)
  dbFetch(query)
}

get_store_items <- function(item.list){
  setDT(item.list)
  return(unique(item.list, by = c("store_id", "cat_id", "item_id")))
}

get_batches <- function(store.items){
  setDT(store.items)
  batches <- store.items[, .(n = .N), by = .(store_id, dept_id)]
  setorder(batches, store_id, dept_id)
  return(batches)
}

# Sample data
# query <- dbSendQuery(con,
#                      "SELECT * FROM sales
#                      LEFT JOIN calendar ON sales.date = calendar.date
#                      LEFT JOIN prices ON sales.wm_yr_wk = prices.wm_yr_wk AND
#                                sales.item_id = prices.item_id AND sales.store_id = prices.store_id
#                      WHERE store_id = 'TX_2'")

fetch_data <- function(con, store.cats, colnames){
  # First store on list of stores to be computed
  first.store <- store.cats$store_id[1]
  first.cat <- store.cats$dept_id[1]
  sql <- glue(
    "SELECT {colnames} FROM sales 
     LEFT JOIN calendar ON sales.date = calendar.date
     LEFT JOIN prices ON sales.wm_yr_wk = prices.wm_yr_wk AND 
               sales.item_id = prices.item_id AND sales.store_id = prices.store_id
     WHERE store_id = '{first.store}' AND dept_id = '{first.cat}'"
  )
  query <- dbSendQuery(con, sql)
  return(dbFetch(query))
}

trim_trailing_zero_sales <- function(raw.data){
  setDT(raw.data)
  setorder(raw.data, date)
  trimmed.data <- raw.data[, trimmed := (cumsum(sold) != 0), 
                           by = .(store_id, item_id)]
  trimmed.data <- trimmed.data[trimmed == TRUE]
  return(trimmed.data)
}

add_sales_value_lags <- function(trimmed_data, lags){
  
}

# fit and predict ---------------------------------------------------------

fit_predict_xgboost <- function(train.data, new.data){
  
  # Declare model
  model <- boost_tree() %>%
    set_mode("regression") %>%
    set_engine("xgboost")
  #  fit(sales.value ~ prices.sell_price, data = sample.data)
  
  # Run model for all the products in the batch
  
  
  # Generate predictions
  
  
  # Insert preictions to database
  
  
}



# How to schedule looped plan?
# Incremnting a variable

xgboost.plan <- drake_plan(
  # Prepare column for writing output
  fct.column    = add_forecast_column(con, FCT.COL),
  # Prepare store/item list
  item.list     = item_list(con, FCT.COL),
  store.items   = get_store_items(item.list),
  batches       = get_batches(store.items),
  # Preparing data for modelling
  raw.data      = fetch_data(con, batches, VARIABLES),
  trimmed_data  = trim_trailing_zero_sales(raw.data),
  lagged.data   = NULL,
  # Training & forecast
  #fit.xgboost  = fit_predict_xgboost()
)
  
make(xgboost.plan)
loadd("raw.data")
