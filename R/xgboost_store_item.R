# PARAMS ------------------------------------------------------------------
FCT.COL   <- "xgboost_v1"
DATA.VARS <- "
  sales.store_id as store_id,
  sales.item_id as item_id,
  sales.date as date, 
  sales.value as sold, 
  prices.sell_price as price,
  calendar.wday as wday,
  calendar.month as month,
  calendar.year as year,
  calendar.event_name_1 as event_name_1,
  calendar.event_type_1 as event_type_1,
  calendar.event_name_2 as event_name_2,
  calendar.event_type_2 as event_type_2,
  calendar.snap_CA, calendar.snap_TX, calendar.snap_WI"

NEW.DATA.VARS <- as.character(glue("
  {FORECAST.ITEM.STORE.TABLE}.store_id as store_id,
  {FORECAST.ITEM.STORE.TABLE}.item_id as item_id,
  {FORECAST.ITEM.STORE.TABLE}.date as date, 
  prices.sell_price as price,
  calendar.wday as wday,
  calendar.month as month,
  calendar.year as year,
  calendar.event_name_1 as event_name_1,
  calendar.event_type_1 as event_type_1,
  calendar.event_name_2 as event_name_2,
  calendar.event_type_2 as event_type_2,
  calendar.snap_CA, calendar.snap_TX, calendar.snap_WI
"))

LAGS    <- c(1, 2, 3, 7, 14)
FACTORS <- c('event_name_1',
             'event_type_1',
             'event_name_2',
             'event_type_2')

VARIABLES <- sold ~ 

#'  [1] "sales.item_id"         "sales.dept_id"         "sales.cat_id"          "sales.store_id"        "sales.state_id"        "sales.value"          
#'  [7] "sales.date"            "sales.wm_yr_wk"        "sales.row_names"       "calendar.date"         "calendar.wm_yr_wk"     "calendar.weekday"     
#' [13] "calendar.wday"         "calendar.month"        "calendar.year"         "calendar.d"            "calendar.event_name_1" "calendar.event_type_1"
#' [19] "calendar.event_name_2" "calendar.event_type_2" "calendar.snap_CA"      "calendar.snap_TX"      "calendar.snap_WI"      "prices.store_id"      
#' [25] "prices.item_id"        "prices.wm_yr_wk"       "prices.sell_price"   

# FETCH RAW DATA ----------------------------------------------------------

# Get list of items without forecast
add_forecast_column <- function(con, col){
  if (!(col %in% dbListFields(con, FORECAST.ITEM.STORE.TABLE))){
    sql <- glue("ALTER TABLE {FORECAST.ITEM.STORE.TABLE} 
                 ADD COLUMN {col} Nullable(Float32)") %>% 
      as.character()
    query <- dbSendQuery(con, sql)
  }
}

get_item_list <- function(con, col){
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

get_current_batch <- function(store.cats){
  return(store.cats[1, ])
}

# Sample data
# query <- dbSendQuery(con,
#                      "SELECT * FROM sales
#                      LEFT JOIN calendar ON sales.date = calendar.date
#                      LEFT JOIN prices ON sales.wm_yr_wk = prices.wm_yr_wk AND
#                                sales.item_id = prices.item_id AND sales.store_id = prices.store_id
#                      WHERE store_id = 'TX_2'")
# sample.data <- dbFetch(query)

fetch_data <- function(con, current.batch, colnames){
  # First store on list of stores to be computed
  first.store <- current.batch$store_id[1]
  first.cat <- current.batch$dept_id[1]
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

fetch_new_data <- function(con, current.batch, colnames){
  # First store on list of stores to be computed
  first.store <- current.batch$store_id[1]
  first.cat <- current.batch$dept_id[1]
  sql <- glue(
    "SELECT {colnames} FROM {FORECAST.ITEM.STORE.TABLE} 
     LEFT JOIN calendar ON {FORECAST.ITEM.STORE.TABLE}.date = calendar.date
     LEFT JOIN prices ON  calendar.wm_yr_wk = prices.wm_yr_wk AND 
               {FORECAST.ITEM.STORE.TABLE}.item_id = prices.item_id AND 
               {FORECAST.ITEM.STORE.TABLE}.store_id = prices.store_id
     WHERE store_id = '{first.store}' AND dept_id = '{first.cat}'"
  )
  query <- dbSendQuery(con, sql)
  return(dbFetch(query))
}

# DATA PREPROCESSING ------------------------------------------------------
trim_trailing_zero_sales <- function(raw.data){
  setDT(raw.data)
  setorder(raw.data, date)
  trimmed.data <- raw.data[, trimmed := (cumsum(sold) != 0), 
                           by = .(store_id, item_id)]
  trimmed.data <- trimmed.data[trimmed == TRUE]
  trimmed.data <- trimmed.data[, !c("trimmed")]
  return(trimmed.data)
}

add_sales_lags <- function(trimmed.data, lags = LAGS){
  setDT(trimmed.data)
  trimmed.data[, paste0('sold_lag', lags) := shift(.SD, lags), 
     by = .(item_id, store_id), .SDcols = "sold"]
  return(trimmed.data)
}

# prepare_train_data <- function(lagged.data){
#   lagged.data %>% 
#     mutate_at(starts_with())
# }

add_sales_lags_new_data <- function(raw.new.data, train.data, lags){
  max.lag    <- max(lags)
  last.dates <- tail(sort(unique(train.data$date)), max.lag)
  train.data.ending <- train.data[date >= min(last.dates)]
  train.data.ending[, is.train := TRUE]
  setDT(raw.new.data)
  raw.new.data[, is.train := FALSE]
  binded <- rbindlist(list(
    raw.new.data, train.data.ending
  ), use.names = TRUE, fill = TRUE)
  setorder(binded, date)
  binded[, paste0('sold_lag', lags) := shift(.SD, lags), 
               by = .(item_id, store_id), .SDcols = "sold"]
  return(binded[is.train != FALSE])
} 

match_calendar_snap <- function(data){
  setDT(data)
  data[, snap := case_when(
    grepl("CA", store_id) ~ calendar.snap_CA,
    grepl("TX", store_id) ~ calendar.snap_TX,
    grepl("WI", store_id) ~ calendar.snap_WI
  )]
  data <- data[, !c("calendar.snap_CA", "calendar.snap_TX", "calendar.snap_WI")]
  return(data)
}

factorize_columns <- function(data, factors){
  setDT(data)
  data <- data %>% 
    mutate_at(factors, as.factor)
  return(data)
}

# fit and predict ---------------------------------------------------------
fit_predict_xgboost <- function(train.data, new.data){
  # Declare model
  model <- boost_tree() %>%
    set_mode("regression") %>%
    set_engine("xgboost")
  #  fit(sales.value ~ prices.sell_price, data = sample.data)
  
  # Run model for all the products in the batch
  models <- trimmed.data %>% 
    .[, .(model = list(fit(model, sold ~ price, data = .SD))), 
      by = .(store_id, item_id)]
  
  # Generate predictions

}

# How to schedule looped plan?
# Incremnting a variable

xgboost.plan <- drake_plan(
  # Prepare column for writing output
  fct.column        = add_forecast_column(con, FCT.COL),
  # Prepare store/item list
  item.list         = get_item_list(con, FCT.COL),
  store.items       = get_store_items(item.list),
  batches           = get_batches(store.items),
  current.batch     = get_current_batch(batches),
  # Preparing data for modelling
  raw.data          = fetch_data(con, current.batch, DATA.VARS),
  trimmed.data      = trim_trailing_zero_sales(raw.data),
  lagged.data       = add_sales_lags(trimmed.data, c(1, 2, 3, 7, 14)),
  reduced.data      = match_calendar_snap(lagged.data),
  data.with.factors = factorize_columns(reduced.data, FACTORS),
  # Preparing new data
  raw.new.data      = fetch_new_data(con, current.batch, NEW.DATA.VARS),
  prepared.new.data = add_sales_lags_new_data(raw.new.data, lagged.data, LAGS),
  reduced.new.data  = match_calendar_snap(prepared.new.data),
  new.data.with.factors = factorize_columns(reduced.new.data, FACTORS)
  # Training & forecast
  #fit.xgboost  = fit_predict_xgboost(train.data, new.data)
  # Saving forecasts
  # results.saved = save_results()
)
  
make(xgboost.plan)
loadd("prepared.new.data")
