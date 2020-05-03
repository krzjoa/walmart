# Get list of items without forecast
add_forecast_column <- function(con, col){
  if (!(col %in% dbListFields(con, FORECAST.ITEM.STORE.TABLE))){
    sql <- glue("ALTER TABLE {FORECAST.ITEM.STORE.TABLE} ADD COLUMN {col} Nullable(Float32)") %>% 
      as.character()
    query <- dbSendQuery(con, sql)
    return(TRUE)
  }
  return(TRUE)
}

item_list <- function(con, col){
  sql <- glue(
    "SELECT store_id, item_id, date
     FROM {FORECAST.ITEM.STORE.TABLE}
     WHERE {col} IS NULL"
  )
  query <- dbSendQuery(con, sql)
  dbFetch(query)
}

get_store_items <- function(item.list){
  setDT(item.list)
  return(unique(item.list, by = c("store_id", "item_id")))
}

get_stores <- function(store.items){
  setDT(store.items)
  return(store.items[, .(n = .N), by = store_id])
}

# Sample data
query <- dbSendQuery(con,
                     "SELECT * FROM sales 
                     LEFT JOIN calendar ON sales.date = calendar.date
                     LEFT JOIN prices ON sales.wm_yr_wk = prices.wm_yr_wk AND 
                               sales.item_id = prices.item_id AND sales.store_id = prices.store_id
                     WHERE store_id = 'TX_2'")

fetch_data <- function(con, store.list){
  # First store on list of stores to be computed
  first.store <- store.list$store_id[1]
  sql <- glue(
    "SELECT * FROM sales 
                     LEFT JOIN calendar ON sales.date = calendar.date
                     LEFT JOIN prices ON sales.wm_yr_wk = prices.wm_yr_wk AND 
                               sales.item_id = prices.item_id AND sales.store_id = prices.store_id
                     WHERE store_id = '{first.store}'"
  )
  query <- dbSendQuery(sql)
  return(dbFetch(query))
}


boost_tree() %>% 
  set_mode("regression") %>% 
  set_engine("xgboost") %>% 
  fit(sales.value ~ prices.sell_price, data = sample.data)


FCT.COL <- "xgboost_v1"

xgboost.plan <- drake_plan(
  # Prepare column for writing output
  fct.column   = add_forecast_column(con, FCT.COL),
  # Prepare store/item list
  item.list    = item_list(con, FCT.COL),
  store.items  = get_store_items(item.list),
  store.list   = get_stores(store.items),
  # Preparing data for modelling
  raw.data     = fetch_data(con, store.list),
  trimmed_data = NULL,
  lagged.data  = NULL,
  # Training & forecast
  fit.xgboost  = fit_predict_xgboost()
)


make(xgboost.plan)

