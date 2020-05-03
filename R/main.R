# Import libraries
library(drake)
library(data.table)
library(magrittr)
library(dplyr)
library(DBI)
library(here)
library(parsnip)
library(recipes)
library(glue)
library(RClickhouse)

# sudo service clickhouse-server start 
keyring::key_set("clickhouse.pwd")

# Setup connection
con <- dbConnect(drv      = RClickhouse::clickhouse(), 
                 host     = "localhost", 
                 port     = 9000, 
                 user     = "default", 
                 password = keyring::key_get("clickhouse.pwd"))

# Sourcing scripts
source(here("config.R"))
source(here("setup_db.R"))
source(here("fit-models.R"))

# Drake plans
plans <- bind_plans(
  # download.plan,
  data.plan,
  # Modelling
  xgboost.plan
)

# dbDisconnect(con)
make(plans, garbage_collection = TRUE,
     memory_strategy = 'preclean')