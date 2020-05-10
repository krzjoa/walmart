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
library(keyring)
library(RClickhouse)

#' Script
#' Could not find libsecret headers or libs.
#' dpkg -L libsecret-1-0 
#' locate libsecret| grep '\.pc'
#' result: 
#' /usr/lib/x86_64-linux-gnu/pkgconfig/
#' export PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig/
#' gnome-keyring-daemon
#' On Ubuntu, you need to install libsecret-1-dev via apt.
#' On RedHat, Fedora, and CentOS, you need to install libsecret-devel via yum or dnf.
#' Note that in addition to libsecret, you either need pkg-config or set the
#' LIBSECRET_CFLAGS and LIBSECRET_LIBS environment variables.

# sudo apt-get install -y gnome-keyring
# sudo service clickhouse-server start 
# getOption("keyring_keyring")
# https://wiki.archlinux.org/index.php/GNOME/Keyring

options(keyring_keyring = "test")
keyring::keyring_unlock("test")
# options(keyring_backend = "secret_service")

# keyring::backend_secret_service$new() -> ss
# ss$keyring_create("test")
# ss$keyring_set_default("test")
# keyring::default_backend("test")

# ss$set("clickhouse.pwd")
# keyring::default_backend(keyring = "test")
# keyring::backend_secret_service$new()
# keyring::key_set("clickhouse.pwd")

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