
# Get simFin Data bulk download
# https://simfin.com/data/bulk


# Get stock prices (from Tickers.xlsx) ------------------------------------- #
library(tidyverse)
library(readxl)
library(BatchGetSymbols)
library(data.table)
library(lubridate)


dir_data <- "C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data-data/"
tickers <- read_excel(paste0(dir_data, "Tickers.xlsx"), sheet = 1) %>% 
    pull(Ticker)

# tickers %>% distinct(Description) %>% pull()

# tickers <- sample(tickers, 5)
options(future.rng.onMisuse = "ignore")
future::plan(future::multisession, workers = floor(parallel::detectCores()/2))

prices <- BatchGetSymbols(tickers = tickers,
                         last.date = Sys.Date(),
                         first.date = "1975-12-31", 
                         # freq.data = "weekly",
                         # do.parallel = TRUE,
                         do.cache=FALSE,
                         thresh.bad.data = 0,
                         be.quiet = TRUE)

# fwrite(prices$df.control, paste0("Control data from Tickers (", str_replace_all(Sys.Date(), "-", " "), ").csv"))

# Save
# fwrite(prices$df.tickers, paste0("Prices from Tickers (", str_replace_all(Sys.Date(), "-", " "), ").csv"))


prices_from_tickers <-
    prices$df.tickers %>%
    as_tibble() %>% 
    group_by(ticker) %>%
    rename(date = ref.date) %>% 
    complete(date = seq.Date(from = min(ymd(date)),
                             to = max(ymd(date)), by = "day")) %>%
    arrange(ticker, date) #%>%
    # slice(xts::endpoints(date, on = "weeks"))


# Save
fwrite(prices_from_tickers, paste0(dir_data, "Prices from Tickers (", str_replace_all(Sys.Date(), "-", " "), ").csv"))
fwrite(prices_from_tickers, paste0("C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/Prices from Tickers (", str_replace_all(Sys.Date(), "-", " "), ").csv"))



# Get FRED data ----------------------------------------------------------- #
library(tidyverse)
library(quantmod)
library(readxl)
library(data.table)

symbols <- read_excel(paste0(dir_data, "FRED Tickers.xlsx"), col_names = TRUE, sheet = 1, range = "A1:B100") %>% 
    drop_na("FRED Symbol") %>% 
    pull("FRED Symbol") %>% 
    .[. != "USDX"]


# symb <- c("DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS5", 
#   "DGS7", "DGS10", "DGS20", "DGS30")

# Get data for all tickers
prices_econ <- getSymbols(symbols, src = "FRED", from = "1975-12-31")#,
                          # auto.assign = FALSE)

Stocks <- lapply(symbols, function(sym) {
    na.omit(getSymbols(sym, src = "FRED", from = "1975-12-31", auto.assign=FALSE))
})
# Then to merge:
    
prices_econ <- data.frame(date = index(do.call(merge, Stocks)),
                          do.call(merge, Stocks)) %>% 
    as_tibble() %>% 
    pivot_longer(-date, names_to = "symbol", values_to = "price") %>% 
    arrange(symbol, date) %>% 
    drop_na()


# Save
fwrite(prices_econ, paste0(dir_data, "Prices from FRED (", str_replace_all(Sys.Date(), "-", " "), ").csv"))
fwrite(prices_econ, paste0("C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/Prices from FRED (", str_replace_all(Sys.Date(), "-", " "), ").csv"))



# ------------------------------------------------------------------------- #
# Get stock prices for each ticker in the fundamentals data
# ------------------------------------------------------------------------- #

# Download prices from Yahoo Finance
library(tidyverse)
library(lubridate)
library(BatchGetSymbols)

tickers <- read_lines(paste0(dir_data, "cleaned data/tickers_with_clean_prices.txt")) %>% 
    {.[. != "NA"]}
# tickers <- sample(tickers, 5)

options(future.rng.onMisuse = "ignore")

get_and_save_prices <- function(rng = 1:100) {
    # RNGkind("L'Ecuyer-CMRG")
    future::plan(future::multisession, workers = floor(parallel::detectCores()/2))
    prices_fund <- BatchGetSymbols(tickers = na.omit(tickers[rng]),
                                   last.date = Sys.Date(),
                                   first.date = "1975-12-31", 
                                   do.parallel = TRUE,
                                   thresh.bad.data = 0,
                                   be.quiet = TRUE)
    
    # Save
    data.table::fwrite(prices_fund$df.tickers, paste0(dir_data, "cleaned data/prices_daily_", first(rng), "_", last(rng), " (", today() %>% str_replace_all("-", " "), ").csv"))
    data.table::fwrite(prices_fund$df.control, paste0(dir_data, "cleaned data/df_control - prices_daily_", first(rng), "_", last(rng), " (", today() %>% str_replace_all("-", " "), ").csv"))    
    
}


get_and_save_prices(rng = 1:100)
get_and_save_prices(rng = 101:200)
get_and_save_prices(rng = 201:300)
get_and_save_prices(rng = 301:400)
get_and_save_prices(rng = 401:500)
get_and_save_prices(rng = 501:600)
get_and_save_prices(rng = 601:700)
get_and_save_prices(rng = 701:800)
get_and_save_prices(rng = 801:900)
get_and_save_prices(rng = 901:1000)
get_and_save_prices(rng = 1001:1100)
get_and_save_prices(rng = 1101:1200)
get_and_save_prices(rng = 1201:1300)
get_and_save_prices(rng = 1301:1400)
get_and_save_prices(rng = 1401:1500)
get_and_save_prices(rng = 1501:1600)
get_and_save_prices(rng = 1601:1700)
get_and_save_prices(rng = 1701:1800)


!!!!! Code to delete old price files



