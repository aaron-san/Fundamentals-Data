
# Get simFin Data bulk download
# https://simfin.com/data/bulk


# Get stock prices (from Tickers.xlsx) ------------------------------------- #
library(tidyverse)
library(readxl)
library(BatchGetSymbols)
library(data.table)
library(lubridate)


dir_data <- "C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/data/"
tickers <- read_excel(paste0(dir_data, "Tickers.xlsx"), sheet = 1) %>% 
    pull(Ticker)

# tickers %>% distinct(Description) %>% pull()

# tickers <- sample(tickers, 5)
options(future.rng.onMisuse = "ignore")
future::plan(future::multisession, workers = floor(parallel::detectCores()/2))

prices <- BatchGetSymbols(tickers = tickers,
                         last.date = Sys.Date(),
                         first.date = "1999-12-31", 
                         # freq.data = "weekly",
                         do.parallel = TRUE,
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

symbols <- read_excel(paste0(dir_data, "FRED Tickers.xlsx"), 
                      col_names = TRUE, sheet = 1, range = "A1:B100") %>% 
    drop_na("FRED Symbol") %>% 
    pull("FRED Symbol") %>% 
    .[. != "USDX"]


# symb <- c("DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS5", 
#   "DGS7", "DGS10", "DGS20", "DGS30")

# Get data for all tickers
# prices_econ <- getSymbols(symbols, src = "FRED", from = "1975-12-31")#,
#                           # auto.assign = FALSE)

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

dir_data <- "C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/data/"
tickers <- 
    read_lines(paste0(dir_data, 
                      "cleaned data/tickers_with_clean_prices.txt")) %>% 
    {.[. != "NA"]}

# 13,764 tickers !!!

tickers_in_fundamentals <- 
    read_tibble(paste0(dir_data, "cleaned data/fundamentals_consolidated.csv")) %>% 
    distinct(ticker) %>% pull()



# tickers <- sample(tickers, 5)

options(future.rng.onMisuse = "ignore")

download_Yhoo_prices <- function(rng = 1:100) {
    # RNGkind("L'Ecuyer-CMRG")
    future::plan(future::multisession, workers = floor(parallel::detectCores()/2))
    prices_fund <- BatchGetSymbols(tickers = na.omit(tickers[rng]),
                                   last.date = Sys.Date(),
                                   first.date = "1999-12-31", 
                                   freq.data = "monthly",
                                   do.parallel = TRUE,
                                   thresh.bad.data = 0,
                                   be.quiet = TRUE)
    
    # Save
    data.table::fwrite(prices_fund$df.tickers, 
                       paste0(dir_data, "cleaned data/prices_monthly_", first(rng),
                              "_", last(rng), " (", today() %>% 
                                  str_replace_all("-", " "), ").csv"))
    data.table::fwrite(prices_fund$df.control, 
                       paste0(dir_data, "cleaned data/df_control - prices_monthly_",
                              first(rng), "_", last(rng), " (", today() %>%
                                  str_replace_all("-", " "), ").csv"))
}


step <- 1000
start <- seq(from = 1, to = length(tickers), by = step)
end <- start + step - 1
end[length(end)] <- min(length(tickers), end[length(end)])

# start <- start[start >= 10200]
# end <- end[end > 10200]

for(i in seq_along(start)) {
    download_Yhoo_prices(rng = start[i]:end[i])
    print(paste0(start[i], "-", end[i], " done!"))
} 


!!!!! Code to delete old price files
# files_control <- list.files("data/cleaned data", full.names = TRUE, 
#                             pattern = "df_control") 
# 
# file_dates <- files_control %>% 
#     str_extract_all("[0-9]{4} [0-9]{2} [0-9]{2}") %>% flatten_chr() %>% 
#     as.Date("%Y %m %d")
# 
# dates_to_keep <- file_dates[file_dates > (Sys.Date() - days(14))] %>% unique()
# 
# files_control[str_detect(files_control, as.character(dates_to_keep))]
# 
# 
# # Find the control files for the most recent download date
# delete_lgl <- files_control %>% str_detect(max_date) %>% map_lgl(isFALSE)
# files_control[delete_lgl]
# file.remove()





