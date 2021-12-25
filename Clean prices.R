


# Identify tickers that could not be downloaded cleanly
#  the last time with BatchGetSymbols
files_control <- get_recent_price_dirs(pattern = "df_control", 
                                       period = "monthly")
tickers_with_dirty_prices <- 
      files_control %>%
      map_df(~read_csv(.x, col_types = cols()) %>% 
                   filter(threshold.decision == "OUT")) %>% 
      pull(ticker)

tickers_with_clean_prices <-
      fundamentals_consolidated %>% 
      distinct(ticker) %>% 
      # Filter out tickers that could not be downloaded cleanly using BatchGetSymbols
      filter(!ticker %in% tickers_with_dirty_prices) %>% 
      pull(ticker) %>% 
      # Manually add tickers that might not be present
      c("SOFI", "PLTR") %>% 
      unique()
length(tickers_with_clean_prices)


# Save list of tickers to use when downloading prices
write_lines(tickers_with_clean_prices, 
            "data/cleaned data/tickers_with_clean_prices.txt")


# 452 companies have over 10 years of data
# 1,375 --> 4,935 companies have over 7 years of data
# 1,651 companies have over 5 years of data


#~~~~~~~~~~~~~~~~~~~~~~~~~#
# Prices data             #
#~~~~~~~~~~~~~~~~~~~~~~~~~#

prices_from_Tickers_file <- list.files("data", pattern = "Prices from Tickers", 
                                       full.names = TRUE) %>% max()
prices_from_Tickers <- 
      read_tibble(prices_from_Tickers_file, date_format = "%m/%d/%Y") %>% 
      select(ticker, date, close = price.close, adjusted = price.adjusted,
             adj_return_daily = ret.adjusted.prices)

prices_SP500TR <- prices_from_Tickers %>% filter(ticker == "^SP500TR")

prices_SP500TR_monthly <-
      prices_SP500TR %>%
      rename(SP500TR_adjusted = adjusted) %>% 
      slice(endpoints(date, on = "months")) %>% 
      # Round dates to end of month (sometimes the last trading
      #  day of a month is before the last day of the month)
      mutate(rounded_date = round_date(date, unit = "month") - days(1),
             SP500TR_adj_return_monthly = SP500TR_adjusted / lag(SP500TR_adjusted, 1) - 1,
             SP500TR_adj_return_qtrly = SP500TR_adjusted / lag(SP500TR_adjusted, 3) - 1) %>% 
      # Rounding the dates sometimes creates duplicate dates if the 
      # most recent date is rounded down to the prior month-end, so
      # remove such duplicates in that case
      filter(!duplicated(rounded_date)) %>% 
      select(ticker, rounded_date, everything()) %>% 
      rename(report_date = rounded_date) %>% 
      select(-date)


# Save
fwrite(prices_SP500TR_monthly, 
       paste0(dir_data, "data/cleaned data/", "prices_SP500TR.csv"))



#--------------#
# Stock prices
#--------------#

# Daily prices
files_prices_daily <- 
      get_recent_price_dirs(period = "daily", dir = "data/cleaned data")


prices_daily_raw <-
      files_prices_daily %>%
      map_df(~read_tibble(.x)) %>%
      select(ticker, date = ref.date, close = price.close, 
             adjusted = price.adjusted,
             adj_return_daily = ret.adjusted.prices) %>% 
      as.data.table() %>% 
      setorder(ticker, date) %>% 
      # Remove rows that contain a duplicate ticker-date key
      # filter(!duplicated(ticker, date)) %>%
      unique(by = c("ticker", "date")) %>% 
      as_tibble() %>% 
      mutate(adjusted = ifelse(adjusted < 0, NA, adjusted)) %>% 
      full_join(prices_from_Tickers)

# prices_daily_raw %>% filter(ticker == "SOFI")

prices_daily_raw_monthly <- 
      prices_daily_raw %>%
      group_by(ticker) %>% 
      slice(endpoints(date, on = "months")) %>% 
      select(!contains("return")) %>% 
      rename(report_date = date)


# Monthly prices
files_prices_monthly <- 
      get_recent_price_dirs(period = "monthly", dir = "data/cleaned data")

prices_monthly_raw <- 
      files_prices_monthly %>% 
      map_df(~read_tibble(.x)) %>%
      select(ticker, report_date = ref.date, close = price.close, 
             adjusted = price.adjusted,
             adj_return_monthly = ret.adjusted.prices) %>% 
      as.data.table() %>% 
      setorder(ticker, report_date) %>%
      # Remove rows that contain a duplicate ticker-date key
      unique(by = c("ticker", "report_date")) %>% 
      as_tibble() %>% 
      mutate(adjusted = ifelse(adjusted < 0, NA, adjusted)) %>% 
      full_join(prices_daily_raw_monthly)

# prices_monthly_raw %>% filter(ticker == "SOFI")




# Weekly prices can be use for sd, drawdown or other metrics.
RSI_n <- function(x, n) {
      if(length(x) > n + 1) {
            rsi <- TTR::RSI(x, n = n)
      } else {
            rsi <- NA
      }
}

price_index_n <- function(x, n) {
      if(length(x) > n + 1) {
            price_index <- signif(x / runMax(x, n = n), digits = 2)
      } else {
            price_index <- NA
      }
}


max_drawdown <- function(rets) {
      rets_index <- cumprod(1 + rets)
      drawdown <- 1 - rets_index/cummax(rets_index)
      max_dd <- max(1 - rets_index/cummax(rets_index))
}



prices_daily_ratios <-
      prices_daily_raw %>% 
      rename(report_date = date) %>% 
      as.data.table() %>% 
      setorder(ticker, report_date) %>% 
      unique() %>% 
      as_tibble() %>% 
      drop_na() %>%
      # filter(date >= Sys.Date() - years(10), date <= Sys.Date()) %>% 
      group_by(ticker) %>%
      # filter(ticker == "INST") %>%
      mutate(RSI_14D = RSI_n(adjusted, n = 14),
             price_index_3Y = adjusted / first(adjusted),
             max_drawdown_3Y = max_drawdown(adj_return_daily)) %>%
      # mutate(price_index = adjusted / first(adjusted)) %>% 
      ungroup() %>% 
      select(ticker, report_date, everything())


# most_recent_data <-
#   prices_daily_ratios %>% 
#   group_by(ticker) %>% 
#   filter(date == max(date))
# colnames(most_recent_data) <- c("ticker", paste0("most_recent_", colnames(most_recent_data[, -1])))

# Save 
fwrite(prices_daily_ratios, 
       paste0(dir_data, "data/cleaned data/prices_daily_ratios (", 
              Sys.Date() %>% str_replace_all("-", " "), ").csv"))


prices_monthly <- 
      prices_monthly_raw %>%
      group_by(ticker) %>% 
      # mutate(return_monthly_adj = adjusted / lag(adjusted) - 1) %>% 
      mutate(sd_adj_returns_annualized = 
                   slide_dbl(adj_return_monthly, sd, .before = 11) * sqrt(12)) %>% 
      # Round report_period dates to nearest month-end
      mutate(rounded_date = round_date(report_date, unit = "month") - days(1)) %>% 
      mutate(price_index = adjusted / first(adjusted)) %>% 
      select(-report_date, -adj_return_monthly) %>% 
      select(ticker, report_date = rounded_date, everything()) %>%
      # Rounding the dates sometimes creates duplicate dates if the 
      # most recent date is rounded down to the prior month-end, so
      # remove such duplicates in that case
      filter(!duplicated(report_date)) %>% 
      ungroup() %>% 
      filter(report_date <= Sys.Date())

# Save
fwrite(prices_monthly, paste0(dir_data, "data/cleaned data/prices_monthly (", 
                              Sys.Date() %>% str_replace_all("-", " "),
                              ").csv"))

# prices_monthly %>% filter(ticker == "PLTR")


tickers_from_prices <- prices_monthly %>% distinct(ticker) %>% pull()


# S&P 500 Prices
prices_SP500TR_monthly_file <- list.files("data/cleaned data", 
                                          pattern = "prices_SP500TR.csv", 
                                          full.names = TRUE)
prices_SP500TR_monthly <- read_tibble(prices_SP500TR_monthly_file)

