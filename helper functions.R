
suppressPackageStartupMessages({
    library(arrow)
    library(tidyverse)
    library(lubridate)
    library(xts)
    library(data.table)
    library(quantmod)
    # library(RcppRoll) # roll_mean, roll_sd, roll_max, roll_min, roll_var, roll_sum, roll_prod, roll_median
    library(slider) # slide_dbl()
})

dir_data <- "C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/"



read_tibble <- function(x, date_format = "%Y-%m-%d", ...) {
    
      ####
      # x <- input_data_file
      # date_format <- "%m/%d/%Y"
      # x <- file_yhoo_profiles[1]
      # date_format <- "%Y_%m_%d"
      # date_format <- "%Y-%m-%d"
      ####
      
    x %>%
        fread(fill = TRUE, ...) %>%
        as_tibble() %>%
        mutate(across(where(is.Date), ~as.Date(.x))) %>% 
        mutate(across(which(sapply(., class) == "integer64" ), as.numeric)) %>% 
        mutate(across(any_of("date"), ~as.Date(.x, date_format)))
}


# Round report date to nearest month-end
  get_rounded_date <- . %>% 
    # as_tibble() %>% 
    mutate(rounded_date = round_date(report_date, unit = "month") - days(1)) %>%
    select(-report_date) %>% 
    select(ticker, rounded_date, everything())
    
# get_rounded_date(tibble(ticker = "MSFT", report_date = ymd("2021-12-29")))


# Some tickers have multiple instances of a date but unequal values, so
#  remove NA values for all tickers and then remove any duplicate 
#  instances of a given ticker and date, keeping the most recent value

# Choose the most recent value where two values or more exist for a given rounded_date
#  - Arrange descending so the most recent value for two nearby dates appears
#  - first and then round them both to the nearest month end, then drop the second (more dated value)

remove_duplicates <- . %>% 
    as.data.table() %>% 
    setorder(report_date) %>%
    # Remove rows that have all NAs
    filter(rowSums(is.na(select(., -c(ticker, report_date)))) != ncol(select(., -c(ticker, report_date)))) %>%
    setorder(ticker, -report_date) %>% 
    add_column(n_vals = rowSums(!is.na(select(., -c(ticker, report_date))))) %>%
    get_rounded_date() %>%
    setorder(rounded_date, -n_vals) %>%
    # Remove duplicate instances of a date for a given ticker
    filter(!duplicated(select(., ticker, rounded_date))) %>% 
    select(-n_vals) %>% 
    setorder(ticker, rounded_date) %>% 
    as_tibble()



# Fill NAs up to two periods (months)
fill_na_two_periods_max <- function(x) {
    #--------#
    # x <- fundamentals_full_dates %>%
    #     filter(ticker == "AAL") %>% pull(research_development)
    #--------#
    non_NA <- which(!is.na(x))
    # If there are no NAs, return x
    if(length(non_NA) == 0) return(x)
    
    diffs <- diff(non_NA)
    for(i in seq_along(diffs)) {
        # i <- 1
        if(diffs[i] <= 3) {
            x[non_NA[i] + seq_len(diffs[i] - 1)] <- x[non_NA[i]]
        } else if(diffs[i] > 3) {
            x[non_NA[i] + 1:2] <- x[non_NA[i]]
        }
    }
    if(last(non_NA) < length(x)) {
        x[last(non_NA) + 1:(min(2, length(x) - last(non_NA)))] <- x[last(non_NA)]
    }
    return(x)
}


read_and_clean <- function(file) {
    ######
    # file <- files_prices[1]
    ######
    
    read_tibble(file) %>% 
        # as_tibble() %>% 
        select(ticker, date = ref.date, close = price.close, 
               adjusted = price.adjusted) %>% 
        group_by(ticker) %>% 
        fill(close, adjusted, .direction = "down") %>% 
        slice(endpoints(date, on = "months")) %>% 
        arrange(ticker, date)
}




# Given tickers and fields, find a complete series of data, if it exists
# Reduce range of tickers and dates to get a complete series for a given field
get_complete_series <- function(data, fields, date_range_yrs = 5) {
    #######
    # data <- ratios_final
    # date_range_yrs <- 3
    # fields <- fields_to_use
    
    # fields <- c("ticker", "fundamentals_date", "sector_yhoo",
    # "free_cash_flow_to_assets", "operating_income_1Q")
    # fields <- c("ticker",
    #            "fundamentals_date",
    #            "sector_yhoo",
    #            "adj_return_6M",
    #            "free_cash_flow_to_assets",
    #            # "leverage_index",
    #            "total_accruals_to_total_assets")
    #######
    
    # Choose the columns specified
    data_subset <-
        data %>% 
        select(ticker, fundamentals_date, any_of(fields))
    
    
    # Search through all possible time spans of length "date_range_yrs" and
    #  find the date range with the fewest NAs
    date_range_w_least_nas <-
        data_subset %>%
        select(-ticker) %>% 
        group_by(fundamentals_date) %>% 
        nest() %>% 
        mutate(fundamentals_date, data_points = map_dbl(data, ~ncol(.x)*nrow(.x))) %>% 
        # left_join(data_subset %>% select(-ticker) %>% count(fundamentals_date)) %>% 
        mutate(na_perc = sum(is.na(unlist(data))) / data_points) %>% 
        ungroup() %>% 
        arrange(fundamentals_date) %>% 
        
        mutate(p_na_trailing_n_yrs = slider::slide_dbl(na_perc, sum, .before = 12*date_range_yrs-1, .complete = TRUE)) %>% 
        slice_min(p_na_trailing_n_yrs) %>% 
        tail(1) %>% 
        {tibble(min_date = .$fundamentals_date - lubridate::years(date_range_yrs), 
                max_date = .$fundamentals_date)}
    
    # Get most complete date range
    data_cl_dates <-
        data_subset %>% 
        filter(between(fundamentals_date, 
                       date_range_w_least_nas$min_date, 
                       date_range_w_least_nas$max_date))
    
    # Get tickers with no NAs in most complete date range
    tickers_to_use <-
        data_cl_dates %>% 
        select(-fundamentals_date) %>% 
        group_by(ticker) %>% 
        nest() %>% 
        transmute(ticker,
                  na_count = sum(is.na(unlist(data)))) %>% 
        filter(na_count == 0) %>% 
        pull(ticker)
    
    if(length(tickers_to_use) == 0) 
        warning("Not enough data available for chosen inputs.")
    
    
    # Clean the sector_yhoo variable by setting sector_yhoo equal to the
    #  first observed value for each ticker
    data_cl_sector <-
        data_cl_dates %>% 
        filter(ticker %in% tickers_to_use) %>% 
        # Set the sector of the ticker to the first observed sector value
        # (ticker MGPI had two unique sectors)
        group_by(ticker) %>% 
        mutate(sector_yhoo = first(sector_yhoo)) %>% 
        ungroup() %>% 
        distinct() %>% 
        arrange(ticker, fundamentals_date)
    
    # If there are any NAs remaining, raise a warning.
    if(data_cl_sector %>% is.na() %>% any()) 
        warning("There are some NAs in the data.")
    
    date_diffs <-
        data_cl_sector %>% 
        arrange(fundamentals_date) %>% 
        group_by(ticker) %>%
        select(ticker, fundamentals_date) %>% 
        mutate(date_diff = as.numeric(fundamentals_date - lag(fundamentals_date))) %>% 
        # Replace each initial NA with a safe number, 30, so that the row isn't dropped later
        mutate(date_diff = replace(date_diff, row_number() == 1, 30))
    
    # date_diffs[!dplyr::between(date_diffs$date_diff, 27, 32), , drop = FALSE]

    
    # If any of the date differences are not between 27 and 32, then stop
    if(!date_diffs %>% pull(date_diff) %>% between(., 27, 32) %>% all())
        warning("Some date sequences are incomplete.")
    
    return(data_cl_sector)
}








