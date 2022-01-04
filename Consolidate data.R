
# Consolidate data from multiple sources
#  - Write processed data to this (and other) project's data directory
#  - Data sources: Yhoo, Simfin, Factset

rm(list = ls())

the_start <- Sys.time()

dir_data <- "C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Download Yahoo fundamentals
# SLOW!!! (over 5,000 tickers' worth of fundamentals)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# reticulate::source_python("C:/Users/user/Desktop/Aaron/R/Projects/Yahoo-Data/get_yahoo_data.py")
# rm(list = ls()[!ls() %in% grep("start|end|dir_data", ls(), value = TRUE)])
# source("C:/Users/user/Desktop/Aaron/R/Projects/Yahoo-Data/clean data.R")
# rm(list = ls()[!ls() %in% grep("start|end|dir_data", ls(), value = TRUE)])
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# FRED and Yahoo prices
# Get prices from FRED and Yahoo
# Slow! > ~41 mins
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# start98 <- Sys.time()
# source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/Download prices from Yahoo and FRED.R")
# end98 <- Sys.time(); end98 - start98
# rm(list = ls()[!ls() %in% grep("start|end|dir_data", ls(), value = TRUE)])
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# SimFin data cleanup
# Consolidate and clean up any newly added simFin downloaded fundamentals
# Slow ~22-28 mins
#  - Only run if new simFin fundamentals were downloaded
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# start37 <- Sys.time()
# source("C:/Users/user/Desktop/Aaron/R/Projects/simfinR/data/unzip files.R")
# source("C:/Users/user/Desktop/Aaron/R/Projects/simfinR/SimFin Data Cleaning.R")
# end37 <- Sys.time(); end37 - start37
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# EDGAR data cleanup
if(pingr::is_online()) {
  source("../EDGAR data/helper functions.R")
  source("../EDGAR data/EDGAR data - consolidation.R")
}
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


# Load libraries and helper functions
source("helper functions.R")


#---------------------------------#
# Part 1 - SimFin Fundamentals    #
#  - https://simfin.com/data/bulk #
#  - downloaded and cleaned       #
#---------------------------------#

dir_sf <- "C:/Users/user/Desktop/Aaron/R/Projects/simfinR/data/cleaned data/"


simfin_shares_basic_file <- list.files(dir_sf, pattern = "simfin_shrs_outs", full.names = TRUE) %>% max()
simfin_is_quarterly_file <- list.files(dir_sf, pattern = "simfin_income_statements_quarterly", full.names = TRUE) %>% max()
simfin_bs_quarterly_file <- list.files(dir_sf, pattern = "simfin_balance_sheets_quarterly", full.names = TRUE) %>% max()
simfin_cf_quarterly_file <- list.files(dir_sf, pattern = "simfin_cash_flow_statements_quarterly", full.names = TRUE) %>% max()
simfin_is_yearly_file <- list.files(dir_sf, pattern = "simfin_income_statements_yearly", full.names = TRUE) %>% max()
simfin_bs_yearly_file <- list.files(dir_sf, pattern = "simfin_balance_sheets_yearly", full.names = TRUE) %>% max()
simfin_cf_yearly_file <- list.files(dir_sf, pattern = "simfin_cash_flow_statements_yearly", full.names = TRUE) %>% max()

# Shares
simfin_shares_basic <- 
  read_tibble(simfin_shares_basic_file) %>% 
  rename(shares_basic = shares_outstanding)

simfin_income_statements_quarterly <- 
    read_tibble(simfin_is_quarterly_file) %>% 
    add_column(form = "income_statement") %>% 
    add_column(source = "simfin") %>% 
    mutate(cost_of_revenue = cost_of_revenue * -1,
           operating_expenses = operating_expenses * -1,
           selling_general_administrative = selling_general_administrative * -1,
           research_development = research_development * -1)
simfin_income_statements_yearly <- 
    read_tibble(simfin_is_yearly_file) %>% 
    add_column(form = "income_statement") %>% 
    add_column(source = "simfin") %>% 
    mutate(cost_of_revenue = cost_of_revenue * -1,
           operating_expenses = operating_expenses * -1,
           selling_general_administrative = selling_general_administrative * -1,
           research_development = research_development * -1)


simfin_balance_sheets_quarterly <- 
    read_tibble(simfin_bs_quarterly_file) %>% 
    add_column(form = "balance_sheet") %>% 
    add_column(source = "simfin")
simfin_balance_sheets_yearly <- 
    read_tibble(simfin_bs_yearly_file) %>% 
    add_column(form = "balance_sheet") %>% 
    add_column(source = "simfin")

simfin_cash_flow_statements_quarterly <- 
    read_tibble(simfin_cf_quarterly_file) %>% 
    add_column(form = "cash_flow_statement") %>% 
    add_column(source = "simfin")
simfin_cash_flow_statements_yearly <- 
    read_tibble(simfin_cf_yearly_file) %>% 
    add_column(form = "cash_flow_statement") %>% 
    add_column(source = "simfin")


simfin_income_statements_quarterly <- 
  read_tibble(simfin_is_quarterly_file) %>%
  add_column(form = "income_statement") %>% 
  add_column(source = "simfin") %>% 
  mutate(cost_of_revenue = cost_of_revenue * -1,
         operating_expenses = operating_expenses * -1,
         selling_general_administrative = selling_general_administrative * -1,
         research_development = research_development * -1)
simfin_income_statements_yearly <- 
  read_tibble(simfin_is_yearly_file) %>% 
  add_column(form = "income_statement") %>% 
  add_column(source = "simfin") %>% 
  mutate(cost_of_revenue = cost_of_revenue * -1,
         operating_expenses = operating_expenses * -1,
         selling_general_administrative = selling_general_administrative * -1,
         research_development = research_development * -1)




# fwrite(simfin_cash_flow_statements_yearly %>% filter(ticker == "UPS"), "simfin_cash_flow_statements_yearly.csv")



#------------------------------#
# Part 2 - Yahoo fundamentals  #
# - scraped & cleaned          #
#------------------------------#

dir_yh1 <- "C:/Users/user/Desktop/Aaron/R/Projects/Yahoo-Data/data/cleaned"

#~~~~~~~~~~~~~~~~~~#
# Balance Sheets
#~~~~~~~~~~~~~~~~~~#
current_bs_quarterly <- list.files(dir_yh1, pattern = "yhoo_balance_sheets_quarterly", full.names = TRUE) %>% max()
current_bs_yearly <- list.files(dir_yh1, pattern = "yhoo_balance_sheets_yearly", full.names = TRUE) %>% max()

yhoo_balance_sheets_quarterly <- 
    read_tibble(current_bs_quarterly) %>% 
    add_column(form = "balance_sheet") %>% 
    add_column(source = "yhoo")
yhoo_balance_sheets_yearly <- 
  read_tibble(current_bs_yearly) %>% 
    add_column(form = "balance_sheet") %>% 
    add_column(source = "yhoo")





#~~~~~~~~~~~~~~~~~~~~#
# Income Statements
#~~~~~~~~~~~~~~~~~~~~#
current_is_yearly <- list.files(dir_yh1, pattern = "income_statements_yearly", full.names = TRUE) %>% max()
current_is_quarterly <- list.files(dir_yh1, pattern = "income_statements_quarterly", full.names = TRUE) %>% max()

yhoo_income_statements_yearly <- 
    read_tibble(current_is_yearly) %>% 
    add_column(form = "income_statement") %>% 
    add_column(source = "yhoo")
yhoo_income_statements_quarterly <- 
    read_tibble(current_is_quarterly) %>% 
    add_column(form = "income_statement") %>% 
    add_column(source = "yhoo")

# Internal consolidation
# - Consolidate 'ebit' and 'operating_income' fields
consolidate_operating_income <- function(df) {
    for(i in seq_len(nrow(df))) {
        # If the operating_income field is missing a figure
        #  and the ebit field contains a figure, fill in the
        #  operating_income field with the ebit figure
        if(is.na(df[i, "operating_income"]) &
           !is.na(df[i, "ebit"])) {
            df[i, "operating_income"] <- df[i, "ebit"]
        }
    }
    # Deselect 'ebit' since it was merged with 'operating_income'
    df <- df %>% select(-ebit)
    return(df)
}


yhoo_income_statements_yearly <- 
    yhoo_income_statements_yearly %>% 
    consolidate_operating_income()

yhoo_income_statements_quarterly <-
    yhoo_income_statements_quarterly %>% 
    consolidate_operating_income()



#~~~~~~~~~~~~~~~~~~~~~~#
# Cash Flow Statements
#~~~~~~~~~~~~~~~~~~~~~~#
file_yhoo_cf_yearly <- list.files(dir_yh1, pattern = "cash_flows_yearly", full.names = TRUE) %>% max()
yhoo_cash_flow_statements_yearly <- 
    read_tibble(file_yhoo_cf_yearly) %>% 
    add_column(form = "cash_flow_statement") %>% 
    add_column(source = "yhoo") %>% 
    mutate(repurchase_of_stock = repurchase_of_stock * -1)


file_yhoo_cf_quarterly <- list.files(dir_yh1, pattern = "cash_flows_quarterly", full.names = TRUE) %>% max()
yhoo_cash_flow_statements_quarterly <- 
    read_tibble(file_yhoo_cf_quarterly) %>% 
    add_column(form = "cash_flow_statement") %>% 
    add_column(source = "yhoo") %>% 
    mutate(repurchase_of_stock = repurchase_of_stock * -1)


#-----------------------------------------------#
# Part 4 - Factset data                         #
# Only run if the raw Factset data is updated
# source("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/Factset data/Clean Factset data.R")
#-----------------------------------------------#

# Read
fs_yearly_file <- list.files(paste0(dir_data, "data/cleaned data"), 
                             pattern = "fs_yearly", full.names = TRUE)
fs_yearly <- read_tibble(fs_yearly_file)

factset_balance_sheets_yearly <- 
    fs_yearly %>% 
    select(ticker, period, report_date, source, common_equity, 
           long_term_debt, total_current_liabilities, 
           total_current_assets, com_eq_retain_earn, 
           short_long_term_debt, eq_tot, property_plant_equipment, 
           net_receivables, pfd_stk, cash_and_short_term_investments, 
           total_liab, pay_tax, intang_oth, total_assets)

factset_income_statements_yearly <- 
    fs_yearly %>% 
    select(ticker, period, report_date, source, gross_profit, 
           revenue, capex, net_inc_basic, div_pfd, 
           operating_income_loss,
           selling_general_administrative, ebitda_oper)

factset_cash_flow_statements_yearly <- 
    fs_yearly %>% 
    select(ticker, period, report_date, source, 
           total_cash_from_operating_activities, depreciation,
           issuance_of_stock, repurchase_of_stock)


#-----------------------------------------------#
#  Part 5 - EDGAR data                          #
#  Downloaded and cleaned with EDGAR data.Rproj #
# - Every 3 months, download bulk folder from 
#   https://www.sec.gov/dera/data/financial-statement-data-sets.html
# - Then, clean up with EDGAR data - consolidation.R
#   from the "EDGAR data" project
#-----------------------------------------------#

dir_r <- "C:/Users/user/Desktop/Aaron/R/Projects/EDGAR data/data/cleaned"
edgar_files <- 
  list.files(dir_r, pattern = "edgar_.*.csv", full.names = TRUE)
edgar_is_file <- edgar_files %>% str_subset("income_statements")
edgar_bs_file <- edgar_files %>% str_subset("balance_sheets")
edgar_cf_file <- edgar_files %>% str_subset("cash_flows")

edgar_income_statements_quarterly <- 
    read_tibble(edgar_is_file) %>% 
    add_column(form = "income_statement", source = "edgar", 
               period = "quarterly")
edgar_balance_sheets_quarterly <- 
  read_tibble(edgar_bs_file) %>% 
  add_column(form = "balance_sheet", source = "edgar", 
             period = "quarterly")
edgar_cash_flow_statements_quarterly <- 
  read_tibble(edgar_cf_file) %>% 
  add_column(form = "cash_flow_statement", source = "edgar", 
               period = "quarterly")


# EDGAR profile data
edgar_profile_data <-
  reduce(list(edgar_income_statements_quarterly %>%
                           select(ticker, name_is_q = name, 
                                  industry_is_q = industry, 
                                  industry_detail_is_q = industry_detail) %>%
                           as.data.table() %>% unique() %>% as_tibble(),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, name_bs_q = name, 
                                  industry_bs_q = industry, 
                                  industry_detail_bs_q = industry_detail) %>%
                           as.data.table() %>% unique() %>% as_tibble(),
                         edgar_cash_flow_statements_quarterly %>% 
                           select(ticker, name_cf_q = name, 
                                  industry_cf_q = industry, 
                                  industry_detail_cf_q = industry_detail) %>%
                           as.data.table() %>% unique() %>% as_tibble()),
         full_join) %>% 
  transmute(ticker, name = coalesce(name_is_q, name_bs_q, name_cf_q), 
            industry = coalesce(industry_is_q, industry_bs_q, industry_cf_q),
            industry_detail = coalesce(industry_detail_is_q, 
                                       industry_detail_bs_q, 
                                       industry_detail_cf_q)) %>% 
  mutate(name = str_remove_all(name, "\\.") %>% str_remove_all("\\,")) %>% 
  distinct() %>% 
  group_by(ticker) %>%
  # Select the industry_detail with the shortest label
  filter(industry_detail == min(industry_detail)) %>% 
  # Select the first occurrence of rows with duplicate tickers
  slice_head(n = 1) %>% 
  ungroup()


# Save to this project's data directory
fwrite(edgar_profile_data, paste0(dir_data, "data/cleaned data/edgar_profiles (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))
# Save to stock-analysis data directory
fwrite(edgar_profile_data, paste0("C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/cleaned data/edgar_profiles (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))


#--------------------------#
#  Part 6 - GraphFundamentals.com data
#--------------------------#

dir_r <- "C:/Users/user/Desktop/Aaron/R/Projects/GraphFundamentals-Data/data/cleaned data"

# Balance sheet files
bs_files <- list.files(dir_r, pattern = "balance_sheets_cleaned", 
                       full.names = TRUE)
max_date_bs_files <- bs_files %>% 
  str_extract("(?<=\\()[0-9]{4} [0-9]{2} [0-9]{2}(?=\\))") %>% max()
bs_data_file <- bs_files %>% str_subset(max_date_bs_files)

# Income statement files
is_files <- list.files(dir_r, pattern = "income_statements_cleaned", 
                       full.names = TRUE)
max_date_is_files <- is_files %>% 
  str_extract("(?<=\\()[0-9]{4} [0-9]{2} [0-9]{2}(?=\\))") %>% max()
is_data_file <- is_files %>% str_subset(max_date_is_files)

# Cash flow files
cf_files <- list.files(dir_r, pattern = "cash_flows_cleaned", 
                       full.names = TRUE)
max_date_cf_files <- cf_files %>% 
  str_extract("(?<=\\()[0-9]{4} [0-9]{2} [0-9]{2}(?=\\))") %>% max()
cf_data_file <- cf_files %>% str_subset(max_date_cf_files)


graphfund_balance_sheets_quarterly <- 
  map_df(bs_data_file, read_tibble) %>% 
  rename(report_date = date) %>% 
  mutate(goodwill = replace_na(goodwill, 0),
         other_intangible_assets_excluding_goodwill = 
           replace_na(other_intangible_assets_excluding_goodwill, 0))

graphfund_income_statements_quarterly <- 
  map_df(is_data_file, read_tibble) %>% 
  rename(report_date = date) %>% 
  mutate(cost_of_revenue = abs(cost_of_revenue),
         total_operating_expense = abs(total_operating_expenses))

graphfund_cashflows_quarterly <- 
  map_df(cf_data_file, read_tibble) %>% 
  rename(report_date = date) %>% 
  select(-net_interest_paid)


#--------------------------#
#  Part 6 - Input data
#  - Data that was input manually
#--------------------------#
source("helper functions.R")
library(tidyverse)
library(data.table)

dir_r <- "C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/data/Input data"
input_data_fundamentals_file <- 
  list.files(dir_r, pattern = "Input data - Fundamentals", 
             full.names = TRUE) %>% max()
input_data_fundamentals <- 
  read_tibble(input_data_fundamentals_file, date_format = "%m/%d/%Y") %>% 
  mutate(fundamentals_date = as.Date(fundamentals_date, "%m/%d/%Y")) %>%
  select(ticker, report_date = fundamentals_date, everything())


input_data_profiles_file <- 
  list.files(dir_r, pattern = "Input data - Profiles", 
             full.names = TRUE) %>% max()
input_data_profiles <- 
  read_tibble(input_data_profiles_file, date_format = "%m/%d/%Y") %>% 
  # Remove empty columns
  janitor::remove_empty(which = "cols")


#--------------------------#
#  Part 7 -  Consolidation
#  - Consolidate Yahoo industry names with SimFin industry names
#  - Consolidate field names
#--------------------------#


#~~~~~~~~~~~~~~~~~~~#
# Balance Sheets
#~~~~~~~~~~~~~~~~~~~#



# Consolidate total_liabilities
total_liabilities <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                 yhoo_total_liab_1Q = total_liab),
                        yhoo_balance_sheets_yearly %>% 
                          select(ticker, report_date, 
                                 yhoo_total_liab_1Y = total_liab),
                        simfin_balance_sheets_quarterly %>% 
                          select(ticker, report_date, 
                                 simfin_total_liab_1Q = total_liabilities),
                        simfin_balance_sheets_yearly %>% 
                          select(ticker, report_date, 
                                 simfin_total_liab_1Y = total_liabilities),
                        factset_balance_sheets_yearly %>% 
                          select(ticker, report_date, 
                                 factset_total_liab_1Y = total_liab),
                        edgar_balance_sheets_quarterly %>% 
                          select(ticker, report_date, 
                                 edgar_total_liab_1Q = total_liab),
                        input_data_fundamentals %>% 
                          select(ticker, report_date, 
                                 input_data_total_liabilities = 
                                   total_liabilities),
                        graphfund_balance_sheets_quarterly %>% 
                          select(ticker, report_date, 
                                 graphfund_total_liabilities = 
                                   total_liabilities)),
         full_join) %>% 
  transmute(ticker, report_date, 
            total_liabilities = coalesce(edgar_total_liab_1Q, 
                                         factset_total_liab_1Y, 
                                         yhoo_total_liab_1Y, 
                                         simfin_total_liab_1Y, 
                                         yhoo_total_liab_1Q, 
                                         simfin_total_liab_1Q, 
                                         yhoo_total_liab_1Q,
                                         input_data_total_liabilities,
                                         graphfund_total_liabilities)) %>% 
  remove_duplicates()


# Consolidate total_assets
total_assets <-
  reduce(
    list(
      yhoo_balance_sheets_quarterly %>%
        select(ticker, report_date,
          yhoo_total_assets_1Q = total_assets
        ),
      yhoo_balance_sheets_yearly %>%
        select(ticker, report_date,
          yhoo_total_assets_1Y = total_assets
        ),
      simfin_balance_sheets_quarterly %>%
        select(ticker, report_date,
          simfin_total_assets_1Q = total_assets
        ),
      simfin_balance_sheets_yearly %>%
        select(ticker, report_date,
          simfin_total_assets_1Y = total_assets
        ),
      factset_balance_sheets_yearly %>%
        select(ticker, report_date,
          factset_total_assets_1Y = total_assets
        ),
      edgar_balance_sheets_quarterly %>%
        select(ticker, report_date,
          edgar_total_assets_1Q = total_assets
        ),
      input_data_fundamentals %>%
        select(ticker, report_date,
          input_data_total_assets = total_assets
        ),
      graphfund_balance_sheets_quarterly %>%
        select(ticker, report_date,
          graphfund_total_assets = total_assets
        )
    ),
    full_join
  ) %>%
  transmute(ticker, report_date,
    total_assets = coalesce(
      edgar_total_assets_1Q,
      factset_total_assets_1Y,
      yhoo_total_assets_1Y,
      simfin_total_assets_1Y,
      yhoo_total_assets_1Q,
      simfin_total_assets_1Q,
      yhoo_total_assets_1Q,
      input_data_total_assets,
      graphfund_total_assets
    )
  ) %>%
  remove_duplicates()

# Consolidate total_current_assets
total_current_assets <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  yhoo_total_current_assets_1Q =
                                    total_current_assets),
                         yhoo_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_total_current_assets_1Y =
                                    total_current_assets),
                         simfin_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  simfin_total_current_assets_1Q =
                                    total_current_assets),
                         simfin_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  simfin_total_current_assets_1Y =
                                    total_current_assets),
                         factset_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  factset_total_current_assets_1Y =
                                    total_current_assets),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  edgar_total_current_assets_1Q =
                                    total_current_assets),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_total_current_assets =
                                    total_current_assets),
                         graphfund_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  graphfund_total_current_assets =
                                    total_current_assets)),
         full_join) %>%
  transmute(ticker, report_date, total_current_assets = 
              coalesce(edgar_total_current_assets_1Q,
                       factset_total_current_assets_1Y,
                       yhoo_total_current_assets_1Y, 
                       simfin_total_current_assets_1Y, 
                       yhoo_total_current_assets_1Q, 
                       simfin_total_current_assets_1Q, 
                       yhoo_total_current_assets_1Q,
                       input_data_total_current_assets,
                       graphfund_total_current_assets)) %>% 
  remove_duplicates()

# Consolidate total_current_liabilities
total_current_liabilities <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  yhoo_total_current_liabilities_1Q =
                                    total_current_liabilities),
                         yhoo_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  yhoo_total_current_liabilities_1Y =
                                    total_current_liabilities),
                         simfin_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  simfin_total_current_liabilities_1Q =
                                    total_current_liabilities),
                         simfin_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  simfin_total_current_liabilities_1Y =
                                    total_current_liabilities),
                         factset_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  factset_total_current_liabilities_1Y =
                                    total_current_liabilities),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  edgar_total_current_liabilities_1Q =
                                    total_current_liabilities),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_total_current_liabilities =
                                    total_current_liabilities),
                         graphfund_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  graphfund_total_current_liabilities =
                                    total_current_liabilities)),
         full_join) %>% 
  transmute(ticker, report_date, total_current_liabilities = 
              coalesce(edgar_total_current_liabilities_1Q,
                       factset_total_current_liabilities_1Y,
                       yhoo_total_current_liabilities_1Y,
                       simfin_total_current_liabilities_1Y,
                       yhoo_total_current_liabilities_1Q,
                       simfin_total_current_liabilities_1Q,
                       yhoo_total_current_liabilities_1Q,
                       input_data_total_current_liabilities,
                       graphfund_total_current_liabilities)) %>%
  remove_duplicates()




# graphfund_balance_sheets_quarterly %>% 
#   select(ticker, report_date, short_term_debt, current_portion_of_long_term_debt) %>% #drop_na(current_portion_of_long_term_debt) 
# filter(!is.na(short_term_debt) & !is.na(current_portion_of_long_term_debt))
# bind_rows()
  

# Consolidate short_long_term_debt
short_long_term_debt <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  yhoo_short_long_term_debt_1Q =
                                    short_long_term_debt),
                         yhoo_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_short_long_term_debt_1Y =
                                    short_long_term_debt),
                         simfin_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  simfin_short_long_term_debt_1Q = 
                                    short_term_debt),
                         simfin_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  simfin_short_long_term_debt_1Y = 
                                    short_term_debt),
                         factset_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  factset_short_long_term_debt_1Y =
                                    short_long_term_debt),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  edgar_short_long_term_debt_1Q =
                                    short_long_term_debt),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_short_long_term_debt =
                                    short_long_term_debt),
                         graphfund_balance_sheets_quarterly %>% 
                           select(ticker, report_date, short_term_debt,
                                  current_portion_of_long_term_debt) %>% 
                           mutate(graphfund_short_long_term_debt = 
                                    coalesce(current_portion_of_long_term_debt,
                                             short_term_debt))),
         full_join) %>%
  transmute(ticker, report_date, 
            short_long_term_debt = coalesce(edgar_short_long_term_debt_1Q,
                                            factset_short_long_term_debt_1Y,
                                            yhoo_short_long_term_debt_1Y,
                                            simfin_short_long_term_debt_1Y,
                                            yhoo_short_long_term_debt_1Q,
                                            simfin_short_long_term_debt_1Q,
                                            yhoo_short_long_term_debt_1Q,
                                            input_data_short_long_term_debt,
                                            graphfund_short_long_term_debt)) %>% 
  remove_duplicates()


# Consolidate long_term_debt 
long_term_debt <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  yhoo_long_term_debt_1Q = long_term_debt),
                         yhoo_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_long_term_debt_1Y = long_term_debt),
                         simfin_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  simfin_long_term_debt_1Q = long_term_debt),
                         simfin_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  simfin_long_term_debt_1Y = long_term_debt),
                         factset_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  factset_long_term_debt_1Y = long_term_debt),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  edgar_long_term_debt_1Q = long_term_debt),
                         input_data_fundamentals %>% 
                           select(ticker, report_date, 
                                  input_data_long_term_debt = long_term_debt),
                         graphfund_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  graphfund_long_term_debt =
                                    non_current_portion_of_long_term_debt)),
         full_join) %>%
  transmute(ticker, report_date, 
            long_term_debt = coalesce(edgar_long_term_debt_1Q,
                                      factset_long_term_debt_1Y,
                                      yhoo_long_term_debt_1Y,
                                      simfin_long_term_debt_1Y,
                                      yhoo_long_term_debt_1Q,
                                      simfin_long_term_debt_1Q,
                                      yhoo_long_term_debt_1Q,
                                      input_data_long_term_debt,
                                      graphfund_long_term_debt)) %>% 
  remove_duplicates()


# Consolidate property_plant_equipment 
property_plant_equipment <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  yhoo_property_plant_equipment_1Q =
                                    property_plant_equipment),
                         yhoo_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  yhoo_property_plant_equipment_1Y =
                                    property_plant_equipment),
                         simfin_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  simfin_property_plant_equipment_1Q =
                                    property_plant_equipment_net),
                         simfin_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  simfin_property_plant_equipment_1Y =
                                    property_plant_equipment_net),
                         factset_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  factset_property_plant_equipment_1Y =
                                    property_plant_equipment),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  edgar_property_plant_equipment_1Q =
                                    property_plant_equipment),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_property_plant_equipment =
                                    property_plant_equipment),
                         graphfund_balance_sheets_quarterly %>% 
                           transmute(ticker, report_date,
                                  graphfund_property_plant_equipment = 
                                    total_assets - goodwill -
                                    other_intangible_assets_excluding_goodwill -
                                    total_current_assets)),
         full_join) %>%
  transmute(ticker, report_date, property_plant_equipment = 
              coalesce(edgar_property_plant_equipment_1Q,
                       factset_property_plant_equipment_1Y,
                       yhoo_property_plant_equipment_1Y,
                       simfin_property_plant_equipment_1Y,
                       yhoo_property_plant_equipment_1Q,
                       simfin_property_plant_equipment_1Q,
                       yhoo_property_plant_equipment_1Q,
                       input_data_property_plant_equipment,
                       graphfund_property_plant_equipment)) %>% 
  remove_duplicates()


# Consolidate net_receivables
net_receivables <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  yhoo_net_receivables_1Q = net_receivables),
                         yhoo_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_net_receivables_1Y = net_receivables),
                         simfin_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  simfin_net_receivables_1Q =
                                    accounts_notes_receivable),
                         simfin_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  simfin_net_receivables_1Y =
                                    accounts_notes_receivable),
                         factset_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  factset_net_receivables_1Y = net_receivables),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  edgar_net_receivables_1Q = net_receivables),
                         input_data_fundamentals %>% 
                           select(ticker, report_date, 
                                  input_data_net_receivables = 
                                    net_receivables)),
         full_join) %>%
  transmute(ticker, report_date, 
            net_receivables = coalesce(edgar_net_receivables_1Q,
                                       factset_net_receivables_1Y, 
                                       yhoo_net_receivables_1Y,
                                       simfin_net_receivables_1Y, 
                                       yhoo_net_receivables_1Q,
                                       simfin_net_receivables_1Q,
                                       yhoo_net_receivables_1Q, 
                                       input_data_net_receivables)) %>% 
  remove_duplicates()


# Consolidate accounts_payable
accounts_payable <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  yhoo_accounts_payable_1Q = accounts_payable),
                         yhoo_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_accounts_payable_1Y = accounts_payable),
                         simfin_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  simfin_accounts_payable_1Q = payables_accruals),
                         simfin_balance_sheets_yearly %>% 
                           select(ticker, report_date, 
                                  simfin_accounts_payable_1Y = payables_accruals),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, report_date, 
                                  edgar_accounts_payable_1Q = accounts_payable),
                         input_data_fundamentals %>% 
                           select(ticker, report_date, 
                                  input_data_accounts_payable = 
                                    accounts_payable)),
         full_join) %>%
  transmute(ticker, report_date, 
            accounts_payable = coalesce(edgar_accounts_payable_1Q,
                                        yhoo_accounts_payable_1Y,
                                        simfin_accounts_payable_1Y,
                                        yhoo_accounts_payable_1Q,
                                        simfin_accounts_payable_1Q,
                                        yhoo_accounts_payable_1Q,
                                        input_data_accounts_payable)) %>% 
  remove_duplicates()


# Consolidate cash_and_short_term_investments 
cash_and_short_term_investments <-
  reduce(list(yhoo_balance_sheets_quarterly %>% 
                mutate(yhoo_cash_and_short_term_investments_1Q = cash +
                         short_term_investments) %>% 
                           select(ticker, report_date,
                                  yhoo_cash_and_short_term_investments_1Q),
                         yhoo_balance_sheets_yearly %>% 
                           mutate(yhoo_cash_and_short_term_investments_1Y = 
                                    cash + short_term_investments) %>% 
                           select(ticker, report_date,
                                  yhoo_cash_and_short_term_investments_1Y),
                         simfin_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  simfin_cash_and_short_term_investments_1Q =
                                    cash_cash_equivalents_short_term_investments),
                         simfin_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  simfin_cash_and_short_term_investments_1Y =
                                    cash_cash_equivalents_short_term_investments),
                         factset_balance_sheets_yearly %>% 
                           select(ticker, report_date,
                                  factset_cash_and_short_term_investments_1Y =
                                    cash_and_short_term_investments),
                         edgar_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  edgar_cash_and_short_term_investments_1Q =
                                    cash_and_short_term_investments),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_cash_and_short_term_investments =
                                    cash_and_short_term_investments),
                         graphfund_balance_sheets_quarterly %>% 
                           select(ticker, report_date,
                                  graphfund_cash_and_short_term_investments =
                                    cash_and_cash_equivalents)),
         full_join) %>%
  transmute(ticker, report_date, 
            cash_and_short_term_investments = 
              coalesce(edgar_cash_and_short_term_investments_1Q,
                       factset_cash_and_short_term_investments_1Y,
                       yhoo_cash_and_short_term_investments_1Y,
                       simfin_cash_and_short_term_investments_1Y,
                       yhoo_cash_and_short_term_investments_1Q,
                       simfin_cash_and_short_term_investments_1Q,
                       yhoo_cash_and_short_term_investments_1Q,
                       input_data_cash_and_short_term_investments,
                       graphfund_cash_and_short_term_investments)) %>% 
  remove_duplicates()



# Consolidate all fields
bs_consolidated <- 
  reduce(list(total_liabilities, total_assets, total_current_assets,
                         total_current_liabilities, short_long_term_debt,
                         long_term_debt, property_plant_equipment,
                         net_receivables, accounts_payable, 
                         cash_and_short_term_investments),
         full_join) %>% 
  select(ticker, rounded_date, everything()) %>%
  as.data.table() %>%
  setorder(ticker, rounded_date) %>% 
  as_tibble()




# Check for any duplicated dates for a given ticker
stopifnot(!bs_consolidated %>% select(ticker, rounded_date) %>% duplicated() %>% any())

# Save to this project's data directory
fwrite(bs_consolidated, paste0(dir_data, "data/cleaned data/bs_consolidated (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))
# Save to stock-analysis data directory
fwrite(bs_consolidated, paste0("C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/cleaned data/bs_consolidated (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))




    
    
#~~~~~~~~~~~~~~~~~~~#
# Income Statements 
#~~~~~~~~~~~~~~~~~~~#

# Consolidate revenue
revenue <-
  reduce(list(yhoo_income_statements_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_revenue_1Y = total_revenue),
                         yhoo_income_statements_quarterly %>%
                           select(ticker, report_date, 
                                  yhoo_revenue_1Q = total_revenue),
                         factset_income_statements_yearly %>%  
                           select(ticker, report_date, 
                                  factset_revenue_1Y = revenue),
                         edgar_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  edgar_revenue_1Q = revenue),
                         simfin_income_statements_yearly %>% 
                           select(ticker, report_date, 
                                  simfin_revenue_1Y = revenue),
                         simfin_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  simfin_revenue_1Q = revenue),
                         input_data_fundamentals %>%
                           select(ticker, report_date, 
                                  input_data_revenue_1Y = revenue_1Y,
                                  input_data_revenue_1Q = revenue_1Q),
                         graphfund_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  graphfund_revenue_1Q = total_revenue)),
         full_join) %>%
  transmute(ticker, report_date,
            revenue_1Y = coalesce(factset_revenue_1Y, 
                                  yhoo_revenue_1Y,
                                  simfin_revenue_1Y, 
                                  input_data_revenue_1Y),
            revenue_1Q = coalesce(edgar_revenue_1Q, 
                                  yhoo_revenue_1Q,
                                  simfin_revenue_1Q, 
                                  input_data_revenue_1Q,
                                  graphfund_revenue_1Q)) %>% 
  remove_duplicates()


# Consolidate gross_profit
gross_profit <-
  reduce(list(yhoo_income_statements_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_gross_profit_1Y = gross_profit),
                         yhoo_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  yhoo_gross_profit_1Q = gross_profit),
                         factset_income_statements_yearly %>% 
                           select(ticker, report_date, 
                                  factset_gross_profit_1Y = gross_profit),
                         edgar_income_statements_quarterly %>%
                           select(ticker, report_date, 
                                  edgar_gross_profit_1Q = gross_profit),
                         input_data_fundamentals %>% 
                           select(ticker, report_date, 
                                  input_data_gross_profit_1Y = gross_profit_1Y),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_gross_profit_1Q = gross_profit_1Q),
                         graphfund_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  graphfund_gross_profit_1Q = gross_profit)),
         full_join) %>%
  transmute(ticker, report_date, 
            gross_profit_1Y = coalesce(factset_gross_profit_1Y,
                                       yhoo_gross_profit_1Y,
                                       input_data_gross_profit_1Y),
            gross_profit_1Q = coalesce(edgar_gross_profit_1Q, 
                                       yhoo_gross_profit_1Q,
                                       input_data_gross_profit_1Q,
                                       graphfund_gross_profit_1Q)) %>% 
  remove_duplicates()


# Consolidate operating_income_loss
operating_income_loss <-
  reduce(list(yhoo_income_statements_yearly %>% 
                           select(ticker, report_date,
                                  yhoo_operating_income_loss_1Y = 
                                    operating_income),
                         yhoo_income_statements_quarterly %>% 
                           select(ticker, report_date,
                                  yhoo_operating_income_loss_1Q = 
                                    operating_income),
                         factset_income_statements_yearly %>% 
                           select(ticker, report_date,
                                  factset_operating_income_loss_1Y =
                                    operating_income_loss),
                         edgar_income_statements_quarterly %>% 
                           select(ticker, report_date,
                                  edgar_operating_income_loss_1Q =
                                    operating_income_loss),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_operating_income_loss_1Y =
                                    operating_income_loss_1Y),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_operating_income_loss_1Q =
                                    operating_income_loss_1Q),
                         graphfund_income_statements_quarterly %>%
                           select(ticker, report_date, 
                                  graphfund_operating_income_loss_1Q =
                                    operating_income)
                         ),
         full_join) %>%
  transmute(ticker, report_date, 
            operating_income_loss_1Y = 
              coalesce(factset_operating_income_loss_1Y,
                       yhoo_operating_income_loss_1Y,
                       input_data_operating_income_loss_1Y),
            operating_income_loss_1Q = 
              coalesce(edgar_operating_income_loss_1Q,
                       yhoo_operating_income_loss_1Q,
                       input_data_operating_income_loss_1Q,
                       graphfund_operating_income_loss_1Q)) %>% 
  remove_duplicates()

 
# Consolidate selling_general_administrative
selling_general_administrative <-
  reduce(list(yhoo_income_statements_yearly %>% 
                           select(ticker, report_date,
                                  yhoo_selling_general_administrative_1Y =
                                    selling_general_administrative),
                         yhoo_income_statements_quarterly %>% 
                           select(ticker, report_date,
                                  yhoo_selling_general_administrative_1Q =
                                    selling_general_administrative),
                         factset_income_statements_yearly %>% 
                           select(ticker, report_date,
                                  factset_selling_general_administrative_1Y =
                                    selling_general_administrative),
                         edgar_income_statements_quarterly %>% 
                           select(ticker, report_date,
                                  edgar_selling_general_administrative_1Q =
                                    selling_general_administrative),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_selling_general_administrative_1Y =
                                    selling_general_administrative_1Y),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_selling_general_administrative_1Q =
                                    selling_general_administrative_1Q),
                         graphfund_income_statements_quarterly %>%
                           transmute(ticker, report_date, 
                                  graphfund_selling_general_administrative_1Q =
                                    max(total_operating_expenses,
                                        total_operating_expenses- 
                                          cost_of_revenue))),
         full_join) %>%
  transmute(ticker, report_date, 
            selling_general_administrative_1Y = 
              coalesce(factset_selling_general_administrative_1Y,
                       yhoo_selling_general_administrative_1Y,
                       input_data_selling_general_administrative_1Y),
            selling_general_administrative_1Q = 
              coalesce(edgar_selling_general_administrative_1Q,
                       yhoo_selling_general_administrative_1Q,
                       input_data_selling_general_administrative_1Q,
                       graphfund_selling_general_administrative_1Q)) %>% 
  remove_duplicates()


# Consolidate net_income_common
net_income_common <-
  reduce(list(yhoo_income_statements_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_net_income_common_1Y =
                                    net_income_applicable_to_common_shares),
                         yhoo_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  yhoo_net_income_common_1Q =
                                    net_income_applicable_to_common_shares),
                         factset_income_statements_yearly %>% 
                           select(ticker, report_date,
                                  factset_net_income_common_1Y = net_inc_basic),
                         edgar_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  edgar_net_income_common_1Q = net_income_common),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_net_income_common_1Y =
                                    net_income_common_1Y),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_net_income_common_1Q =
                                    net_income_common_1Q)),
         full_join) %>%
  transmute(ticker, report_date, 
            net_income_common_1Y = 
              coalesce(factset_net_income_common_1Y, 
                       yhoo_net_income_common_1Y, 
                       input_data_net_income_common_1Y),
            net_income_common_1Q = 
              coalesce(edgar_net_income_common_1Q, 
                       yhoo_net_income_common_1Q, 
                       input_data_net_income_common_1Q)) %>% 
  remove_duplicates()


# Consolidate net_income
net_income <-
  reduce(list(yhoo_income_statements_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_net_income_1Y = net_income),
                         yhoo_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  yhoo_net_income_1Q = net_income),
                         factset_income_statements_yearly %>% 
                           select(ticker, report_date, 
                                  factset_net_income_1Y = net_inc_basic),
                         edgar_income_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  edgar_net_income_1Q = NetIncomeLoss),
                         input_data_fundamentals %>% 
                           select(ticker, report_date, 
                                  input_data_net_income_1Y = net_income_1Y,
                                  input_data_net_income_1Q = net_income_1Q),
                         graphfund_income_statements_quarterly %>%
                           transmute(ticker, report_date, 
                                     graphfund_net_income_1Q =
                                       net_income)),
         full_join) %>%
  transmute(ticker, report_date, 
            net_income_1Y = coalesce(factset_net_income_1Y, 
                                     yhoo_net_income_1Y, 
                                     input_data_net_income_1Y),
            net_income_1Q = coalesce(edgar_net_income_1Q, 
                                     yhoo_net_income_1Q,
                                     input_data_net_income_1Q,
                                     graphfund_net_income_1Q)) %>% 
  remove_duplicates()




# Consolidate all fields
is_consolidated <- 
  reduce(list(revenue, gross_profit, operating_income_loss,
                         selling_general_administrative, net_income_common,
                         net_income),
         full_join) %>% 
  select(ticker, rounded_date, everything()) %>% 
  as.data.table() %>% 
  setorder(ticker, rounded_date) %>% 
  as_tibble()



# Check for any duplicated dates for a given ticker
stopifnot(!is_consolidated %>% select(ticker, rounded_date) %>% duplicated() %>% any())

  
# Save to this project's data directory
fwrite(is_consolidated, paste0(dir_data, "data/cleaned data/is_consolidated (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))
# Save to stock-analysis data directory
fwrite(is_consolidated, paste0("C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/cleaned data/is_consolidated (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))






#~~~~~~~~~~~~~~~~~~~~~~#
# Cash Flow Statements 
#~~~~~~~~~~~~~~~~~~~~~~#


# Consolidate net_cash_from_operating_activities 
cash_from_operating_activities <-
  reduce(list(simfin_cash_flow_statements_yearly %>% 
                           select(ticker, report_date,
                                  simfin_cash_from_operating_activities_1Y =
                                    net_cash_from_operating_activities),
                         simfin_cash_flow_statements_quarterly %>% 
                           select(ticker, report_date,
                                  simfin_cash_from_operating_activities_1Q =
                                    net_cash_from_operating_activities),
                         factset_cash_flow_statements_yearly %>% 
                           select(ticker, report_date,
                                  factset_cash_from_operating_activities_1Y =
                                    total_cash_from_operating_activities),
                         yhoo_cash_flow_statements_yearly %>% 
                           select(ticker, report_date,
                                  yhoo_cash_from_operating_activities_1Y =
                                    total_cash_from_operating_activities),
                         edgar_cash_flow_statements_quarterly %>% 
                           select(ticker, report_date,
                                  edgar_cash_from_operating_activities_1Q =
                                    total_cash_from_operating_activities),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_cash_from_operating_activities_1Y =
                                    cash_from_operating_activities_1Y),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_cash_from_operating_activities_1Q =
                                    cash_from_operating_activities_1Q),
                         graphfund_cashflows_quarterly %>%
                           select(ticker, report_date,
                                  graphfund_cash_from_operating_activities_1Q =
                                    net_cash_provided_by_operating_activities)),
         full_join) %>%
  # mutate(rounded_date = round_date(report_date, unit = "month") - days(1)) %>%
  transmute(ticker, report_date,
            cash_from_operating_activities_1Q = 
              coalesce(edgar_cash_from_operating_activities_1Q,
                       simfin_cash_from_operating_activities_1Q,                                          input_data_cash_from_operating_activities_1Q,
                       graphfund_cash_from_operating_activities_1Q),
            cash_from_operating_activities_1Y = 
              coalesce(factset_cash_from_operating_activities_1Y,
                       yhoo_cash_from_operating_activities_1Y,
                       simfin_cash_from_operating_activities_1Y,
                       input_data_cash_from_operating_activities_1Y)
            ) %>% 
  remove_duplicates()




# Consolidate cash_from_investing_activities 
cash_from_investing_activities <-
  reduce(list(simfin_cash_flow_statements_yearly %>% 
                select(ticker, report_date,
                       simfin_cash_from_investing_activities_1Y =
                         net_cash_from_investing_activities),
                         simfin_cash_flow_statements_quarterly %>% 
                select(ticker, report_date,
                       simfin_cash_from_investing_activities_1Q =
                         net_cash_from_investing_activities),
                         yhoo_cash_flow_statements_yearly %>% 
                select(ticker, report_date, 
                       yhoo_cash_from_investing_activities_1Y =
                         total_cashflows_from_investing_activities),
                         edgar_cash_flow_statements_quarterly %>% 
                select(ticker, report_date,
                       edgar_cash_from_investing_activities_1Q =
                         total_cash_from_investing_activities),
                         input_data_fundamentals %>% 
                select(ticker, report_date,
                       input_data_cash_from_investing_activities_1Y = 
                         cash_from_investing_activities_1Y,
                       input_data_cash_from_investing_activities_1Q =
                         cash_from_investing_activities_1Q),
              graphfund_cashflows_quarterly %>%
                select(ticker, report_date,
                       graphfund_cash_from_investing_activities_1Q =
                         net_cash_provided_by_investing_activities)),
         full_join) %>%
  # mutate(rounded_date = round_date(report_date, unit = "month") - days(1)) %>%
  transmute(ticker, report_date,
            cash_from_investing_activities_1Q = 
              coalesce(edgar_cash_from_investing_activities_1Q,
                       simfin_cash_from_investing_activities_1Q,
                       input_data_cash_from_investing_activities_1Q,
                       graphfund_cash_from_investing_activities_1Q),
            cash_from_investing_activities_1Y = 
              coalesce(yhoo_cash_from_investing_activities_1Y,
                       simfin_cash_from_investing_activities_1Y,
                       input_data_cash_from_investing_activities_1Y)
            ) %>% 
  remove_duplicates()
  

# Consolidate cash_from_financing_activities 
cash_from_financing_activities <-
  reduce(list(simfin_cash_flow_statements_yearly %>% 
                select(ticker, report_date,
                       simfin_cash_from_financing_activities_1Y =
                         net_cash_from_financing_activities),
                         simfin_cash_flow_statements_quarterly %>% 
                select(ticker, report_date,
                       simfin_cash_from_financing_activities_1Q =
                         net_cash_from_financing_activities),
                         yhoo_cash_flow_statements_yearly %>% 
                select(ticker, report_date, 
                       yhoo_cash_from_financing_activities_1Y =
                         total_cash_from_financing_activities),
                         edgar_cash_flow_statements_quarterly %>% 
                select(ticker, report_date,
                       edgar_cash_from_financing_activities_1Q =
                         total_cash_from_financing_activities),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_cash_from_financing_activities_1Y =
                                    cash_from_financing_activities_1Y),
              input_data_fundamentals %>% 
                select(ticker, report_date,
                       input_data_cash_from_financing_activities_1Q =
                         cash_from_financing_activities_1Q),
              graphfund_cashflows_quarterly %>%
                select(ticker, report_date,
                       graphfund_cash_from_financing_activities_1Q =
                         net_cash_provided_by_financing_activities)),
         full_join) %>%
  transmute(ticker, report_date,
            cash_from_financing_activities_1Q = 
              coalesce(edgar_cash_from_financing_activities_1Q,
                       simfin_cash_from_financing_activities_1Q,
                       input_data_cash_from_financing_activities_1Q,
                       graphfund_cash_from_financing_activities_1Q),
            cash_from_financing_activities_1Y = 
              coalesce(yhoo_cash_from_financing_activities_1Y,
                       simfin_cash_from_financing_activities_1Y,
                       input_data_cash_from_financing_activities_1Y)
            ) %>% 
  remove_duplicates()

# Consolidate depreciation_amortization  
depreciation_amortization <- 
  reduce(list(simfin_cash_flow_statements_yearly %>% 
                           select(ticker, report_date,
                                  simfin_depreciation_amortization_1Y =
                                    depreciation_amortization),
                         simfin_cash_flow_statements_quarterly %>% 
                           select(ticker, report_date,
                                  simfin_depreciation_amortization_1Q =
                                    depreciation_amortization),
                         factset_cash_flow_statements_yearly %>% 
                           select(ticker, report_date,
                                  factset_depreciation_amortization_1Y =
                                    depreciation),
                         yhoo_cash_flow_statements_yearly %>% 
                           select(ticker, report_date,
                                  yhoo_depreciation_amortization_1Y = 
                                    depreciation),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_depreciation_amortization_1Y =
                                    depreciation_amortization_1Y),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_depreciation_amortization_1Q =
                                    depreciation_amortization_1Q)),
         full_join) %>% 
  # mutate(rounded_date = round_date(report_date, unit = "month") - days(1)) %>%
  transmute(ticker, report_date,
            depreciation_amortization_1Q = 
              coalesce(simfin_depreciation_amortization_1Q,
                       input_data_depreciation_amortization_1Q),
            depreciation_amortization_1Y = 
              coalesce(factset_depreciation_amortization_1Y,
                       yhoo_depreciation_amortization_1Y,
                       simfin_depreciation_amortization_1Y,
                       input_data_depreciation_amortization_1Y)) %>% 
  remove_duplicates()


# Consolidate cash_from_repurchase_of_equity  
cash_from_repurchase_of_equity <-
  reduce(list(simfin_cash_flow_statements_yearly %>% 
                           select(ticker, report_date,
                                  simfin_cash_from_repurchase_of_equity_1Y =
                                    cash_from_repurchase_of_equity),
                         simfin_cash_flow_statements_quarterly %>% 
                           select(ticker, report_date,
                                  simfin_cash_from_repurchase_of_equity_1Q =
                                    cash_from_repurchase_of_equity),
                         factset_cash_flow_statements_yearly %>% 
                           mutate(factset_cash_from_repurchase_of_equity_1Y =
                                    issuance_of_stock - repurchase_of_stock) %>%
                           select(ticker, report_date,
                                  factset_cash_from_repurchase_of_equity_1Y),
                         yhoo_cash_flow_statements_yearly %>% 
                           mutate(yhoo_cash_from_repurchase_of_equity_1Y =
                                    issuance_of_stock - repurchase_of_stock) %>%
                           select(ticker, report_date,
                                  yhoo_cash_from_repurchase_of_equity_1Y),
                         input_data_fundamentals %>% 
                           select(ticker, report_date,
                                  input_data_cash_from_repurchase_of_equity_1Y =
                                    cash_from_repurchase_of_equity_1Y,
                                  input_data_cash_from_repurchase_of_equity_1Q =
                                    cash_from_repurchase_of_equity_1Q)),
         full_join) %>% 
  transmute(ticker, report_date,
            cash_from_repurchase_of_equity_1Q = 
              coalesce(simfin_cash_from_repurchase_of_equity_1Q,
                       input_data_cash_from_repurchase_of_equity_1Q),
            cash_from_repurchase_of_equity_1Y = 
              coalesce(factset_cash_from_repurchase_of_equity_1Y,
                       yhoo_cash_from_repurchase_of_equity_1Y,
                       simfin_cash_from_repurchase_of_equity_1Y,
                       input_data_cash_from_repurchase_of_equity_1Y)) %>% 
  remove_duplicates()



# Consolidate net_income from cash flow data
net_income <-
  reduce(list(simfin_cash_flow_statements_yearly %>% 
                           select(ticker, report_date, 
                                  simfin_net_income_1Y = net_income_starting_line),
                         simfin_cash_flow_statements_quarterly %>% 
                           select(ticker, report_date, 
                                  simfin_net_income_1Q = net_income_starting_line),
                         yhoo_cash_flow_statements_yearly %>% 
                           select(ticker, report_date, 
                                  yhoo_net_income_1Y = net_income),
                         input_data_fundamentals %>% 
                           select(ticker, report_date, 
                                  input_data_net_income_1Y = net_income_1Y),
                         input_data_fundamentals %>% 
                           select(ticker, report_date, 
                                  input_data_net_income_1Q = net_income_1Q),
                         graphfund_cashflows_quarterly %>%
                           select(ticker, report_date,
                                  graphfund_net_income_1Q = net_income)),
         full_join) %>%
  transmute(ticker, report_date,
            net_income_1Q = coalesce(simfin_net_income_1Q, 
                                     input_data_net_income_1Q,
                                     graphfund_net_income_1Q),
            net_income_1Y = coalesce(yhoo_net_income_1Y, 
                                     simfin_net_income_1Y, 
                                     input_data_net_income_1Y)) %>% 
  remove_duplicates()


# Consolidate all fields
cf_consolidated <- 
  reduce(list(cash_from_operating_activities,
                         cash_from_investing_activities,
                         cash_from_financing_activities, 
                         depreciation_amortization,
                         cash_from_repurchase_of_equity,
                         net_income),
         full_join) %>% 
  select(ticker, rounded_date, everything()) %>% 
  as.data.table() %>% 
  setorder(ticker, rounded_date) %>% 
  as_tibble()

  



# Check for any duplicated dates for a given ticker
stopifnot(!cf_consolidated %>% select(ticker, rounded_date) %>% duplicated() %>% any())

# Save to this project's data directory
fwrite(cf_consolidated, paste0(dir_data, "data/cleaned data/cf_consolidated (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))
# Save to stock-analysis data directory
fwrite(cf_consolidated, paste0("C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/cleaned data/cf_consolidated (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))



# Clear namespace
# rm(list = ls()[!ls() %in% grep("start|end|dir_data", ls(), value = TRUE)])



# library(plotly)
# plot <- yhoo_income_statements_quarterly %>% 
#     full_join(simfin_income_statements_quarterly) %>% 
#     # add_count(ticker) %>%
#     # select(ticker, n) %>% 
#     # filter(n >= 4*10) %>% 
#     # summarize(ticker, max_count = max(n))
#     filter(ticker == "AAPL") %>% 
#     arrange(report_date) %>%
#     # View()
#     ggplot(aes(report_date, revenue)) +
#     geom_point(color = "firebrick2", size = 2) +
#     geom_line(color = "firebrick2") +
#     # scale_y_continuous(label = scales::dollar_format(scale = 1e-6)) +
#     scale_x_date(labels = scales::date_format("%Y/%m"),
#                  breaks = scales::date_breaks("3 months")) +
#     # geom_label(aes(label = scales::dollar_format(scale = 1e-6)(revenue)), color = "midnightblue",
#     #            nudge_y = 1.3, nudge_x = -1.3) +
#     labs(title = "Cash", subtitle = "(in millions)", x = "", y = "") +
#     theme_minimal() +
#     theme(axis.text.y = element_blank(),
#           panel.grid.minor.x = element_blank(),
#           panel.grid.major.y = element_blank(),
#           plot.background = element_rect(fill = "#BFD5E3"))
# 
# ggplotly(plot)






#~~~~~~~~~~~~~~~~~~~~~~#
# Sector/Industry data
#~~~~~~~~~~~~~~~~~~~~~~#

source("helper functions.R")

# Profiles (ratios + industry, company names, etc.)

file_edgar_profiles <- list.files(paste0(dir_data, "data/cleaned data"), 
                                  pattern = "edgar_profiles", full.names = TRUE)
edgar_profile_data <- map_df(file_edgar_profiles, ~read_tibble(.x)) %>% 
  as.data.table() %>% unique() %>% as_tibble()


# Yahoo profile data
file_yhoo_profiles <- 
  list.files("C:/Users/user/Desktop/Aaron/R/Projects/Yahoo-Data/data/cleaned",
             pattern = "yhoo_profiles", full.names = TRUE) %>% max()

yhoo_profile_data <- 
  read_tibble(file_yhoo_profiles) %>% 
  as.data.table() %>% unique() %>% as_tibble() %>%
  rename(report_date = download_date) %>% 
  get_rounded_date() %>% 
  rename(report_date = rounded_date,
         shares_basic = shares_outstanding) %>% 
  # Rename some fields (to match industry names in YHOO data)
  mutate(industry = industry %>% 
               str_replace("^coking_coal$", 
                           "coal") %>% 
               str_replace("^consulting_services$", 
                           "consulting") %>%
               str_replace("^drug_manufacturers_general$", 
                           "drug_manufacturers") %>% 
               str_replace("^farm_heavy_construction_machinery$",
                           "farm_construction_machinery") %>% 
               str_replace("^insurance_diversified$", 
                           "insurance") %>% 
               str_replace("^advertising_agencies$",
                           "advertising_marketing_services") %>% 
               str_replace("^software_application$", 
                           "application_software") %>% 
               str_replace("^beverages_wineries_distilleries$",
                           "beverages_alcoholic") %>% 
               str_replace("^apparel_retail$", 
                           "retail_apparel_specialty") %>% 
               str_replace("^specialty_business_services$", 
                           "business_services") %>% 
               str_replace("^consulting_services$",
                           "consulting_services_outsourcing") %>% 
               str_replace("^medical_instruments_supplies$",
                           "medical_instruments_equipment") %>% 
               str_replace("^insurance_life$", 
                           "insurance_diversified_life") %>% 
               str_replace("^insurance_property_casualty$",
                           "insurance_diversified_property_casualty") %>% 
               str_replace("^insurance_specialty$",
                           "insurance_diversified_specialty") %>% 
               str_replace("^oil_gas_equipment_services$", 
                           "oil_gas_services") %>% 
               str_replace("^building_products_equipment$", 
                           "industrial_products") %>% 
               str_replace("^airports_air_services$", 
                           "transportation_logistics") %>% 
               str_replace("^banks_regional$", 
                           "banks") %>% 
               str_replace("^software_infrastructure$", 
                           "software_application") %>% 
               str_replace("^semiconductor_equipment_materials$", 
                           "semiconductors") %>% 
               str_replace("^scientific_technical_instruments$",
                           "computer_hardware") %>% 
               str_replace("^leisure$", 
                           "travel_leisure") %>% 
               str_replace("^software_infrastructure$", 
                           "online_media"))
           


dir_sf <- "C:/Users/user/Desktop/Aaron/R/Projects/simfinR/data/cleaned data"

# Sector/industry data
simfin_profile_data_file <- list.files(dir_sf, pattern = "simfin_sector_industry_data", full.names = TRUE) %>% max()
simfin_profile_data <- 
  read_tibble(simfin_profile_data_file) %>% 
    mutate(across(c(sector, industry), ~snakecase::to_snake_case(.x)))

# Save to this project's data directory
fwrite(simfin_profile_data, paste0(dir_data, "data/cleaned data/simfin_profile_data (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))
# Save to stock-analysis data directory
fwrite(simfin_profile_data, paste0("C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/cleaned data/simfin_profile_data (", Sys.Date() %>% str_replace_all("-", " "), ").csv"))


# Join profile data sources
profile_data_consolidated <-
  bind_rows(
    edgar_profile_data %>%
      rename(
        industry_edgar = industry,
        name_edgar = name,
        industry_detail_edgar = industry_detail
      ),
    yhoo_profile_data %>%
      rename(industry_yhoo = industry, sector_yhoo = sector),
    simfin_profile_data %>%
      rename(
        industry_simfin = industry,
        sector_simfin = sector,
        name_simfin = company
      ),
    input_data_profiles %>%
      rename(input_data_industry_yhoo = industry_yhoo,
             input_data_sector_yhoo = sector_yhoo,
             input_data_shares_basic_seeking_alpha = 
               shares_basic_seeking_alpha)
  ) %>%
  mutate(industry_yhoo = coalesce(industry_yhoo, 
                                  input_data_industry_yhoo),
         sector_yhoo = coalesce(sector_yhoo, 
                                  input_data_sector_yhoo)) %>% 
  select(-input_data_industry_yhoo, -input_data_sector_yhoo) %>% 
  # select(ticker, report_date, 
  #        contains("industry") | contains("sector") | contains("name") |
  #          contains("shares")) %>% 
  group_by(ticker) %>%
  fill(-ticker, .direction = "downup") %>%
  ungroup() %>%
  filter(!duplicated(ticker)) %>%
  get_rounded_date() %>%
  rename(report_date = rounded_date) %>%
  mutate(ticker = str_to_upper(ticker)) %>%
  # Some tickers had duplicate rows with the same ticker and dates, but
  # the 'name_edgar' values were different
  mutate(across(where(is.character), ~ str_remove_all(.x, " /NEW"))) %>%
  mutate(across(where(is.character), ~ na_if(.x, ""))) %>%
  group_by(ticker) %>%
  fill(everything(), .direction = "downup") %>%
  # filter(!name_edgar %in% "ATEL CAPITAL EQUIPMENT FUND IX LLC") %>%
  as.data.table() %>%
  unique() %>%
  as_tibble() %>%
  mutate(across(
    contains("industry") | contains("sector"),
    snakecase::to_snake_case)
  ) %>%
  mutate(industry_edgar = 
           str_replace(industry_edgar, "uncategorized", "unknown")) %>%
  mutate(industry_simfin = 
           str_replace(industry_simfin, "other", "unknown")) %>% 
  mutate(across(starts_with("industry_") | starts_with("sector_"),
    ~replace_na(.x, "unknown"))
    ) %>% 
  group_by(ticker) %>% 
  fill(everything(), .direction = "up") %>% 
  slice(1) %>% 
  ungroup()


if(profile_data_consolidated %>% count(ticker) %>% filter(n > 1) %>% nrow() > 0)
  stop("Some duplicate tickers!")
  
# save to this project's data directory
fwrite(profile_data_consolidated, 
       paste0(dir_data, "data/cleaned data/profile_data_consolidated (", 
              Sys.Date() %>% str_replace_all("-", " "), ").csv"))






#-------#
# Ratios
# Create ratios tibble and save it here and to stock-analysis data directory
#-------#

# Load libraries and helper functions
source("helper functions.R")


# Read data
profile_data_consolidated_file <- 
  list.files(paste0(dir_data, "data/cleaned data"), 
             pattern = "^profile_data_consolidated", full.names = TRUE) %>% max()
profile_data_consolidated <- read_tibble(profile_data_consolidated_file) 

bs_files <- list.files(paste0(dir_data, "data/cleaned data"), 
                       pattern = "bs_consolidated", full.names = TRUE) %>% max()
bs_consolidated <- map_df(bs_files, ~read_tibble(.x))

is_files <- list.files(paste0(dir_data, "data/cleaned data"), 
                       pattern = "is_consolidated", full.names = TRUE) %>% max()
is_consolidated <- map_df(is_files, ~read_tibble(.x))

cf_files <- list.files(paste0(dir_data, "data/cleaned data"), 
                       pattern = "cf_consolidated", full.names = TRUE) %>% max()
cf_consolidated <- map_df(cf_files, ~read_tibble(.x))



# profile_data_consolidated %>% filter(ticker == "SOFI") %>%
#   janitor::remove_empty("cols") %>% View()



# Shares
# ls_tibble_objects <- ls()[unlist(map(ls(), ~is_tibble(get(.x))))]

# objects_that_contain_shares <-  
#   ls_tibble_objects %>% 
#   map(~get(.x)) %>% 
#   map(~str_detect(colnames(.x), "shares") %>% any()) %>% 
#   unlist() %>% 
#   ls_tibble_objects[.]
# objects_that_contain_shares



shares_basic <-
  reduce(list(edgar_income_statements_quarterly %>%
                select(ticker, report_date = rounded_date, 
                       edgar_is_q_shares_basic = shares_basic),
              input_data_fundamentals %>%
                transmute(ticker, report_date, 
                          input_data_yhoo_shares_basic = 
                            yhoo_shares_basic),
              # profile_data_consolidated contains:
              # - edgar_profile_data
              # - yhoo_profile_data
              # - simfin_profile_data
              # - input_data_profiles
              profile_data_consolidated %>%
                filter(!is.na(report_date)) %>% 
                select(ticker, report_date, 
                       profile_data_input_data_shares_basic_seeking_alpha =
                         input_data_shares_basic_seeking_alpha,
                       profile_data_shares_basic = shares_basic),
              simfin_balance_sheets_quarterly %>% 
                select(ticker, report_date, 
                       simfin_shares_basic_bs_1Q = shares_basic),
              simfin_balance_sheets_yearly %>%
                select(ticker, report_date, 
                       simfin_shares_basic_bs_1Y = shares_basic),
              simfin_cash_flow_statements_quarterly %>% 
                select(ticker, report_date, 
                       simfin_shares_basic_cf_1Q = shares_basic),
              simfin_cash_flow_statements_yearly %>% 
                select(ticker, report_date, 
                       simfin_shares_basic_cf_1Y = shares_basic),
              simfin_income_statements_quarterly %>% 
                select(ticker, report_date, 
                       simfin_shares_basic_is_1Q = shares_basic),
              simfin_income_statements_yearly %>% 
                select(ticker, report_date, 
                       simfin_shares_basic_is_1Y = shares_basic),
              simfin_shares_basic %>% 
                select(ticker, report_date = date, 
                       simfin_shares_basic = shares_basic)),
         full_join) %>% 
  transmute(ticker, report_date,
            shares_basic = coalesce(
              edgar_is_q_shares_basic,
              simfin_shares_basic_bs_1Q,
              simfin_shares_basic_bs_1Y,
              simfin_shares_basic_cf_1Q,
              simfin_shares_basic_cf_1Y,
              simfin_shares_basic_is_1Q,
              simfin_shares_basic_is_1Y,
              simfin_shares_basic,
              input_data_yhoo_shares_basic,
              profile_data_shares_basic,
              profile_data_input_data_shares_basic_seeking_alpha)) %>% 
  as.data.table() %>% 
  drop_na() %>% 
  unique() %>%
  get_rounded_date() %>%
  group_by(ticker, rounded_date) %>% 
  slice(1) %>% 
  ungroup() %>% 
  setorder(ticker, rounded_date) %>% 
  as_tibble()



# Consolidate net_income fields from is and cf
net_income <-
  full_join(is_consolidated %>% 
              select(ticker, rounded_date, 
                     is_net_income_1Q = net_income_1Q, 
                     is_net_income_1Y = net_income_1Y),
            cf_consolidated %>% 
              select(ticker, rounded_date, 
                     cf_net_income_1Q = net_income_1Q, 
                     cf_net_income_1Y = net_income_1Y)) %>% 
  mutate(net_income_1Q = coalesce(is_net_income_1Q, cf_net_income_1Q),
         net_income_1Y = coalesce(is_net_income_1Y, cf_net_income_1Y)) %>% 
  select(-is_net_income_1Q, -cf_net_income_1Q, 
         -is_net_income_1Y, -cf_net_income_1Y)



fundamentals_consolidated <- 
  reduce(list(
    is_consolidated %>% select(-net_income_1Q, -net_income_1Y),
    cf_consolidated %>% select(-net_income_1Q, -net_income_1Y),
    bs_consolidated,
    shares_basic,
    net_income,
    profile_data_consolidated %>% 
      select(!contains("shares_basic")) %>% 
      select(ticker, rounded_date = report_date, where(is.numeric)) %>% 
      drop_na(rounded_date)),
    full_join) %>% 
  rename(report_date = rounded_date) %>% 
  filter(!is.na(ticker)) %>% 
  as.data.table() %>% 
  unique() %>% 
  setorder(ticker, report_date) %>%
  # Remove rows that have all NAs
  filter(rowSums(is.na(select(., -c(ticker, report_date)))) != 
           ncol(select(., -c(ticker, report_date)))) %>%
  setorder(ticker, -report_date) %>% 
  # add_column(n_vals = rowSums(!is.na(select(., -c(ticker, report_date))))) %>%
  select(ticker, report_date, everything()) %>% 
  # !!!!!!
  # setorder(report_date, -n_vals) %>%
  group_by(ticker, report_date) %>%
  fill(everything(), .direction = "up") %>%
  slice(1) %>% 
  ungroup() %>% 
  # !!!!!!!!!!!
  # Remove duplicate instances of a date for a given ticker
  # filter(!duplicated(select(., ticker, report_date))) %>% 
  # select(-n_vals) %>% 
  setorder(ticker, report_date) %>% 
  as_tibble() %>% 
  filter(ticker != "") 


# fundamentals_consolidated %>% filter(ticker == "SOFI") %>% 
#   janitor::remove_empty("cols") %>% View()


# If there are any duplicate dates for any ticker, stop
if(fundamentals_consolidated %>% select(ticker, report_date) %>% duplicated() %>% which() %>% any()) stop()


# Save
fwrite(fundamentals_consolidated, 
       paste0(dir_data, "data/cleaned data/fundamentals_consolidated.csv"))


# Remove dates and numeric cols that were already merged with fundamentals above
profile_data_final <-
  profile_data_consolidated %>% 
  select(where(is.character))
  
fwrite(profile_data_final, 
       paste0(dir_data, "data/cleaned data/profile_data_final (", 
              Sys.Date() %>% str_replace_all("-", " "), ").csv"))


# Get cleaned weekly and monthly prices
start_cp <- Sys.time()
source("Clean prices.R")
end_cp <- Sys.time(); end_cp - start_cp

#-----------------------------------------#
# Part 10 - Merge Prices and Fundamentals
#-----------------------------------------#
fundamentals_consolidated <- 
  read_tibble("data/cleaned data/fundamentals_consolidated.csv")

# Filter out tickers that have no price data
fundamentals_with_prices <-
  fundamentals_consolidated %>% 
  filter(ticker %in% tickers_from_prices)

# Fill date range
fundamentals_full_dates <- 
  fundamentals_with_prices %>% 
  group_by(ticker) %>%
  complete(report_date = 
               seq.Date(first(report_date), last(report_date), by = "months") %>%
               round_date(unit = "month") - days(1)) %>%
  as.data.table() %>% 
  setorder(ticker, report_date) %>% 
  as_tibble()


combined_fundamentals_merged <-
  fundamentals_full_dates %>%
  left_join(prices_monthly) %>%
  left_join(prices_SP500TR_monthly)

# Save
fwrite(combined_fundamentals_merged, 
       paste0(dir_data, "data/cleaned data/combined_fundamentals_merged.csv"))


# Load libraries and helper functions
source("helper functions.R")

# Find percent of missing values
# combined_fundamentals_merged %>%
#   select(ticker, report_date, where(is.numeric)) %>%
#   pivot_longer(-c(ticker, report_date)) %>%
#   mutate(exists = !is.na(value)) %>%
#   group_by(ticker) %>%
#   summarize(pct_na = sum(is.na(value))/n()) %>%
#   arrange(pct_na) %>% 
#   slice_max(order_by = pct_na, n = 10)


#----------#
# Ratios   
#----------#

# Load libraries and helper functions
source("helper functions.R")

# Read
combined_fundamentals_merged <- 
  read_tibble(paste0(dir_data, 
                     "data/cleaned data/combined_fundamentals_merged.csv"))

# date_spans <-
#   combined_fundamentals_merged %>% 
#   group_by(ticker) %>% 
#   summarize(across(report_date, list(start = min, end = max), 
#                    .names = "{.fn}")) %>% 
#   mutate(years = floor(as.numeric(end - start)/365))

# date_spans %>%
#   filter(years <= 25) %>% 
#   ggplot() +
#   geom_histogram(aes(x=years), fill = "maroon", color = "white", 
#                  alpha = 0.7, binwidth = 1) +
#   theme_minimal() +
#   labs(title = "Range of years for the tickers", y = NULL)

# tickers_to_use <- 
#   date_spans %>% 
#   filter(years >= 10) %>% pull(ticker)



# Ratios 
###############
# For testing
# x <- combined_fundamentals_merged %>% filter(str_starts(ticker, "A"))
###############

# Not Used
replace_na_values <- function(x) {
  x %>%   
  mutate(
      short_long_term_debt = replace_na(short_long_term_debt, 0),
      long_term_debt = replace_na(long_term_debt, 0),
      depreciation_amortization_1Q = replace_na(depreciation_amortization_1Q, 0),
      depreciation_amortization_1Y = replace_na(depreciation_amortization_1Y, 0),
      property_plant_equipment = replace_na(property_plant_equipment, 0),
      cash_and_short_term_investments =
        replace_na(cash_and_short_term_investments, 0)
      )
}


## Slow! ~ 6 mins
start9 <- Sys.time()
ratios <- 
  combined_fundamentals_merged %>%
  # filter(ticker %in% sample(ticker, 5)) %>%
  append_derived_values() %>% 
  append_price_stats() %>% 
  append_filled_values() %>% 
  append_cleaned_values() %>% 
  append_profitability_ratios() %>% 
  append_liquidity_solvency_ratios() %>% 
  append_manipulation_metrics() %>% 
  append_activity_ratios()
end9 <- Sys.time(); end9 - start9  

the_end <- Sys.time(); the_end - the_start # ~ 15 mins


# Save
fwrite(ratios, paste0("data/cleaned data/", "ratios ", 
                      str_replace_all(Sys.Date(), "-", "_"), ".csv"))



source("helper functions.R")
ratios_file <- list.files("data/cleaned data", pattern = "ratios ",
                          full.names = TRUE) %>% max()
ratios <- read_tibble(ratios_file)



# WILL5000INDFC	Wilshire 5000 Total Market;Full Cap Index; Index; Daily; Not seasonally adjusted
# prices_from_FRED <- 
#   list.files("C:/Users/user/Desktop/Aaron/R/Projects/Fundamentals-Data/data",
#              pattern = "Prices from FRED", full.names = TRUE) %>% max()
# index_WILL5000 <- read_tibble(prices_from_FRED) %>% 
#   filter(symbol == "WILL5000INDFC")

# !!! Get total market cap data (Couldn't find online)

# Append total market caps
ratios_w_market_caps <-
  ratios %>%
  group_by(report_date) %>%
  mutate(total_market_cap = sum(market_cap,
                                na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(total_market_cap = na_if(total_market_cap, 0)) %>% 
  distinct()

SP500TR <- read_tibble("data/SP500TR.csv")

SP500TR_return <- 
  SP500TR %>% 
  fill(close, adjusted, .direction = "down") %>% 
  slice(endpoints(date, on = "months")) %>% 
  mutate(SP500TR_return_1M = adjusted / lag(adjusted, 1) - 1) %>% 
  select(report_date = date, SP500TR_return_1M) %>% 
  drop_na() %>% 
  mutate(report_date = round_date(report_date, unit = "month") - days(1))

ratios_w_market_data <-
  ratios_w_market_caps %>% 
  left_join(SP500TR_return)

profile_data_final_file <- 
  list.files(paste0(dir_data, "data/cleaned data"), 
             pattern = "^profile_data_final", full.names = TRUE) %>% max()
profile_data <- read_tibble(profile_data_final_file)

ratios_w_profile_data <-
  ratios_w_market_data %>% 
  left_join(profile_data) %>%
  group_by(ticker) %>% 
  fill(starts_with("industry_") | starts_with("sector_"),
       .direction = "downup") %>%
  mutate(across(starts_with("industry_") | starts_with("sector_"),
                       ~replace_na(.x, "unknown"))) %>% 
  ungroup()


# If any ticker has multiple sector_yhoo values, stop
if(nrow(ratios_w_profile_data %>%
        distinct(ticker, sector_yhoo) %>% 
        filter(sector_yhoo != "") %>% 
        count(ticker) %>% 
        filter(n > 1)) > 0) stop()


# Replace strange Japanese characters and excessive underscores in 
# sector and industry variables
# ratios_cleaned_categories <- 
#   ratios_w_profile_data %>%
#   mutate(across(where(is.character), 
#                 # Remove strange characters from strings
#                 ~ str_replace_all(.x, "[^\u0001-\u007F]", "")))



# For sectors or industries:
#  If a ticker has multiple categories, then choose the value that is the most common value among all tickers and use it as the category for the ticker

consolidate_categories <- function(data, field = "sector_yhoo") {
  
  #####
  # data <- ratios_cleaned_categories
  # field <- "sector_yhoo"
  ####
  
  if(!str_detect(field, "sector|industry")) 
    stop("Field must contain 'sector' or 'industry' in name.")
  
  
  all_categories <- 
    unique(data[, c("ticker", field), drop = FALSE])
  
  tickers_with_mult_categories <- 
    all_categories[all_categories[field] != "", , drop = FALSE] %>% 
    mutate(across(everything(), ~str_replace_na(.x, ""))) %>% 
    count(ticker) %>% 
    filter(ticker != "") %>% 
    filter(n > 1) %>% 
    pull(ticker)
  
  for(tick in tickers_with_mult_categories) {
    
    ####
    # tick <- "ASNB"
    ####
    
    categories <- 
      data[data$ticker == tick, field, drop = FALSE] %>% 
      unique() %>% 
      pull()
    
    
    category_counts <-
      table(all_categories[[field]], useNA = "always") %>% 
      as.data.frame() %>% 
      set_names(field, "n")
    
    
    category_to_use <-
      category_counts[match(categories, category_counts[[field]]), ] %>% 
      slice_max(n) %>% 
      pull(field) %>% 
      as.character()
    
    data[data$ticker == tick, field] <- category_to_use
    
  }
  
  return(data)
  
}

# ratios_cleaned_categories %>% select(contains("sector")) %>% colnames()
# ratios_cleaned_categories %>% select(contains("industry")) %>% colnames()

ratios_final <-
  ratios_w_profile_data %>% 
  consolidate_categories(field = "sector_yhoo") %>% 
  consolidate_categories(field = "sector_simfin") %>% 
  consolidate_categories(field = "industry_edgar") %>% 
  consolidate_categories(field = "industry_detail_edgar") %>% 
  consolidate_categories(field = "industry_yhoo") %>% 
  consolidate_categories(field = "industry_simfin")


# ratios_cleaned_categories  %>% 
#   select(ticker, report_date, name_edgar) %>%
#   mutate(across(where(is.character), ~str_remove_all(.x, " /NEW"))) %>% 
#   mutate(across(where(is.character), ~na_if(.x, ""))) %>% 
#   fill(name_edgar, .direction = "downup") %>% 
#   as.data.table() %>% unique() %>% as_tibble()
ratios_final %>% colnames() %>% sort()


# Save (SLOW !!)
ratios_final %>%
  mutate(file_name = industry_yhoo) %>%
  group_by(file_name) %>% 
  group_walk(~ write_csv(.x, paste0("data/cleaned data/by industry/ratios - ", 
                                   .y$file_name, ".csv")))

# Save to stock-analysis data directory
# - Remove old files
file.remove(list.files("C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/cleaned data/by industry", pattern = "ratios - ", full.names = TRUE))
# Copy new files
file.copy(list.files("data/cleaned data/by industry", pattern = "ratios - ", full.names = TRUE), "C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/cleaned data/by industry", overwrite = TRUE)


profiles_final <- 
  ratios_final %>% 
  select(ticker, contains("name"), contains("sector"), contains("industry"),
         contains("summary")) %>% 
  distinct()

# Save to "stock-analysis" project directory
fwrite(profiles_final, 
       "C:/Users/user/Desktop/Aaron/R/Shiny apps/stock-analysis/data/cleaned data/profiles_final.csv")







# source("helper functions.R")
# 
# ratios_final_files <-
#   list.files(paste0(dir_data, "data/cleaned data/by industry"), 
#              pattern = "ratios - ", full.names = TRUE)
# ratios_final <- ratios_final_files %>% 
#   map_df(read_tibble)



# plot <-
#       ratios_final_filled %>% 
#       summarize(across(everything(), ~sum(is.na(.x)) / n())) %>% 
#       pivot_longer(-ticker) %>% 
#       select(-ticker) %>% 
#       arrange(value) %>% 
#       mutate(name = str_trunc(name, 35)) %>% 
#       mutate(name = snakecase::to_title_case(name)) %>% 
#       ggplot(aes(value, fct_reorder(name, value), color = value,
#                  text = paste(
#                        name, "\n",
#                        scales::percent_format(accuracy = 1)(value),
#                        sep = ""
#                  ))) +
#       # geom_vline(aes(xintercept = value))
#       geom_point(size = 2, show.legend = FALSE) +
#       labs(y = "", x = "% Missing") +
#       scale_x_continuous(breaks = seq(0, 1.0, by = 0.1), labels = scales::percent_format(accuracy = 1)) +
#       scale_color_gradient(high = "red", low = "midnightblue") +
#       theme_light() +
#       theme(panel.grid.major.y = element_blank())
      
# plotly::ggplotly(plot, tooltip = "text")


# skimr::skim(ratios_final)

# sector_industry_fields <- 
#   ratios_final %>% 
#   select(contains("sector") | contains("industry")) %>% 
#   colnames()

# name_sector_industry_fields <- 
#   ratios_final %>%
#   colnames() %>% 
#   str_subset("^sector_|^industry_|^name_")


# ratios_final_filled <-
#   ratios_final %>% 
#   # filter(ticker == "BCC") %>% 
#   group_by(ticker) %>% 
#   arrange(ticker, report_date) %>%
#   mutate(across(everything(), ~fill_na_two_periods_max(.x))) %>% 
#   ungroup()

# Save
# fwrite(ratios_final_filled, paste0("data/cleaned data/", "ratios_final_filled ", 
#                       str_replace_all(Sys.Date(), "-", "_"), ".csv"))



# source("helper functions.R")
# ratios_final_filled_file <- list.files("data/cleaned data", 
#                                        pattern = "ratios_final_filled",
#                           full.names = TRUE) %>% max()
# ratios_final_filled <- read_tibble(ratios_final_filled_file)




# req_fields <-
#   c("ticker", "report_date", 
#     "revenue_1Y", "operating_income_loss_1Y",
#     "total_assets", "total_liabilities",
#     "cash_and_short_term_investments")

# ticker_nas <-
#   ratios_final %>%
#   select(!all_of(name_sector_industry_fields)) %>% 
#   group_by(ticker) %>% 
#   arrange(ticker, report_date) %>% 
#   # Remove trailing and leading NAs in the adjusted column 
#   #  (entirely removes some tickers!)
#   trim_nas(., adjusted) %>% 
#   ungroup() %>% 
#   group_by(ticker) %>% 
#   summarize(across(everything(), ~sum(is.na(.x)) / n())) %>% 
#   ungroup() 

# tickers_to_use <-
#   ticker_nas %>% 
#   select(all_of(req_fields)) %>% 
#   select(-ticker, -report_date) %>% 
#   {rowMeans(.) < 0.7} %>% 
#   which() %>% 
#   pull(ticker_nas, ticker)[.]


# fields_to_use <-
#   ratios_final %>%
#   filter(ticker %in% tickers_to_use) %>% 
#   select(!all_of(name_sector_industry_fields)) %>% 
#   group_by(ticker) %>% 
#   arrange(ticker, report_date) %>% 
#   # Remove trailing and leading NAs in the adjusted column 
#   #  (entirely removes some tickers!)
#   trim_nas(., adjusted) %>% 
#   ungroup() %>% 
#   summarize(across(everything(), ~sum(is.na(.x)) / n())) %>% 
#   gather() %>% 
#   arrange(value) %>%
#   filter(value < 0.7) %>%
#   pull(key) %>%
#   unique()
# fields_to_use


# data_partially_complete <-
#   ratios_final %>% 
#   filter(ticker %in% tickers_to_use) %>% 
#   select(all_of(fields_to_use))




# 5 Years
# ratios_complete_set_5y <-
#   get_complete_series(data = data_partially_complete, 
#                       years = 5,
#                       fields = fields_to_use) %>% 
#   left_join(ratios_final %>% 
#               select(ticker, report_date, all_of(name_sector_industry_fields)))
# ratios_complete_set_5y %>% distinct(ticker) # 257 --> 1,997 tickers --> 312
# ratios_complete_set_5y %>% 
#   summarize(across(report_date, list(start = min, end = max), .names = "{.fn}"))

# 8 Years
# ratios_complete_set_8y <-
#       get_complete_series(data = data_partially_complete, 
#                           years = 8,
#                           fields = fields_to_use)
# ratios_complete_set_8y %>% distinct(ticker) # 159 --> 1,475 tickers --> 193
# ratios_complete_set_8y %>% 
#   summarize(across(report_date, list(start = min, end = max), .names = "{.fn}"))

# 10 Years
# ratios_complete_set_10y <-
#   get_complete_series(data = data_partially_complete, 
#                       years = 10,
#                       fields = fields_to_use)
# ratios_complete_set_10y %>% distinct(ticker) # 32 --> 1,290 tickers --> 76
# ratios_complete_set_10y %>% 
#   summarize(across(report_date, list(start = min, end = max), .names = "{.fn}"))


# Save
# fwrite(ratios_complete_set_5y, 
#        paste0(dir_data, "data/cleaned data/", "ratios_complete_set_5y (",
#               str_replace_all(Sys.Date(), "-", " "), ")"))
# fwrite(ratios_complete_set_8y, 
#        paste0(dir_data, "data/cleaned data/", "ratios_complete_set_8y (",
#               str_replace_all(Sys.Date(), "-", " "), ")"))
# fwrite(ratios_complete_set_10y, 
#        paste0(dir_data, "data/cleaned data/", "ratios_complete_set_10y (",
#               str_replace_all(Sys.Date(), "-", " "), ")"))







# Load libraries and helper functions
source("helper functions.R")

ratios_final_files <-
  list.files(paste0(dir_data, "data/cleaned data/by industry"), 
             pattern = "ratios - ", full.names = TRUE)
ratios_final <- ratios_final_files %>% 
  map_df(read_tibble)


# ratios_final %>% filter(ticker == "SOFI") %>% pull(industry_yhoo)



# Obs per ticker
set.seed(123)
ratios_final %>% 
  # filter(str_starts(ticker, "Y")) %>% 
  filter(ticker %in% sample(ticker, 25)) %>%
  # filter(ticker == "AIRT") %>% 
    select(-name_edgar, -industry_edgar, -industry_detail_edgar, -sector_yhoo, -industry_yhoo, -long_business_summary, -name_simfin, -sector_simfin, -industry_simfin) %>% 
    # group_by(ticker, fundamentals_date) %>% 
    pivot_longer(-c(ticker, report_date), names_to = "field") %>% 
    mutate(field = str_trunc(field, 20)) %>%
  drop_na(value) %>%
  group_by(ticker) %>% 
  add_count(field) %>%
  select(-value, -report_date) %>% 
  arrange(n) %>% 
  distinct() %>%
  mutate(field = reorder(field, -n)) %>% 
  ungroup() %>%
  arrange(n) %>% 
# tally()
# group_by(ticker) %>% 
  complete(fill = unique(field)) %>%
  # select(ticker, field, n) %>% 
  # select(ticker) %>%
  ggplot(aes(x = ticker, y = field, fill = n)) +
  geom_tile() +
  scale_fill_distiller(palette = "Spectral", trans = "reverse", guide = guide_legend(reverse = TRUE), na.value = "black") +
  theme_light() +
  labs(x = "", y = "",
       title ="No. of monthly observations for each ticker",
       fill = "Obser.")



# Percent of Missing Values?
set.seed(123)
ratios_final %>% 
  # Choose 10 random tickers
  filter(ticker %in% sample(ticker, 25)) %>%
  select(ticker, where(is.numeric)) %>% 
  pivot_longer(-ticker, names_to = "field", values_to = "value") %>% 
  group_by(ticker, field) %>% 
  transmute(ticker, field, na_ratio = sum(is.na(value)) / n()) %>% 
  filter(na_ratio > 0.9) %>%
  # mutate(font_color = factor(ifelse(na_ratio == 0, "white", "black"))) %>% 
  ggplot(aes(ticker, field, fill = na_ratio)) +
  geom_tile(color = "white", lwd = 1) +
  # geom_text(aes(label = percent_format(accuracy = 1)(na_ratio)), color = "black", size = 4) +
  scale_fill_gradient(low = "white", high = "red", 
                      name = "", labels = scales::percent) +
  labs(title = "Percent of Missing Values", x = "", y = "") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 3),
        legend.key.height = unit(1, "cm"),
        legend.key.width = unit(0.2, "cm"))






# library(slider)
# vec <- c(NA, NA, .1, .2, .3, .4, .5, .6, .7, .8, .9, NA)
# 
# slide_dbl(vec, sd, .before = 2)
# slide_dbl(vec, mean, .before = 2)
# slide_dbl(1 + vec, prod, .before = 2) ^ (1/3) - 1
# ((1 + .1)*(1 + .2)*(1 + .3))^(1/3) - 1
# 
# 
# plot_data <-
#     ratios_joined %>% 
#     select(ticker, rounded_date, operating_profit_margin, 
#            accruals_pct_chg_4q) %>% #, gross_margin, operating_profit_margin_sd3y, gross_margin_sd3y, roe_sma4q, accruals, net_income, total_cash_from_operating_activities, roa, net_income, total_assets) %>% 
#     filter(ticker %in% c("DNKN", "ORLY", "MSFT", "MNST", "MRVL")) %>%
#     pivot_longer(-c(ticker, rounded_date), names_to = "field", values_to = "value") %>% 
#     drop_na(value)
# 
# plot_data %>% 
#     ggplot(aes(x = rounded_date, y = value, color = ticker)) +
#     geom_point(size = 2) +
#     geom_line() +
#     facet_wrap(vars(field), scales = "free_y")
#     # geom_line() +
#     # scale_y_discrete() +
#     # geom_point(aes(x = report_date, y = operating_profit_margin), color = "midnightblue", size = 2) +
#     # geom_line(aes(x = report_date, y = operating_profit_margin), color = "midnightblue") +
#     # scale_y_continuous(label = scales::dollar_format(scale = 1e-6)) +
#     # scale_x_date(labels = scales::date_format("%y/%m"),
#                  # breaks = scales::date_breaks("3 months")) +
#     # geom_label(aes(label = scales::dollar_format(scale = 1e-6)(operating_profit_margin_sd3Y)), color = "midnightblue",
#     #            nudge_y = 1.3, nudge_x = -1.3) +
#     # labs(title = grep("\\_", " ", str_to_title(unique(plot_data$field))), #subtitle = "(in millions)", 
#          # x = "", y = "") +
#     theme_minimal() +
#     expand_limits(x = as.Date("2021-12-31")) +
#     scale_x_date(#date_breaks = "5 years", date_labels = "%Y/%m",
#         breaks = as.Date(c("2005-12-31", "2010-12-31", "2015-12-31", "2020-12-31")),         
#         minor_breaks = NULL) +
#     # expand_limits(y = c(0.33, 0.37)) +
#     theme(#axis.text.y = element_blank(),
#           # panel.grid.minor.x = element_blank(),
#           #panel.grid.major.y = element_blank(),
#           # plot.background = element_rect(fill = "#BFD5E3"),
#           legend.position = "right",
#           legend.title = element_blank())
    


