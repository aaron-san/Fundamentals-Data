
# Data quality check
# - Analyze the quality of the cleaned data

# library(tidyverse)
# 
# balance_sheets_quarterly_files <- list.files("cleaned data", pattern = "balance_sheets_quarterly", full.names = TRUE)
# balance_sheets_quarterly <- map(balance_sheets_quarterly_files, ~read_csv(.x, col_types = cols()))
# balance_sheets_quarterly <- bind_rows(balance_sheets_quarterly)
# 
# balance_sheets_yearly_files <- list.files("cleaned data", pattern = "balance_sheets_yearly", full.names = TRUE)
# balance_sheets_yearly <- map(balance_sheets_yearly_files, ~read_csv(.x, col_types = cols()))
# balance_sheets_yearly <- bind_rows(balance_sheets_yearly)


# balance_sheets_quarterly %>% distinct(ticker)
# balance_sheets_quarterly %>% 
#     group_by(ticker) %>% 
#     has_gaps(avg_days_bw_period = 365/4, mean_day_tolerance = 3,
#              day_tolerance = 40) %>% 
#     filter(has_gaps == TRUE)
    
# balance_sheets_quarterly %>% colnames()




