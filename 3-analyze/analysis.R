library(tidyverse)
library(lubridate)
library(zoo)
library(scales)
library(ggsci)
library(reshape2)
# library(stargazer)

source("secrets.r")

con <- DBI::dbConnect(RMariaDB::MariaDB(),
        host = sqlHost,
        user = sqlUser,
        password = sqlPass,
        dbname = "defiData",
        bigint="integer" # needed to convert int64 to type R libraries (ggplot) can handle
)


div <- 1000000

mergeRecords <- tbl(con, 'mergeRecordsCache')
algoResults <- tbl(con, 'algoResults')
addrGroups <- tbl(con, 'addrGroups')
priceData <- tbl(con, 'priceData')


### Table 4: Summary of dataset
# Part A: Count of transactions and addresses
df1 <- mergeRecords %>%
  filter(!(trxType %like% "%Liquidat%")) %>% # Filter out liquidation transactions
  group_by(protocol) %>%
  summarise(uniqueAddr = n_distinct(addr1), countTransac = n())

# Part B: Value of transactions
df2 <- mergeRecords %>%
  group_by(protocol, trxType) %>%
  summarise(
    lockValue1 = if_else(
      trxType %in% c('Deposit', 'Mint'),
      sum(token1Usd),
      if_else(
        trxType == 'frob',
        sum(token1Usd[token1Usd > 0]),
        0
      )
    ),
    unlockValue1 = if_else(
      trxType %in% c('RedeemUnderlying', 'Redeem'),
      sum(token1Usd),
      if_else(
        trxType == 'frob',
        abs(sum(token1Usd[token1Usd < 0])),
        0
      )
    ),
    debtCreate1 = if_else(
      trxType == 'Borrow',
      sum(token1Usd),
      if_else(
        trxType == 'frob',
        sum(token2Usd[token2Usd > 0]),
        0
      )
    ),
    debtRepay1 = if_else(
      trxType %in% c('Repay', 'RepayBorrow'),
      sum(token1Usd),
      if_else(
        trxType == 'frob',
        abs(sum(token2Usd[token2Usd < 0])),
        0
      )
    ),
    swap1 = if_else(
      trxType == 'Swap',
      sum(abs(token1Usd)),
      0
    )
  ) %>% 
  group_by(protocol) %>%
  summarise(lockValue = sum(lockValue1), unlockValue = sum(unlockValue1), debtCreate = sum(debtCreate1), debtRepay = sum(debtRepay1), swap = sum(swap1))

# Merge two tables by protocol
df3 <- inner_join(df1, df2, by="protocol")
df3



### Section 6.2 text: group statistics
addrGroups %>% summarise(groups = max(groupID) + 1, addresses = n())
# NOTE: added one to the groupID, because groupID starts at 0 (avoid off-by-one error)

addrGroups %>% group_by(groupID) %>% summarise(numAddr = count(address)) %>% summarise(addrMore10 = sum(numAddr >= 10), addrOnly1 = sum(numAddr == 1))



### Table 5: Summary of algorithm results
# Collateral locked by month (debt/free/total, % debt)
# NOTE: sum of total collateral deposited, by month (Table 5) matches sum of total collateral deposited, by protocol (Table 4)
start_time <- Sys.time()

# The table algoResults does not capture all collateral locked, just the collateral locked by eligible addresses. Therefore, total collateral 
# must be derived from mergeRecords.

# Debt collateral
df1 <- algoResults %>%
  mutate(month = sql("EXTRACT(MONTH FROM blockTime)")) %>%
  group_by(month) %>%
  summarise(collatDebt = sum(debtAmtUsd))

# Free collateral
df2 <- mergeRecords %>%
  filter(trxType %in% c('frob', 'Deposit', 'Mint'),
    if (trxType == 'frob') {
      token1Usd > 0
    } else {
      TRUE
    }) %>%
  mutate(month = sql("EXTRACT(MONTH FROM blockTime)")) %>%
  group_by(month) %>%
  summarize(totalCollat = sum(token1Usd))

df3 <- inner_join(df1, df2, by="month") %>%
  mutate(debtPct = collatDebt / totalCollat, collatNonDebt = totalCollat - collatDebt) %>%
  relocate(collatNonDebt, .after = collatDebt)
df3

end_time <- Sys.time()
end_time - start_time



### Table 6A: Correlation of % collateral change vs. change in debt %
# Also: histogram of collateral change, debt change

# NOTE: the sum of collatTotalDaily (65,905,418,924) equals the sum in other tables in this file. The sum of collatTotalRoll (63,332,300,234) 
# does not equal collatTotalDaily, because the first 6 days are not present (not enough days for a 7-day rolling average)

create_summary_table <- function(typeGroup, calcType, scatterType, rollDays, lagTime) {
  typeGroup <- tolower(typeGroup)
  calcType <- tolower(calcType)
  scatterType <- tolower(scatterType)
  
  if (!(typeGroup %in% c('hour', 'day', 'week'))) {
    stop('Incorrect grouping type. Type must be "day" or "week".')
  }
  if (!(calcType %in% c('period', 'rolling'))) {
    stop('Incorrect calculation type. Type must be "period" or "rolling".')
  }
  if (!(scatterType %in% c('all', 'monthly'))) {
    stop('Incorrect scatter type. Type must be "all or "monthly".')
  }
  if (rollDays < 1 | rollDays > 30 | !(rollDays %% 1 == 0)) {
    stop('Incorrect number of days to average. Days to roll average may be between 1 and 30.')
  }
  if (lagTime < 1 | lagTime > 30 | !(lagTime %% 1 == 0)) {
    stop('Incorrect number of days to lag. Days to lag may be between 1 and 30.')
  }
  
  df1 <- algoResults %>%
    mutate(date = 
      if (typeGroup == 'day') {
        as_date(blockTime)
      } else if (typeGroup == 'hour') {
        sql("DATE_FORMAT(blockTime,'%Y-%m-%d %H:00')")
      } else { # typeGroup = 'week'
        week(as_date(blockTime))
      } ) %>%
    group_by(date) %>%
    summarise(collatDebtPeriod = sum(debtAmtUsd)) %>%
    as_tibble() %>%
    mutate(collatDebtRoll = rollmean(collatDebtPeriod, k = rollDays, align="right", fill=NA))
  
  df2 <- mergeRecords %>%
    filter(
      trxType %in% c('frob', 'Deposit', 'Mint'),
      if_else(trxType == 'frob', token1Usd > 0, TRUE)) %>%
    mutate(date = 
      if (typeGroup == 'day') {
        as_date(blockTime)
      } else if (typeGroup == 'hour') {
        sql("DATE_FORMAT(blockTime,'%Y-%m-%d %H:00')")
      } else { # typeGroup = 'week'
        week(as_date(blockTime))
      } ) %>%
    group_by(date) %>%
    summarize(collatTotalPeriod = sum(token1Usd)) %>%
    as_tibble() %>%
    mutate(collatTotalRoll = rollmean(collatTotalPeriod, k = rollDays, align="right", fill=NA))
  
  df3 <- inner_join(df1, df2, by='date') %>%
    mutate(
      debtPctPeriod = lead(collatDebtPeriod,n=lagTime) / lead(collatTotalPeriod,n=lagTime),
      collatChgPctPeriod = (collatTotalPeriod - lag(collatTotalPeriod, n = lagTime)) / lag(collatTotalPeriod, n = lagTime),
      debtPctRoll = lead(collatDebtRoll,n=lagTime) / lead(collatTotalRoll, n=lagTime),
      collatChgPctRoll = (collatTotalRoll - lag(collatTotalRoll, n = lagTime)) / lag(collatTotalRoll, n = lagTime),
      monthYear = floor_date(as_date(date), "month")) %>%
      relocate(collatTotalPeriod, collatTotalPeriod, debtPctPeriod, collatChgPctPeriod, .after = collatDebtPeriod)
  print(df3)
  
  if (calcType == 'period') {
    corrResult <- cor.test(df3$debtPctPeriod, df3$collatChgPctPeriod, method='pearson')
    
    if (scatterType == 'all') {
      chart1 <- ggplot(df3, aes(x=collatChgPctPeriod, y=debtPctPeriod))
      lmResult <- lm(debtPctPeriod ~ collatChgPctPeriod, data=df3)
    } else { # scattertype == 'monthly'
      chart1 <- ggplot(df3, aes(x=debtPctPeriod, y= collatChgPctPeriod, color=factor(monthYear)))
      
      months = df3 %>% group_by(monthYear) %>% summarise()
      lmResult <- list()
      for(i in 1:nrow(months)){
        month_scalar = months[[i,1]]
        subset_df3 = df3 %>% filter(monthYear == month_scalar)
        lmResult[[i]] <- lm(debtPctPeriod ~ collatChgPctPeriod, data=subset_df3)
      }
    }
    
    # chart2 <- ggplot(df3, aes(x=collatChgPctPeriod)) + geom_histogram()
    # chart3 <- ggplot(df3, aes(x=debtPctPeriod)) + geom_histogram()
  } else { # calcType == 'rolling'
    corrResult <- cor.test(df3$debtPctRoll, df3$collatChgPctRoll, method='pearson')
    
    if (scatterType == 'all') {
      chart1 <- ggplot(df3, aes(x=collatChgPctRoll, y=debtPctRoll))
      lmResult <- lm(debtPctRoll ~ collatChgPctRoll, data=df3)
    } else { # scattertype == 'monthly'
      chart1 <- ggplot(df3, aes(x=collatChgPctRoll, y=debtPctRoll, color=factor(monthYear)))
      
      months = df3 %>% group_by(monthYear) %>% summarise()
      lmResult <- list()
      for(i in 1:nrow(months)){
        month_scalar = months[[i,1]]
        subset_df3 = df3 %>% filter(monthYear == month_scalar)
        lmResult[[i]] <- lm(debtPctRoll ~ collatChgPctRoll , data=df3)
      }
    }
    
    # chart2 <- ggplot(df3, aes(x=collatChgPctRoll)) + geom_histogram()
    # chart3 <- ggplot(df3, aes(x=debtPctRoll)) + geom_histogram()
  }
  
  # chart1 <- chart1 + 
  #   geom_point() +
  #   geom_smooth(method=lm, se = FALSE) +
  #   scale_color_npg() + # Using this color palette because it has more than 8 colors (nejm only has 8, and our chart had 9 variables)
  #   labs(x="\nChange in collateral deposited", y="Debt-financed collateral\n") + 
  #   theme_classic() + 
  #   theme(
  #    axis.title.x = element_text(color="black"),
  #    axis.title.y = element_text(color="black"),
  #    axis.text.x = element_text(color="black"),
  #    axis.text.y = element_text(color="black"),
  #    text = element_text(size=16),
  #    panel.grid.major = element_blank(),
  #    panel.grid.minor = element_blank(),
  #    plot.margin = margin(10, 20, 5, 0)
  #   ) +
  #   scale_x_continuous(labels = percent_format(accuracy = 1), expand = c(0, 0)) +
  #   scale_y_continuous(labels = percent_format(accuracy = 1), expand = c(0, 0))
  # ggsave("debtVsCollat.pdf", width = 7, height = 5)
  
  # stargazer(lmResult, title="Regression results", type="text")
  print(corrResult)
  # print(chart1)
  # print(chart2)
  # print(chart3)
}

create_summary_table('hour', 'period', 'all', 7, 1)
create_summary_table('day', 'period', 'all', 7, 1)



### Table 6B: Correlation of % price change vs. change in debt %

create_summary_table <- function(typeGroup, lagTime) {
  typeGroup <- tolower(typeGroup)
  
  if (!(typeGroup %in% c('hour', 'day'))) {
    stop('Incorrect grouping type. Type must be "hour" or day".')
  }
  
  if (typeGroup == 'hour') {
    sqlStr <- "DATE_FORMAT(blockTime,'%Y-%m-%d %H:00')"
  } else { # typeGroup == 'day'
    sqlStr <- "DATE_FORMAT(blockTime,'%Y-%m-%d 00:00')"
  }
  
  # 1. Prices
  df1 <- priceData %>%
    filter(pair == 'ETH-USD') %>%
    rename(date=time)
  
  # 2a. Total collateral deposited
  df2a <- mergeRecords %>%
    filter(
      trxType %in% c('frob', 'Deposit', 'Mint'),
      if_else(trxType == 'frob', token1Usd > 0, TRUE))  %>%
    mutate(date = sql(sqlStr)) %>%
    group_by(date)  %>%
    summarize(collatTotal = sum(token1Usd))
  
  # 2b. Debt-financed collateral deposited
  df2b <- algoResults %>%
    mutate(date = sql(sqlStr)) %>%
    group_by(date) %>%
    summarise(collatDebt = sum(debtAmtUsd))
  
  # Joined table
  # NOTE: When joining hourly prices (df1) to daily amounts (df2a, df2b), the price being joined is the one at 00:00 hours
  df3 <- left_join(df1, df2a, by='date') %>%
    left_join(., df2b, by='date') %>%
    as_tibble() %>%
    mutate(
      collatChgPct = (lead(collatTotal,n=lagTime) - collatTotal) / collatTotal, # Change from Period 2 to Period 3 (lead)
      debtPct = lead(collatDebt,n=lagTime) / lead(collatTotal,n=lagTime), # Percent in Period 3 (lead)
      priceChgPct = (priceInUsd - lag(priceInUsd, n = lagTime)) / lag(priceInUsd, n = lagTime) # Change from Period 1 (lag) to Period 2 (current)
    ) %>%
    # Drop na's from the first row and last row
    drop_na(priceChgPct, collatChgPct, debtPct)
  
  # print(cor.test(df3$priceChgPct, df3$collatChgPct, method='pearson'))
  print(cor.test(df3$priceChgPct, df3$debtPct, method='pearson'))
  
  # chart1 <- ggplot(df3, aes(x=priceChgPct, y=debtPct)) + 
  #   geom_point() +
  #   geom_smooth(method=lm, se = FALSE) +
  #   scale_color_npg() + # Using this color palette because it has more than 8 colors (nejm only has 8, and our chart had 9 variables)
  #   labs(x="\nChange in price", y="Debt-financed collateral\n") + 
  #   theme_classic() + 
  #   theme(
  #    axis.title.x = element_text(color="black"),
  #    axis.title.y = element_text(color="black"),
  #    axis.text.x = element_text(color="black"),
  #    axis.text.y = element_text(color="black"),
  #    text = element_text(size=16),
  #    panel.grid.major = element_blank(),
  #    panel.grid.minor = element_blank(),
  #    plot.margin = margin(10, 20, 5, 0)
  #   ) +
  #   scale_x_continuous(labels = percent_format(accuracy = 1), expand = c(0, 0)) +
  #   scale_y_continuous(labels = percent_format(accuracy = 1), expand = c(0, 0)) +
  #   coord_cartesian(clip = "off")
  # 
  # print(chart1)

}

create_summary_table('hour', 1)
create_summary_table('day', 1)




### Figure 3a/b (protocol/currency) and Figure 5 (total): Percentage of debt-financed collateral, 7-day rolling average
start_time <- Sys.time()
create_summary_table <- function(typeGroup, rollDays) {
  typeGroup <- tolower(typeGroup)
  
  if (!(typeGroup %in% c('protocol', 'currency', 'none'))) {
    stop('Incorrect grouping type. Type must be "protocol", "currency" or "none".')
  }
  if (rollDays < 1 | rollDays > 30 | !(rollDays %% 1 == 0)) {
    stop('Incorrect number of days to average. Rolling average may be based on any number of days between 1 and 30.')
  }
  
  datalist1 <- list()
  datalist2 <- list()
  if (typeGroup == 'protocol') {
    names <- c('Aave', 'Compound', 'Maker')
  } else if (typeGroup == 'currency') {
    names <- c('DAI', 'USDC', 'USDT', 'WBTC', 'WETH')
  } else {
    names <- c(TRUE)
  }
  
  for (i in 1:length(names)) {
    name <- names[i]
    temp1 <- algoResults %>%
      filter(if (typeGroup == 'protocol') {
          protocol == name
        } else if (typeGroup == 'currency') {
          token == name
        } else { # typeGroup == 'none'
          TRUE
        }) %>%
      mutate(date=as_date(blockTime)) %>%
      group_by(
        date,
        if (typeGroup == 'protocol') {
          protocol
        } else if (typeGroup == 'currency') {
          token
        }) %>%
      summarise(collatDebtDaily = sum(debtAmtUsd)) %>%
      as_tibble() %>%
      mutate(collatDebtRoll = rollmean(collatDebtDaily, k = rollDays, align="right", fill=NA)) %>%
      drop_na(collatDebtRoll)
    
    temp2 <- mergeRecords %>%
      filter(if (typeGroup == 'protocol') {
          protocol == name
        } else if (typeGroup == 'currency') {
          token1Symbol == name
        } else { # typeGroup == 'none'
          TRUE
        },
        trxType %in% c('frob', 'Deposit', 'Mint'),
        if_else(trxType == 'frob', token1Usd > 0, TRUE)) %>%
      mutate(date=as_date(blockTime)) %>%
      group_by(
        date,
        if (typeGroup == 'protocol') {
          protocol
        } else if (typeGroup == 'currency') {
          token1Symbol
        }) %>%
      summarize(collatTotalDaily = sum(token1Usd)) %>%
      as_tibble() %>%
      mutate(collatTotalRoll = rollmean(collatTotalDaily, k = rollDays, align="right", fill=NA)) %>%
      drop_na(collatTotalRoll)
    
    datalist1[[i]] <- temp1
    datalist2[[i]] <- temp2
  }
  df1 <- bind_rows(datalist1)
  df2 <- bind_rows(datalist2)
  
  if (typeGroup == 'protocol') {
    df1 <- df1 %>% rename(protocol = "if (...) NULL")
    df2 <- df2 %>% rename(protocol = "if (...) NULL")
  } else if (typeGroup == 'currency') {
    df1 <- df1 %>% rename(currency = "if (...) NULL")
    df2 <- df2 %>% rename(currency = "if (...) NULL")
  } else { # typeGroup == 'none'
    df1 <- df1 %>% select(-'if (...) NULL')
    df2 <- df2 %>% select(-'if (...) NULL')
  }

  df3 <- inner_join(df1, df2, by=c(
    'date',
    if (typeGroup == 'protocol') {
        'protocol'
    } else if (typeGroup == 'currency') {
      'currency'
    })) %>%
    mutate(debtPct = collatDebtRoll / collatTotalRoll)
  
  if (typeGroup == 'protocol') {
    chart <- ggplot(df3, aes(x=date, y=debtPct, group=protocol,color=protocol))
  } else if(typeGroup == 'currency') {
    chart <- ggplot(df3, aes(x=date, y=debtPct, group=currency,color=currency))
  } else { # typeGroup == 'none'
    chart <- ggplot(df3, aes(x=date, y=debtPct))
  }

  chart <- chart +
    geom_line(size=1) +
    scale_x_date(date_labels = "%b %Y", expand = c(0, 0)) +
    scale_y_continuous(labels = percent_format(accuracy = 1), limits=c(0,1), expand = c(0, 0)) +
    theme_classic() +
    theme(
     axis.title.x = element_text(color="black"),
     axis.title.y = element_text(color="black"),
     axis.text.x = element_text(color="black"),
     axis.text.y = element_text(color="black"),
     text = element_text(size=16),
     panel.grid.major = element_blank(),
     panel.grid.minor = element_blank()
    ) +
    scale_color_nejm(name =
      if (typeGroup == 'protocol') {
         "Protocol"
       } else if (typeGroup == 'currency') {
         "Currency"
       } else{
         TRUE
       }) +
    labs(x="\nTime", y = "Debt % of collateral\n")
  print(chart)
  
  print(df3)
  
  # Amount below shows totals by of debt-financed collateral, not %. Only turn on if wanting to save to PDF. ggsave will save latest ggplot, so
  # using this code below will cause the chart with debt-financed totals (not %'s) to be saved to PDF
  # chart1 <- df3 %>% group_by(date) %>%
  #   summarize(totDebt=sum(collatDebtRoll)) %>%
  #   ggplot(aes(x=date, y=totDebt)) +
  #   geom_line(size=1) +
  #   scale_x_date(date_labels = "%b %Y", expand = c(0, 0)) +
  #   scale_y_continuous(labels = unit_format(unit = "M", scale = 1e-6), limits=c(0,300000000), expand = c(0, 0)) +
  #   theme_classic() +
  #   theme(
  #    axis.title.x = element_text(color="black"),
  #    axis.title.y = element_text(color="black"),
  #    axis.text.x = element_text(color="black"),
  #    axis.text.y = element_text(color="black"),
  #    text = element_text(size=16),
  #    panel.grid.major = element_blank(),
  #    panel.grid.minor = element_blank()
  #   ) +
  #   scale_color_nejm(name =
  #     if (typeGroup == 'protocol') {
  #        "Protocol"
  #      } else if (typeGroup == 'currency') {
  #        "Currency"
  #      } else{
  #        TRUE
  #      }) +
  #   labs(x="\nTime", y = "Debt-financed collateral\n")
  # print(chart1)
}

create_summary_table('protocol', 7)
ggsave("debtPctProtocol.pdf")
create_summary_table('currency', 7)
ggsave("debtPctCurrency.pdf")
create_summary_table('none', 7)
ggsave("debtPctTotal.pdf")

end_time <- Sys.time()
end_time - start_time



### Figure 4a-d: Transaction volumes for deposit/borrowing, by protocol/currency
start_time <- Sys.time()
create_chart <- function(typeParam, groupingParam, rollDays) {
  # switch to lower, so we don't have to worry about case sensitivity
  type = tolower(typeParam)
  grouping = tolower(groupingParam)
  
  if (!(type %in% c('borrow', 'deposit'))) {
    stop('Incorrect chart type. Chart type must either be "Borrow" or "Deposit".')
  }
  if (!(grouping %in% c('protocol', 'currency'))) {
    stop('Incorrect grouping type. Grouping must either be "protocol" or "currency".')
  }
  if (rollDays < 1 | rollDays > 30 | !(rollDays %% 1 == 0)) {
    stop('Incorrect number of days to average. Rolling average may be based on any number of days between 1 and 30.')
  }
  
  names <- c('Aave', 'Compound', 'Maker')
  datalist <- list()
  
  if (type == 'borrow') {
    trxTypes <- c('Borrow', 'Borrow', 'frob')  
  } else { # deposit
    trxTypes <- c('Deposit', 'Mint', 'frob')
  }
  
  for (i in 1:length(names)) {
    name <- names[i]
    trx <- trxTypes[i]
    temp <- mergeRecords %>%
      filter(
        protocol == name,
        trxType == trx,
        if (name == 'Maker') {
          if (type == 'borrow') {
            token2Usd > 0
          } else { # type == 'deposit'
            token1Usd > 0
          } 
        } else {
          TRUE # throwaway value to make sure it passes through filter
        }
        ) %>%
      mutate(day=as_date(blockTime)) %>%
      group_by(
        if (grouping == 'currency') {
          if (name == 'Maker' & type == 'borrow') {
            token2Symbol
          } else {
            token1Symbol
          }
        } else { # grouping == protocol
          protocol
        },
        day
        ) %>%
      summarize(
        totalUsd = 
          if (name == 'Maker' & type == 'borrow') {
           sum(token2Usd)
          } else { # (name != Maker) || (name == Maker & type == deposit)
            sum(token1Usd)
          }
        ) %>%
      as_tibble() # rollmean doesn't work in sql query, so have to convert to tibble
    
    # Drop the first 6 rows, which don't have enough observations to create a 7-day average
    if (grouping == 'protocol') {
      temp <- temp %>%
        mutate(mean7day = rollmean(totalUsd, k = rollDays, align="right", fill=NA)) %>%
        drop_na(mean7day) %>% 
        rename(protocol = "if (...) NULL") # rename column
    }
    
    datalist[[i]] <- temp
  }
  
  full_list <- bind_rows(datalist)
  if (grouping == 'currency') {
    full_list <- full_list %>% 
    rename(token1Symbol = "if (...) NULL") %>%
    group_by(token1Symbol, day) %>%
    summarize(totalUsd = sum(totalUsd)) %>%
    mutate(mean7day = rollmean(totalUsd, k = rollDays, align="right", fill=NA)) %>%
    drop_na(mean7day)
  }
  print(full_list %>% group_by(day) %>% summarize(total = sum(mean7day)))
  
  full_list %>% ggplot(aes(
      x=day,
      y = mean7day / div,
      fill =
       if (grouping == 'protocol') {
         protocol
       } else {
         token1Symbol
       })) +
    geom_area() +
    scale_x_date(date_labels = "%b %Y", expand = c(0, 0)) +
    scale_y_continuous(
      expand = c(0, 0),
      labels = label_comma(suffix = "M", sep = ""),
      limits=c(0,1200) # Hard-coded, so that borrowing and deposits have same Y scale
      ) +
    theme_classic() +
    theme(
     axis.title.x = element_text(color="black"),
     axis.title.y = element_text(color="black"),
     axis.text.x = element_text(color="black"),
     axis.text.y = element_text(color="black"),
     text = element_text(size=16)   
     ) +
    scale_fill_nejm() + 
    labs(
      x="\nTime", 
      y = if (type == 'borrow') {
          "Amounts Borrowed (USD)\n"
        } else { # deposit
          "Amounts Deposited (USD)\n"
        }, 
      fill = if (grouping == 'protocol') {
         "Protocol"
       } else {
         "Currency"
       }
    )
}

create_chart('borrow', 'protocol', 7)
ggsave("usdBorrowProtocol.pdf")
create_chart('borrow', 'currency', 7)
ggsave("usdBorrowCurrency.pdf")
create_chart('deposit', 'protocol', 7)
ggsave("usdDepositProtocol.pdf")
create_chart('deposit', 'currency', 7)
ggsave("usdDepositCurrency.pdf")

end_time <- Sys.time()
end_time - start_time




### NOTE: Graphs 1-6 are included for reference below, but were not used in the final paper
# Graph 1: Count of pair transactions in Uniswap, ordered by count
mergeRecords %>% filter(protocol == 'Uniswap') %>%
  mutate(pair=paste(token1Symbol, token2Symbol, sep="-")) %>%
  count(pair) %>%
  ggplot(aes(x=reorder(pair,n), y=n)) + geom_bar(stat="identity") + coord_flip()

# Graph 2: Count of transaction types, by protocol
mergeRecords %>% count(protocol, trxType, sort=TRUE)

# NOTE: totals are listed as integer64, which ggplot can't handle. Therefore, RMariaDB setting changes int64 to regular integer

# Graph 3a: Count of transactions per month, stacked area
mergeRecords %>% mutate(day=as_date(blockTime)) %>%
    group_by(day, protocol) %>%
    summarize(total = count(id)) %>%
    ggplot(aes(x=day,y=total, fill=protocol)) +
      geom_area() +
      scale_x_date(date_labels = "%b")

# Graph 3b: Count of transactions per month (not including Uniswap)
mergeRecords %>% filter(protocol != "Uniswap") %>%
    mutate(day=as_date(blockTime)) %>%
    group_by(day, protocol) %>%
    summarize(total = count(id)) %>%
    ggplot(aes(x=day,y=total, fill=protocol)) +
      geom_area() +
      scale_x_date(date_labels = "%b")



# Graph 4a: Debt vs. non-debt amounts
# Debt collateral
df1 <- algoResults %>%
  mutate(day=as_date(blockTime)) %>%
  group_by(day) %>%
  summarise(sumDebt = sum(debtAmtUsd)) %>%
  as_tibble() %>%
  mutate(Debt = rollmean(sumDebt, k = 7, align="right", fill=NA)) %>%
  drop_na(Debt)

# Free collateral
df2 <- mergeRecords %>%
  filter(trxType %in% c('frob', 'Deposit', 'Mint'),
    if (trxType == 'frob') {
      token1Usd > 0
    } else {
      TRUE
    }) %>%
  mutate(day=as_date(blockTime)) %>%
  group_by(day) %>%
  summarize(sumFree = sum(token1Usd)) %>%
  as_tibble() %>%
  mutate(`Non-debt` = rollmean(sumFree, k = 7, align="right", fill=NA)) %>%
  drop_na(`Non-debt`)

df3 <- inner_join(df1, df2, by="day") %>% melt(vars="day",measure.vars=c("Debt","Non-debt"))
df3

ggplot(df3, aes(x=day,y=value/div, fill=fct_reorder(variable, value, .desc = TRUE))) +
  geom_area(position="identity") +
  labs(fill="Collateral", x="\nTime", y="Amounts Locked (USD)\n") +
  scale_x_date(date_labels = "%b %Y", expand = c(0, 0)) +
  scale_y_continuous(labels = label_comma(suffix = "M", sep = ""), expand = c(0, 0)) +
  theme_classic() +
  theme(
   axis.title.x = element_text(color="black"),
   axis.title.y = element_text(color="black"),
   axis.text.x = element_text(color="black"),
   axis.text.y = element_text(color="black"),
   text = element_text(size=16)) +
  scale_fill_nejm()

ggsave("debtFree.pdf")

# Graph 4b: Debt vs. non-debt, two sides of one chart
df1 <- df1 %>%
  mutate(Debt = -Debt)

ggplot() +
  geom_area(data = df1, aes(x=day, y=Debt/div), fill="#0072B5FF") +
  geom_area(data = df2, aes(x=day, y=`Non-debt`/div), fill="#BC3C29FF") +
  scale_x_date(date_labels = "%b %Y", expand = c(0, 0)) +
  scale_y_continuous(labels = label_comma(suffix = "M", sep = "")) +
  coord_flip()




# Graph 5: Debt by protocol
create_chart <- function(groupingParam) {
  # switch to lower, so we don't have to worry about case sensitivity
  grouping = tolower(groupingParam)
  
  if (!(grouping %in% c('protocol', 'currency'))) {
    stop('Incorrect grouping type. Grouping must either be "protocol" or "currency".')
  }
  
  algoResults %>%
    mutate(day=as_date(blockTime)) %>%
    group_by(day, if (grouping == 'protocol') {
          protocol
        } else { # grouping == protocol
          token
        }) %>%
    summarise(sumDebt = sum(debtAmtUsd)) %>%
    as_tibble() %>%
    rename(grouping = "if (...) NULL") %>%
    mutate(sumDebt = rollmean(sumDebt, k = 7, align="right", fill=NA)) %>%
    ggplot(aes(x=day,y=sumDebt, fill=grouping)) +
    geom_area() +
    labs(fill="Protocol", x="\nTime", y="Amounts Locked (USD)\n") +
    scale_x_date(date_labels = "%b %Y", expand = c(0, 0)) +
    scale_y_continuous(labels = label_number_si(), expand = c(0, 0)) +
    theme(
     axis.title.x = element_text(color="black"),
     axis.title.y = element_text(color="black"),
     axis.text.x = element_text(color="black"),
     axis.text.y = element_text(color="black"),
     text = element_text(size=16)) +
    scale_fill_nejm()
}

create_chart('protocol')
create_chart('currency')



# Graph 6: Density chart of group size
df1 <- addrGroups %>%
  group_by(groupID) %>%
  summarise(countAddr = n()) %>%
  arrange(desc(countAddr))
  
df1 %>%
  filter(countAddr >= 10) %>%
  ggplot(aes(x=countAddr)) +
    geom_density(aes(y=..count..), color="black", fill="blue", alpha=0.3) +
    scale_x_continuous(breaks=c(0,1,2,3,4,5,10,30,100,300,1000), trans="log1p", expand=c(0,0)) +
    scale_y_continuous(breaks=c(0,125,250,375,500,625,750), expand=c(0,0)) +
    theme_bw()

df1 %>%
  filter(countAddr < 10) %>%
  ggplot(aes(x=countAddr)) +
    geom_density(aes(y=..count..), color="black", fill="blue", alpha=0.3) +
    scale_x_continuous(breaks=c(0,1,2,3,4,5,10,30,100,300,1000), trans="log1p", expand=c(0,0)) +
    scale_y_continuous(breaks=c(0,125,250,375,500,625,750), expand=c(0,0)) +
    theme_bw()

#   data %>%
  # ggplot(aes(x=countAddr)) +
  #   geom_density(aes(y=..count..), color="black", fill="blue", alpha=0.3, stat = "bin") +
  #   scale_x_continuous(breaks=c(0,1,2,3,4,5,10,30,100,300,1000), trans="log1p", expand=c(0,0)) +
  #   scale_y_continuous(breaks=c(0,125,250,375,500,625,750), expand=c(0,0)) +
  #   theme_bw()

# Layer on USD value (lhs of chart) of debt locked for each group



DBI::dbDisconnect(con)


