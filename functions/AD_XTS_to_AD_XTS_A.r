library(xts)
library(dplyr)

## CONTAINS ALL FUNCTIONS THAT CONVERT FROM ADAGE XTS TO ADAGE XTS

#APPLICATION 1
#Name: Builds aggregated timeseries based on prices 
#define function which converts AD important attributes of trades into aggregated series
#From GUI we have
#Input datasource = Adage
#<<<<<<< Updated upstream
#input dataset structure = Trades
#input format = RDS
#Output datasource = Adage
#Output format = RDS
#output dataset structure = aggregated intraday measures
#=======
#input dataset structure= Trades
#input format = RDS
#Output datasource = Adage
#Output format = RDS
#output dataset structure= aggregated intraday measures
#>>>>>>> Stashed changes

#PARAMETER 1: lengthPeriod (e.g. 5) this is an integer
#PARAMETER 2: unitPeriod (e.g. "minutes")
# this is a drop down list with the following choices in order
# "mins" or "minutes" (minutes)
# "hours" (hours)
# "secs" or "seconds" (seconds)
# "ms" or "milliseconds" (milliseconds)
# "us" or "microseconds" (microseconds) 

#for now, input atributes are: Price, Volume and Market.VWAP
#output attributes are:

aggreFunLst1 <- function(x) {
  c(data.frame(coredata(x))%>%filter(TradeCount!=0)%>%nrow(),
    sum(x$ShareVolumeTraded),
    sum(x$DollarVolumeTraded))
}

colLast <- function(x) {
  tail(x,1)
}

calAggreMeasures <- function(google, stockprices_xts, ExchangeCode, tradeType, lengthPeriod, unitPeriod) {
  if (google == TRUE) {
    #if the data source is gcp
    if(ExchangeCode!="all"){
      stockprices_df <- data.frame(coredata(stockprices_xts),stringsAsFactors = FALSE)
      stockprices_df <- stockprices_df %>% mutate_at(.vars = c("Price", "Volume"), ~ifelse(Ex_Cntrb_ID != ExchangeCode, 0, .))
      stockprices_xts_rec <- xts(stockprices_df, order.by = index(stockprices_xts))
      stockprices_xts <- stockprices_xts_rec
    }

    if(tradeType!="all"){
      stockprices_df <- data.frame(coredata(stockprices_xts),stringsAsFactors = FALSE)
      stockprices_df <- stockprices_df %>% mutate_at(.vars = c("Price", "Volume"), ~ifelse(TradeCategory != tradeType, 0, .))
      stockprices_xts_rec <- xts(stockprices_df, order.by = index(stockprices_xts))
      stockprices_xts <- stockprices_xts_rec
    }
  } else {
    #if the data source is from refinitive
    if(ExchangeCode!="all"){
      stockprices_df <- data.frame(coredata(stockprices_xts),stringsAsFactors = FALSE)
      stockprices_df <- stockprices_df %>% mutate_at(.vars = c("Price", "Volume"), ~ifelse(Ex.Cntrb.ID != ExchangeCode, 0, .))
      stockprices_xts_rec <- xts(stockprices_df, order.by = index(stockprices_xts))
      stockprices_xts <- stockprices_xts_rec
    }
    if(tradeType!="all"){
      stockprices_df <- data.frame(coredata(stockprices_xts),stringsAsFactors = FALSE)
      stockprices_df <- stockprices_df %>% mutate_at(.vars = c("Price", "Volume"), ~ifelse(TradeCategory != tradeType, 0, .))
      stockprices_xts_rec <- xts(stockprices_df, order.by = index(stockprices_xts))
      stockprices_xts <- stockprices_xts_rec
    }
  }
  
  stockprices_xts <- stockprices_xts[,c("Price","Volume")]
  
  #insert a column recording the value of price*volume to stock_data.xts
  stockprices_xts <- merge(stockprices_xts, as.double(stockprices_xts$Price) * as.double(stockprices_xts$Volume))
  
  #change the column names to measure names
  colnames(stockprices_xts) <- c("TradeCount","ShareVolumeTraded","DollarVolumeTraded")
  
  #calculate endpoints for chosen period
  period_lenthp = endpoints(stockprices_xts, unitPeriod , lengthPeriod)
  
  #calculate the offeset for align.time() function.
  if(unitPeriod=="hours"){
    lengthPeriod2 <- lengthPeriod*60
  }else if(unitPeriod=="days"){
    lengthPeriod2 <- lengthPeriod*60*24
  }else{
    lengthPeriod2 <- lengthPeriod
  }
  aligntimeoffset <- lengthPeriod2*60
  
  #Use aggregation functions to calculate intraday measures respectively
  intrameasures_xts <- period.apply(stockprices_xts, INDEX=period_lenthp,FUN=aggreFunLst1) %>% align.time(n=aligntimeoffset)
  #Calculate VWAP by dividing DollarVolume by total volume for each time period and merge with other measures.
  intrameasures_xts <- merge(intrameasures_xts, 
                             VWAP=ifelse(intrameasures_xts$ShareVolumeTraded==0,0,
                                         intrameasures_xts$DollarVolumeTraded/intrameasures_xts$ShareVolumeTraded))

  colnames(intrameasures_xts)[4] <- "VWAP"
  
  if(ExchangeCode!="all"){
    colnames(intrameasures_xts) <- c(paste0("TradeCount_",ExchangeCode),paste0("ShareVolumeTraded_",ExchangeCode),paste0("DollarVolumeTraded_",ExchangeCode),paste0("VWAP_", ExchangeCode))
  }
  if(tradeType!="all"){
    colnames(intrameasures_xts) <- paste0(colnames(intrameasures_xts),'_',tradeType)
  }
  
  intrameasures_xts
}

ad_xts_rds_to_ad_aggtrademeasures_xts <- function(google, path, filename,lengthPeriod,unitPeriod) {
# Modified by Hanzhao Lu, adding another way to check the source of data
if (google == 'TRUE') {
  google <- TRUE
} else {
  google <- FALSE
}
#Check file existence
filename_input <- paste0(path,paste0(filename,".rds"))

if(!file.exists(filename_input))
{message(paste0(filename_input,': this file does not exist!'))
  }

periodName <- paste0('_',paste0(lengthPeriod,unitPeriod))
filename_output <- paste0(paste0(path,filename,periodName),".rds")

library(dplyr)
library(xts)

#Read RDS file  
stock_xts.lst = readRDS(filename_input)

qualifiersfilename <- paste0("./","TRTHQualifiers.csv")
#Now we read the correspondence between qualifiers and trade categories from a csv file
qua_tradecat_tbl <- read.csv(qualifiersfilename, header = T, sep = ',', stringsAsFactors = F)
tradetypes <- unique(qua_tradecat_tbl[,4])


stockmeasures_xts.lst <- list()

lapply(stock_xts.lst, function(stockprices_xts){
 
  intrameasures_xts <- calAggreMeasures(google,stockprices_xts,"all","all",lengthPeriod, unitPeriod)
  
  #modifed by Kun Lu, 20200628, calculate measures only regarding current exchanges and tradeCategories in the trading data.
  #exchangeCodes <- list("ASX","CHA")
  #modified by Alfred Lu use different column name if datat source is gcp
  if (google) {
    exchangeCodes <- unique(stockprices_xts[,"Ex_Cntrb_ID"])
  } else {
    exchangeCodes <- unique(stockprices_xts[,"Ex.Cntrb.ID"])
  }
  
  #tradetypes <- unique(stockprices_xts[,"TradeCategory"])
  
  #if(is.na(exchangeCodes[[1]])){
  #  stop("The value of column \"Ex.Cntrb.ID\" shouldn't be NA!")
  #}
  
  if(length(exchangeCodes) > 1){
    lapply(exchangeCodes, function(exchangeCode){
      intrameasures_anexchange.xts <- calAggreMeasures(google,stockprices_xts, exchangeCode, "all", lengthPeriod, unitPeriod)
      intrameasures_xts <<- merge(intrameasures_xts, intrameasures_anexchange.xts)
    
      #modifed by Kun Lu, 20200628, calculate measures only regarding current exchanges and tradeCategories in the trading data.
      #modified by Alfred lu, 20211111, use different column name if data source is gcp
      if (google) {
        tradetypes <- unique(stockprices_xts[stockprices_xts[,"Ex_Cntrb_ID"]==exchangeCode,"TradeCategory"])
      } else {
        tradetypes <- unique(stockprices_xts[stockprices_xts[,"Ex.Cntrb.ID"]==exchangeCode,"TradeCategory"])
      }
    
      lapply(tradetypes, function(tradetype){
        intrameasures_atradetype.xts <- calAggreMeasures(google,stockprices_xts, exchangeCode, tradetype,lengthPeriod, unitPeriod)
        intrameasures_xts <<- merge(intrameasures_xts, intrameasures_atradetype.xts)
      })
    })
  }else{ #only one exchange
    tradetypes <- unique(stockprices_xts[,"TradeCategory"])
    
    lapply(tradetypes, function(tradetype){
      #modified by Alfred Lu, 20211111, add anew variable google
      intrameasures_atradetype.xts <- calAggreMeasures(google, stockprices_xts, "all", tradetype,lengthPeriod, unitPeriod)
      intrameasures_xts <<- merge(intrameasures_xts, intrameasures_atradetype.xts)
    })
    
  }
  
  xtsAttributes(intrameasures_xts)$RIC <- xtsAttributes(stockprices_xts)$RIC
  xtsAttributes(intrameasures_xts)$GMTOffset <- xtsAttributes(stockprices_xts)$GMTOffset
  xtsAttributes(intrameasures_xts)$EventType <- xtsAttributes(stockprices_xts)$EventType
  xtsAttributes(intrameasures_xts)$Market <- xtsAttributes(stockprices_xts)$Market
  xtsAttributes(intrameasures_xts)$XTSName <- paste0(xtsAttributes(stockprices_xts)$XTSName,"_measures","_",lengthPeriod, unitPeriod)
  
  stockmeasures_xts.lst[[length(stockmeasures_xts.lst)+1]] <<- intrameasures_xts
})

### Saving the XTS object
###After that, the XTS object stock_data.xts could be saved in a **`.rds`** file for future use like any other standard R object. You can use the following code to do this.
saveRDS(stockmeasures_xts.lst, file=filename_output)
return(filename_output)

}
## Example of usage
#<<<<<<< Updated upstream
#ad_xts_csv_to_ad_aggtrademeasures_xts("/Users/redatawfiki/Documents/UNSW/Github/unsw-intradaymeasures-project/Code/ADAGE\ library/",
#                                      "DC_BHPAX_20190717_trade_partial",5,"minutes")
#=======
#ad_xts_csv_to_ad_aggtrademeasures_xts("/Users/redatawfiki/Documents/UNSW/Github/unsw-intradaymeasures-project/Code/ADAGE\ library/",
#                                      "DC_BHPAX_20190717_trade_partial",5,"minutes")
#>>>>>>> Stashed changes
#ad_xts_csv_to_ad_aggtrademeasures_xts("C:\\Users\\fethi\\Documents\\gitHubUNSW\\unsw-intradaymeasures-project\\Code\\ADAGE library\\",
#                                     "DC_BHPAX_20190717_trade_partial",1,"minutes")
##-------------------------------------------------------------------------------------------------------------------
#ad_xts_rds_to_ad_aggtrademeasures_xts("./",
#                                       "DC_6StocksAUX_20190717_trade_2hours",5,"minutes")
# ts <- readRDS("DC_6StocksAUX_20190717_trade_2hours_5minutes.rds")
# firstts <- ts[[1]] 
########################################################################################################################
#-------------------------------------------------------------
  ## Generating timeseries of quote measures
  #APPLICATION 2 (in AD to AD)
  #Name: Aggregate ADAGE quote timeseries from Datascope tick data
  #From GUI we have
  #Input datasource = Adage
  #dataset structure= Quotes
  #Input format=RDS
  #Output dastasource=Adage
  #Output format=RDS
  
  
ad_xts_rds_to_ad_aggquotemeasures_xts <- function(path, filename,lengthPeriod,unitPeriod) {
  
  #Check file existence
  filename_input <- paste0(path,paste0(filename,".rds"))
  
  if(!file.exists(filename_input))
  {message(paste0(filename_input,': this file does not exist!'))
  }
  
  periodName <- paste0('_',paste0(lengthPeriod,unitPeriod))
  filename_output <- paste0(path, filename, periodName, "_q.rds")
  
  #Read RDS file  
  stock_xts.lst = readRDS(filename_input)
  
  #xts_quotes = stock_xts.lst[[1]]
  
  stockmeasures_xts.lst <- list()
  
  lapply(stock_xts.lst, function(xts_quotes){
    
    period = endpoints(xts_quotes, unitPeriod , lengthPeriod)
    
    #calculate the offeset for align.time() function.
    if(unitPeriod=="hours"){
      lengthPeriod2 <- lengthPeriod*60
    }else if(unitPeriod=="days"){
      lengthPeriod2 <- lengthPeriod*60*24
    }else{
      lengthPeriod2 <- lengthPeriod
    }
    aligntimeoffset <- lengthPeriod2*60
    
    #get the dataframe from the XTS object and add columns to the dataframe
    df_xts_quotes <- data.frame(coredata(xts_quotes),stringsAsFactors = FALSE)
    df_xts_quotes <- df_xts_quotes %>% mutate("LastQuotedSpread" = as.double(lag(QuotedSpread)))
    df_xts_quotes <- df_xts_quotes %>% mutate("LastPercentageSpread" = as.double(lag(PercentageSpread)))
    df_xts_quotes <- df_xts_quotes %>% mutate("LastQuotedDollarDepth" = as.double(lag(QuotedDollarDepth)))
    df_xts_quotes <- df_xts_quotes %>% mutate("LastQuotedShareDepth" = as.double(lag(QuotedShareDepth)))
    df_xts_quotes <- df_xts_quotes %>% mutate("Date.Time" = as.POSIXct(Date.Time, format = "%Y-%m-%d %H:%M:%OS", tz = "UTC")) 
    df_xts_quotes <- df_xts_quotes %>% mutate("LastDate.Time" = as.POSIXct(lag(Date.Time), format = "%Y-%m-%d %H:%M:%OS", tz = "UTC"))
    df_xts_quotes <- df_xts_quotes %>% mutate("Timealigned_start" = align.time(Date.Time, n=aligntimeoffset) - aligntimeoffset)
    df_xts_quotes <- df_xts_quotes %>% mutate("Timealigned_end" = align.time(Date.Time, n=aligntimeoffset))
    df_xts_quotes <- df_xts_quotes %>% mutate("TimeExisting_start" =  as.double(difftime(Date.Time, Timealigned_start)))
    df_xts_quotes <- df_xts_quotes %>% mutate("TimeExisting_end" =  as.double(difftime(Timealigned_end, Date.Time)))
    
    #set TimeExisting_start to 0 for all the rows except the ones at the beginning of each interval.
    df_xts_quotes[-(period[1:(length(period)-1)]+1),"TimeExisting_start"]=0
    #modify TimeExisting values of the rows at the end of each interval.
    df_xts_quotes[period[2:length(period)],"TimeExisting"]=df_xts_quotes[period[2:length(period)],"TimeExisting_end"]
    
    removeNAColumns <- c("QuotedSpread", "LastQuotedSpread", 
                         "PercentageSpread", "LastPercentageSpread", 
                         "QuotedDollarDepth", "LastQuotedDollarDepth", 
                         "QuotedShareDepth", "LastQuotedShareDepth", 
                         "TimeExisting", "TimeExisting_start")
    df_xts_quotes[,removeNAColumns] <- replace(df_xts_quotes[,removeNAColumns], is.na(df_xts_quotes[,removeNAColumns]), 0)
    
    df_xts_quotes <- df_xts_quotes %>% mutate("TimeExistingSum" = 
                                                as.double(TimeExisting) + TimeExisting_start)
    
    #modify TimeExistingSum and related quote measure values of the rows at the beginning of each interval.
    #If this is the first row in the raw data, the previous quote measure values are already set to 0,
    #so we only adjust the TimeExistingSum value.
    #If this is the first interval of a transaction day, set the previous quote measure values to 0 and 
    #adjust the current TimeExistingSum value.
    df_xts_quotes[1, "TimeExistingSum"] <- df_xts_quotes[1, "TimeExistingSum"] - 
                                                  df_xts_quotes[1, "TimeExisting_start"]
    for( i1 in 2:(length(period)-1)) {
      index1 <- period[i1]+1
      if(as.Date(df_xts_quotes[index1,"Date.Time"] + xtsAttributes(xts_quotes)$GMTOffset*60*60) 
         > as.Date(df_xts_quotes[index1,"LastDate.Time"] + xtsAttributes(xts_quotes)$GMTOffset*60*60)){
        df_xts_quotes[index1,"LastQuotedSpread"]<-0
        df_xts_quotes[index1,"LastPercentageSpread"]<-0
        df_xts_quotes[index1,"LastQuotedDollarDepth"]<-0
        df_xts_quotes[index1,"LastQuotedShareDepth"]<-0
        df_xts_quotes[index1, "TimeExistingSum"] <- df_xts_quotes[index1, "TimeExistingSum"] - 
          df_xts_quotes[index1, "TimeExisting_start"]
      }
    }
    
    #calculate each quote measure (Step 1).
    df_xts_quotes <- df_xts_quotes %>% mutate("QuotedSpreadSum" = 
                                                as.double(QuotedSpread)*as.double(TimeExisting)+LastQuotedSpread*TimeExisting_start)
    df_xts_quotes <- df_xts_quotes %>% mutate("PercentageSpreadSum" = 
                                                as.double(PercentageSpread)*as.double(TimeExisting)+LastPercentageSpread*TimeExisting_start)
    df_xts_quotes <- df_xts_quotes %>% mutate("DollarDepthSum" = 
                                                as.double(QuotedDollarDepth)*as.double(TimeExisting)+LastQuotedDollarDepth*TimeExisting_start)
    df_xts_quotes <- df_xts_quotes %>% mutate("ShareDepthSum" = 
                                                as.double(QuotedShareDepth)*as.double(TimeExisting)+LastQuotedShareDepth*TimeExisting_start)
    
    df_xts_quotes <- df_xts_quotes %>% select(QuotedSpreadSum, PercentageSpreadSum, DollarDepthSum, ShareDepthSum, TimeExistingSum)
    #glue <- function(x){
    #  dft <- data.frame(coredata(x))
    #}
    
    #Rebuild a XTS object.
    xts_quotes_merged <- xts(df_xts_quotes, order.by = index(xts_quotes))
    
    #Use aggregation to calculate each measure (Step 2).
    xts_quotes_merged_sum <- period.apply(xts_quotes_merged, period, colSums) %>% 
                                align.time(n=aligntimeoffset)
    
    #adjust the time index
    index(xts_quotes_merged_sum) <- index(xts_quotes_merged_sum) - aligntimeoffset
    
    #Calculate the time weighted measures (Step 3).
    xts_quotes_merged_weighted <- xts_quotes_merged_sum %>%
      merge(xts_quotes_merged_sum[,"QuotedSpreadSum"]/xts_quotes_merged_sum[,"TimeExistingSum"]) %>%
      merge(xts_quotes_merged_sum[,"PercentageSpreadSum"]/xts_quotes_merged_sum[,"TimeExistingSum"]) %>%
      merge(xts_quotes_merged_sum[,"DollarDepthSum"]/xts_quotes_merged_sum[,"TimeExistingSum"]) %>%
      merge(xts_quotes_merged_sum[,"ShareDepthSum"]/xts_quotes_merged_sum[,"TimeExistingSum"])
    
    colnames(xts_quotes_merged_weighted)[c(6:9)] = c("WeightedQuotedSpread", "WeightedPercentageSpread", "WeightedDollarDepth", "WeightedShareDepth")
    xts_quotes_final <- xts_quotes_merged_weighted[,c("WeightedQuotedSpread", "WeightedPercentageSpread", "WeightedDollarDepth", "WeightedShareDepth")]
    
    
    # aligned_times = period.apply(xts_quotes[,"TimeExisting"], period,sum) %>%
    #   align.time(aaa, n=300)
    # aligned_time_quoted_spread = period.apply(xts_quotes[,"Time*QuotedSpread"], period,sum) %>%
    #   align.time(aaa, n=300)
    # aligned_time_percentage_spread = period.apply(xts_quotes[,"Time*PercentageSpread"], period,sum) %>%
    #   align.time(aaa, n=300)
    # aligned_time_dollar_depth = period.apply(xts_quotes[,"Time*DollarDepth"], period,sum) %>%
    #   align.time(aaa, n=300)
    # aligned_time_share_depth = period.apply(xts_quotes[,"Time*ShareDepth"], period,sum) %>%
    #   align.time(aaa, n=300)
    # TODO: add more fields to calculated time-weighted averages for, or do using a function
    
    # xts_quotes_merged = aligned_times %>%
    #   merge(aligned_time_quoted_spread) %>%
    #   merge(aligned_time_percentage_spread) %>%
    #   merge(aligned_time_dollar_depth) %>%
    #   merge(aligned_time_share_depth) 
    # 
    # xts_quotes_merged = xts_quotes_merged %>%
    #   merge(xts_quotes_merged[,"Time.QuotedSpread"]/xts_quotes_merged[,"TimeExisting"]) %>%
    #   merge(xts_quotes_merged[,"Time.PercentageSpread"]/xts_quotes_merged[,"TimeExisting"]) %>%
    #   merge(xts_quotes_merged[,"Time.DollarDepth"]/xts_quotes_merged[,"TimeExisting"]) %>%
    #   merge(xts_quotes_merged[,"Time.ShareDepth"]/xts_quotes_merged[,"TimeExisting"])
    # 
    # colnames(xts_quotes_merged)[c(6:9)] = c("WeightedQuotedSpread", "WeightedPercentageSpread", "WeightedDollarDepth", "WeightedShareDepth")
    # 
    # xts_quotes_final = xts_quotes_merged[,c("WeightedQuotedSpread", "WeightedPercentageSpread", "WeightedDollarDepth", "WeightedShareDepth")]
  
    xtsAttributes(xts_quotes_final)$RIC <- xtsAttributes(xts_quotes)$RIC
    xtsAttributes(xts_quotes_final)$Market <- xtsAttributes(xts_quotes)$Market
    xtsAttributes(xts_quotes_final)$GMTOffset <- xtsAttributes(xts_quotes)$GMTOffset
    xtsAttributes(xts_quotes_final)$EventType <- xtsAttributes(xts_quotes)$EventType
    xtsAttributes(xts_quotes_final)$XTSName <- paste0(xtsAttributes(xts_quotes)$XTSName,"_measures")
    
    stockmeasures_xts.lst[[length(stockmeasures_xts.lst)+1]] <<- xts_quotes_final
  })
  
  
  
  ### Saving the XTS object
  ###After that, the XTS object stock_data.xts could be saved in a **`.rds`** file for future use like any other standard R object. You can use the following code to do this.
  saveRDS(stockmeasures_xts.lst, file=filename_output)
  return(filename_output)
  
}

## Example of usage
#ad_xts_csv_to_ad_aggquotemeasures_xts("/Users/redatawfiki/Documents/UNSW/Github/unsw-intradaymeasures-project/Code/ADAGE\ library/",
#           "BHP_TS_Sample_small_less_cols_q",5,"minutes")
#ad_xts_csv_to_ad_aggquotemeasures_xts("C:\\Users\\fethi\\Documents\\gitHubUNSW\\unsw-intradaymeasures-project\\Code\\ADAGE library\\",
#                                      "BHP_TS_Sample_small_less_cols_q",1,"minutes")
#ad_xts_rds_to_ad_aggquotemeasures_xts("./","DC_6StocksAUX_20190717_trade_2hours_q",5,"minutes")
# qts <- readRDS("DC_6StocksAUX_20190717_trade_2hours_q_5minutes_q.rds")
# firstq <- qts[[1]] 

# xtsAttributes(firstq)$RIC
#[1] "ANZ.AUX"
#xtsAttributes(firstq)$EventType 
#[1] "Intraday Measures"
#xtsAttributes(firstq)$GMTOffset 
#[1] 10
#xtsAttributes(firstq)$XTSName
#[1] "DC_6StocksAUX_20190717_trade_2hours_ANZ.AUX_measures"


#APPLICATION 3
#Name: Aggregate and filter aggregated intraday timeseries
#From GUI we have
#Input datasource = Adage

#input dataset structure = aggregated intraday measures
#input format = RDS
#Output datasource = Adage
#Output format = RDS
#output dataset structure = aggregated intraday measures

#PARAMETER 1: StartTime (optional) e.g. 9am
#PARAMETER 2: EndTime (optional) e.g. 4pm
#PARAMETER : GMTorlocal indicates whether StartTime and EndTime are GMT time or local time.
#PARAMETER 3: lengthPeriod (e.g. 5) this is an integer
#PARAMETER 4: unitPeriod like before but we need to add "day"
#PARAMETER 5: The user can choose the columns of the output file for example
#ColumnNames: A list which will deliver column with name columnName
#ExchangeNames (optional): A list which will deliver column with name columnName_ExchangeName
#TradeCategoryNames (optional): A list which will deliver column with name columnName_ExchangeName_TradeCategoryName
#We can have a keyword "ALL" for everything.

ad_aggmeasures_xts_to_ad_aggmeasures_xts <- function(path, filename, partialOrAll, GMTorlocal, startTime, endTime, 
                      lengthPeriod,unitPeriod, columnNames, ExchangeNames, TradeCategoryNames){
  #Check file existence
  filename_input <- paste0(path,paste0(filename,".rds"))
  
  if(!file.exists(filename_input))
  {message(paste0(filename_input,': this file does not exist!'))
  }
  
  periodName <- paste0('_',paste0(lengthPeriod,unitPeriod))
  filename_output <- paste0(path, filename, periodName, ".rds")
  
  #Read RDS file  
  measures_xts.lst = readRDS(filename_input)
  
  if(unitPeriod=="hours"){
    lengthPeriod2 <- lengthPeriod*60
  }else if(unitPeriod=="days"){
    lengthPeriod2 <- lengthPeriod*60*24
  }else{
    lengthPeriod2 <- lengthPeriod
  }
  
  if(columnNames[[1]]=="ALL"){
    columnNames <- getAllTradeMeasureNames()
  }
  
  if(ExchangeNames[[1]]=="ALL"){
    ExchangeNames <- getAllExchangeNames()
  }
  
  aggmeasures_xts.lst <- list()
  
  lapply(measures_xts.lst, function(xts_measures){
    
    if(TradeCategoryNames[[1]]=="ALL"){
      TradeCategoryNames <- getAllTradecategories()
    }
    
    if(TradeCategoryNames[[1]]!="NONE"){
      #in case the xts object does not have all the trade categories
      tradeNames <- c()
      
      lapply(TradeCategoryNames, function(TradeCategoryName){
        #Does the xts object have a column or columns corresponding to this trade type?
        if(sum(grepl(paste0("_",TradeCategoryName,"$"), names(xts_measures)))>0){
          tradeNames <<- c(tradeNames, TradeCategoryName)
        }
      })
      
      TradeCategoryNames <- tradeNames
    }
    
    allColumnNames <- c()
    lapply(columnNames, function(columnName){
      
      #For example, "tradeCount"
      allColumnNames <<- c(allColumnNames, columnName)
      
      if(ExchangeNames[[1]]!="NONE"){
        lapply(ExchangeNames, function(ExchangeName){
          
          #For example, "tradeCount_ASX"
          allColumnNames <<- c(allColumnNames, paste0(columnName,"_",ExchangeName))
          
          if(TradeCategoryNames[[1]]!="NONE"){
            lapply(TradeCategoryNames, function(TradeCategoryName){
              
              #For example, "tradeCount_ASX_CentrePoint"
              allColumnNames <<- c(allColumnNames, paste0(columnName,"_",ExchangeName,"_",TradeCategoryName))
            })
          }
        })
      }#else{
      #  if(TradeCategoryNames[[1]]!="NONE"){
      #    lapply(TradeCategoryNames, function(TradeCategoryName){
      
      #For example, "tradeCount_ASX_CentrePoint"
      #     allColumnNames <<- c(allColumnNames, paste0(columnName,"_",TradeCategoryName))
      #    })
      #  }
      #}
    })
    
    GMTOffset1 <- xtsAttributes(xts_measures)$GMTOffset
    if(partialOrAll == "partial" & GMTorlocal == "local"){
      startTime <- as.POSIXlt(startTime) - GMTOffset1*60*60
      endTime <- as.POSIXlt(endTime) - GMTOffset1*60*60
      #my.lt = as.POSIXlt("2010-01-09 22:00:00")
      #my.lt <- my.lt + 60*60*3
      #abc <- paste0("",my.lt)
    }
    
    if(partialOrAll == "partial"){
      xts_measures_period <- xts_measures[paste0(startTime,"/",endTime)]
    }else{
      xts_measures_period <- xts_measures
    }
    #xts2 <- xts1["T01:10/T02:10"]
    #xts2 <- xts1["2020-04-27 00:10:00/2020-04-27 06:40:00"]
    
    #period1 = endpoints(xts_measures_period, unitPeriod , lengthPeriod) - 1
    #period1 = period1[period1>0]
    period1 = endpoints(xts_measures_period, unitPeriod , lengthPeriod)
    
    #aggregate
    xts_measures_period_agg <- 
      period.apply(xts_measures_period[,!grepl("^VWAP", names(xts_measures_period))], period1,colSums) %>%
                                  align.time(n=lengthPeriod2*60)
    
    #aggregate measures beginning with "VWAP"
    xts_measures_period_agg_1 <- 
      period.apply(xts_measures_period[,grepl("^VWAP", names(xts_measures_period))], period1,FUN=colLast) %>%
      align.time(n=lengthPeriod2*60)
    
    xts_measures_period_agg <- merge(xts_measures_period_agg, xts_measures_period_agg_1)
    
    #adjust the time index
    index(xts_measures_period_agg) <- index(xts_measures_period_agg) - lengthPeriod2*60
    
    #Filter
    xts_measures_period_agg <- xts_measures_period_agg[,allColumnNames]
    
    xtsAttributes(xts_measures_period_agg)$RIC <- xtsAttributes(xts_measures)$RIC
    xtsAttributes(xts_measures_period_agg)$GMTOffset <- xtsAttributes(xts_measures)$GMTOffset
    xtsAttributes(xts_measures_period_agg)$EventType <- xtsAttributes(xts_measures)$EventType
    xtsAttributes(xts_measures_period_agg)$Market <- xtsAttributes(xts_measures)$Market
    xtsAttributes(xts_measures_period_agg)$XTSName <- paste0(xtsAttributes(xts_measures)$XTSName,"_measures","_",lengthPeriod, unitPeriod)
    
    
    aggmeasures_xts.lst[[length(aggmeasures_xts.lst)+1]] <<- xts_measures_period_agg
  })
  
  ### Saving the XTS object
  ###After that, the XTS object stock_data.xts could be saved in a **`.rds`** file for future use like any other standard R object. You can use the following code to do this.
  saveRDS(aggmeasures_xts.lst, file=filename_output)
  return(filename_output)
}

getAllTradecategories <- function(){
  qualifiersfilename <- "./TRTHQualifiers.csv"
  
  #Now we read the correspondence between qualifiers and trade categories from a csv file
  qua_tradecat_tbl <- read.csv(qualifiersfilename, header = T, sep = ',', stringsAsFactors = F)
  
  unique(qua_tradecat_tbl[,4])
}

getAllExchangeNames <- function(){
  c("ASX","CHA")
}

getAllTradeMeasureNames <- function() {
  c("TradeCount","ShareVolumeTraded","DollarVolumeTraded","VWAP")
}

getAllQuoteMeasureNames <- function() {
  c("WeightedQuotedSpread","WeightedPercentageSpread","WeightedDollarDepth","WeightedShareDepth")
}