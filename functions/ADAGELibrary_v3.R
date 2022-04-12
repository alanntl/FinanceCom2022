#########################################################################
# This is a consolidated library containing all ADAGE Processing programs
#########################################################################
library(xts)

#####################################
# MISC FUNCTIONS
#####################################


#Change NAs to 0s using this function
na.zero <- function (x) {
  x[is.na(x)] <- 0
  return(x)
}

library(tidyverse)
#Remove carets from index codes
removeCaret <- function (code) {
  if((str_sub(code,1,1))=='^')
  {return(str_sub(code,2,str_length(code)))}
  else{return(code)} 
}

extractMarket <- function(ric){
  result <- "Nothing"
  sub <- str_extract(ric,regex("\\.[A-Z]+"))
  n <- str_length(sub)
  if(is.na(sub)) {(result<-"US") }else{
    result <- str_sub(sub,2,str_length(sub))
  }
  return(result)
}

#######################################################################
#######################################################################
# IMPORT FUNCTIONS
#######################################################################
#######################################################################

#######################################################################
# YAHOO FINANCE TO ADAGE RDS 
########################################################################

library(quantmod)

YF_OL_to_AD_XTS <- function(stock_list,start_date,end_date){
  
  ts_list <- list()
  
  
  for (idx in seq(length(stock_list))){
    stock_index = stock_list[idx]
    ts_list[[idx]] <- get(removeCaret(getSymbols(stock_index, verbose = TRUE, src = "yahoo", 
                                     from=start_date,to=end_date)))
    xtsAttributes(ts_list[[idx]]) <- list(RIC=removeCaret(stock_index),
                                          GMTOffset=0,
                                          EventType="Daily Measures",
                                          Market=extractMarket(stock_index),
                                          XTSName=paste0("Yahoo Finance Daily Stock Prices for ",stock_index))
  }
  
  return(ts_list)
}

#######################################################################
# DATASCOPE TO ADAGE RDS 
# Author: Kun Lu
########################################################################

#These applications reqire a file TRTHQualifiers.csv in top directory!!!
#APPLICATION 1
#Name: Build ADAGE price timeseries from Datascope tick data
#define function which converts DC Tick data into important attributes of trades
#for now we have Price, Volume and Market.VWAP
#From GUI we have
#Input data = Datascope
#dataset structure= Tick History
#Input format=CSV
#Output format=RDS

create_qualifier_tradecategory_hashmap <- function(curqualifiers){
  library(hashmap)
  
  qualifiersfilename <- "./TRTHQualifiers.csv"
  
  #Now we read the correspondence between qualifiers and trade categories from a csv file
  qua_tradecat_tbl <- read.csv(qualifiersfilename, header = T, sep = ',', stringsAsFactors = F)
  
  #create a hashmap with qualifiers as keys and trade categories as values
  qua_tradecat_hm <- hashmap(qua_tradecat_tbl[,1], qua_tradecat_tbl[,4])
  
  #find out the qualifiers which do not exist in the csv file
  curqualifiers[,2] <- gsub("CHA","CHIX",curqualifiers[,2])
  notexistinglst <- curqualifiers[qua_tradecat_hm$has_keys(curqualifiers[,1])==FALSE,]
  
  if(dim(notexistinglst)[[1]]!=0){
    #output a warning message
    notexistingstr <- ""
    sapply(notexistinglst[,1], function(astr){
      notexistingstr <<- paste0(notexistingstr,astr,", ")
    })
    cat(paste0("Warning: found some qualifiers without a category type: ", notexistingstr,"\n"))
    
    #insert new qualifier lines into the csv file
    qua_tradecat_tbl <- rbind(qua_tradecat_tbl, 
                            data.frame(Qualifier=notexistinglst[,1], 
                                       Trade.type=rep("",dim(notexistinglst)[[1]]), 
                                       Market=notexistinglst[,2], 
                                       Abbreviation=notexistinglst[,1]))
    write.csv(qua_tradecat_tbl, file = qualifiersfilename, row.names=FALSE, quote=FALSE)
  
    #insert new key-value pairs into the hashmap
    qua_tradecat_hm$insert(notexistinglst[,1],notexistinglst[,1])
  }
  
  #return the hashmap
  qua_tradecat_hm
}

#20191209 Kun Lu, this helper function deals with trade records of each RIC, convert them
#into a XTS object and append the object to the list.
dc_tick_csv_to_ad_trades_xts_helper <- function(google = FALSE, tradeData, filename1){
  
  ### Creating the index
  ### The next step is to create the **index** of the XTS object, which can be derived from column *Date.Time* of `stockTickData`.
  ### (Modified by Alfred Lu)Use different parameter names in different condition
  if (google) {
    ti <- "Date_Time"
    ri <- "RIC"
    gm <- "GMT_Offset"
    ex <- "Ex_Cntrb_ID"
  } else {
    ti <- "Date.Time"
    ri <- "X.RIC"
    gm <- "GMT.Offset"
    ex <- "Ex.Cntrb.ID"
  }
  print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
  #Extract the values of Date.Time column, store them in a vector named timeidx.
  timeidx <- tradeData[,"Date_Time"]
  print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
  #Use "UTC" as time zone
  timeidx <- as.POSIXct(timeidx, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
  
  ###The values of column *X.RIC* and *GMT.Offset* are always the same for all the trading records, so we extract them in the following code and will add them to the XTS object as attributes later.
  x_ric <- tradeData[1, ri]
  gmt_offset <- tradeData[1, gm]
  
  #And we will also add the following variables as attributes of the XTS object
  xts_name <- paste0(filename1,"_",x_ric)
  event_type <- "Intraday Measures"
  
  #add the Maket name as an new attribute of the XTS object
  tmparr <- strsplit(x_ric, "[.]")
  market_name <- tmparr[[1]][2]
  
  #currentqualifiers <- unique(tradeData[,"Qualifiers"])
  currentqualifiers <- unique(tradeData[,c("Qualifiers", ex)])
  qua_tradecat_hm <- create_qualifier_tradecategory_hashmap(curqualifiers=currentqualifiers)
  
  ### Creating core data
  ### Then we will use column *Price*, *Volume* and *Market.VWAP* of `stockTickData` to construct the **data** of the XTS object. We use fuction `select` in R package `dplyr` to extract all the columns except *X.RIC*, *Date.Time*, *GMT.Offset*, *Type* and *Domain*.
  #tradeData <- tradeData %>%
  #  select(Price, Volume, Market.VWAP, Qualifiers, Ex.Cntrb.ID) %>%
  #  mutate(TradeCategory=qua_tradecat_hm[[Qualifiers]])
  ### Use different column names for different source of data
  if (google) {
    tradeData <- tradeData %>%
      select(Price, Volume, Market_VWAP, Qualifiers, Ex_Cntrb_ID) %>%
      mutate(TradeCategory=qua_tradecat_hm[[Qualifiers]])
  } else { 
    tradeData <- tradeData %>%
      select(Price, Volume, Market.VWAP, Qualifiers, Ex.Cntrb.ID) %>%
      mutate(TradeCategory=qua_tradecat_hm[[Qualifiers]])
  }
  
  #We add three attributes (RIC, GMTOffset, EventType and XTSName) in constructor xts(). 
  stock_data.xts <- xts(tradeData, order.by = timeidx, RIC=x_ric, GMTOffset=gmt_offset, EventType = event_type, Market = market_name, XTSName=xts_name)
  
  return(stock_data.xts)
  
}

dc_tick_csv_to_ad_trades_xts <- function(google, path, filename) {
  
#Add by Alfred Lu, Check data source
  if (google == 'TRUE') {
    google <- TRUE
  } else {
    google <- FALSE
  }
  
  #Check file existence
  filename_input <- paste0(path,paste0(filename),".csv")
  
  if(!file.exists(filename_input))
  {message(paste0(filename_input,': this file does not exist!'))
  }
  
  filename_output <- paste0(path,paste0(filename,".rds"))
  
  #Import the stock trading data from the csv file into a R object named stockTickData.
  
  #In read.csv(), we set the argument stringsAsFactors to F so that strings would not 
  #be converted into factors during importing, but remain in character vectors.
  stockTickData <- read.csv(filename_input, header = T, sep = ',', stringsAsFactors = F)
  
  #set the maximum number of digits to print to 6 when formatting time values 
  #in seconds, since the high frequency timeseries uses microsecond time indices.
  options(digits.secs=6)
  library(dplyr)
  
  #(Original)stockTickData <- stockTickData %>% filter(Type=="Trade" & Price>0)  
  #stockTickDatalist <- split(stockTickData, stockTickData$X.RIC)
  ### Modified By Alfred Lu
  if (google) {
    stockTickDatalist <- split(stockTickData, stockTickData$RIC)
  } else {
    stockTickData <- stockTickData %>% filter(Type=="Trade" & Price>0)
    stockTickDatalist <- split(stockTickData, stockTickData$X.RIC)
  }

  library(xts)
  
  ### Creating the List which contains XTS objects
  ### We now create a List object to hold XTS objects so that we can store multiple XTS objects in one RDS file.
  stock_xts.lst <- list()
  lapply(stockTickDatalist, function(tickData){
    
    #modifed by Kun Lu, 20200628, remove "\"" from original Qualifers.
    tickData[,"Qualifiers"] <- gsub("\"","",tickData[,"Qualifiers"])
    
    stock_xts_obj <- dc_tick_csv_to_ad_trades_xts_helper(google, tradeData=tickData, filename1=filename)
    stock_xts.lst[[length(stock_xts.lst)+1]] <<- stock_xts_obj
  })
  
  ### Saving the XTS object list
  saveRDS(stock_xts.lst, file=filename_output)
  return(filename_output)
}
## Example of usage
#dc_tick_csv_to_ad_trades_xts("/Users/redatawfiki/Documents/UNSW/Github/unsw-intradaymeasures-project/Code/ADAGE\ library/","DC_BHPAX_20190717_trade_partial")
##-------------------------------------------------------------------------------------------------------------------
#####################################################################################################################
#APPLICATION 2 (in DC to AD)
#Name: Build ADAGE quote timeseries from Datascope tick data
#From GUI we have
#Input datasource = Datascope
#dataset structure= Tick History
#Input format=CSV
#Output dastasource=Adage
#Output format=RDS

#20191209 Kun Lu, this helper function deals with quote records of each RIC, convert them
#into a XTS object and append the object to the list.
dc_tick_csv_to_ad_quotes_xts_helper <- function(quoteData, filename1){
  
  #Generate the index
  order = quoteData[,"Date.Time"]
  
  ###The values of column *X.RIC* and *GMT.Offset* are always the same for all the trading records, so we extract them in the following code and will add them to the XTS object as attributes later.
  x_ric <- quoteData[1,"X.RIC"]
  gmt_offset <- quoteData[1,"GMT.Offset"]
  
  #And we will also add the following variables as attributes of the XTS object
  xts_name <- paste0(filename1,"_",x_ric)
  event_type <- "Intraday Measures"
  
  ### Creating core data
  ### Then we will use column *Price*, *Volume* and *Market.VWAP* of `stockTickData` to construct the **data** of the XTS object. We use fuction `select` in R package `dplyr` to extract all the columns except *X.RIC*, *Date.Time*, *GMT.Offset*, *Type* and *Domain*.
  quoteData <- quoteData %>%
    select(Bid.Price,Bid.Size,Ask.Price, Ask.Size, TimeExisting, 'Time*QuotedSpread', 'Time*PercentageSpread', 'Time*DollarDepth', 'Time*ShareDepth')
  
  ### Constructing the XTS object
  ### With the core data, index and attributes, now we can use `xts` constructor to create the XTS object like this:
  #We add three attributes (RIC, GMTOffset, EventType and XTSName) in constructor xts(). 
  stock_data.xts <- xts(quoteData, order.by = order, by=0.1, RIC=x_ric, GMTOffset=gmt_offset, EventType = event_type, XTSName=xts_name)
  
  return(stock_data.xts)
  
  #xts_quotes = xts(quotes_detailed[,c("Bid.Price","Bid.Size","Ask.Price", "Ask.Size", "TimeExisting", "Time*QuotedSpread", "Time*PercentageSpread", "Time*DollarDepth", "Time*ShareDepth")], order.by=order, by=0.1)
  
}

dc_tick_csv_to_ad_quotes_xts <- function(path, filename) {
  
  #Check file existence
  filename_input <- paste0(path,paste0(filename,".csv"))
  
  if(!file.exists(filename_input))
  {message(paste0(filename_input,': this file does not exist!'))
  }
  
  filename_output <- paste0(path,paste0(filename,"_q.rds"))
  
  #Import the stock trading data from the csv file into a R object named stockTickData.
  
  #In read.csv(), we set the argument stringsAsFactors to F so that strings would not 
  #be converted into factors during importing, but remain in character vectors.
  stockTickData <- read.csv(filename_input, header = T, sep = ',', stringsAsFactors = F)
  
  library(dplyr)
  # Calculating quote-based measures
  quotes <- stockTickData %>%
    filter(Type == "Quote") 
  
  quotes_detailed <- quotes %>%
    mutate("QuotedSpread" = Ask.Price - Bid.Price) %>%
    mutate("PercentageSpread" = (Ask.Price - Bid.Price)/((Ask.Price + Bid.Price)/2)) %>%
    mutate("QuotedDollarDepth" = (Bid.Size*Bid.Price + Ask.Size*Ask.Price)/2) %>%
    mutate("QuotedShareDepth" = (Bid.Size + Ask.Size)/2) %>%
    mutate("Date.Time" = as.POSIXct(Date.Time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")) %>%
    mutate("TimeExisting" = as.double(difftime(lead(Date.Time), Date.Time))) %>%
    mutate("Time*QuotedSpread" = QuotedSpread*TimeExisting) %>%
    mutate("Time*PercentageSpread" = PercentageSpread*TimeExisting) %>%
    mutate("Time*DollarDepth" = QuotedDollarDepth*TimeExisting) %>%
    mutate("Time*ShareDepth" = QuotedShareDepth*TimeExisting) %>%
    mutate("Bid.Size" = as.double(Bid.Size))%>%
    mutate("Ask.Size" = as.double(Ask.Size))
  
  
#Need to replace all above by formulas
#  QuotedSpreadCalc <- function (frame){
#    with(frame,Ask.Price-Bid.Price)
#  }
  
#  quotes_detailed2 = quotes %>%
#    mutate("QuotedSpread" = QuotedSpreadCalc(quotes))
  
  options(digits.secs=6)
  
  stockTickDatalist <- split(quotes_detailed, quotes_detailed$X.RIC)
  
  library(xts)
  
  ### Creating the List which contains XTS objects
  ### We now create a List object to hold XTS objects so that we can store multiple XTS objects in one RDS file.
  stock_xts.lst <- list()
  
  lapply(stockTickDatalist, function(tickData){
    stock_xts_obj <- dc_tick_csv_to_ad_quotes_xts_helper(quoteData=tickData, filename1=filename)
    stock_xts.lst[[length(stock_xts.lst)+1]] <<- stock_xts_obj
  })
  
  ### Saving the XTS object list
  saveRDS(stock_xts.lst, file=filename_output)
  return(filename_output)
}
## Example of usage
#dc_tick_csv_to_ad_quotes_xts(".\\","DC_6StocksAUX_20190717_trade_2hours")
#sixStocks_q <- readRDS("DC_6StocksAUX_20190717_trade_2hours_q.rds")
#firstStock_q <- sixStocks_q[[1]]
#dc_tick_csv_to_ad_trades_xts(".\\","DC_6StocksAUX_20190717_trade_2hours")
# sixStocks <- readRDS("DC_6StocksAUX_20190717_trade_2hours.rds")
# firstStock <- sixStocks[[1]]
#=======
#}
## Example of usage
#<<<<<<< Updated upstream
#dc_csv_to_ad_xts("C:\\Users\\fethi\\Documents\\gitHubUNSW\\unsw-intradaymeasures-project\\Code\\ADAGE library\\","DC_BHPAX_20190717_trade_partial")
#=======
#dc_tick_csv_to_ad_trades_xts("/Users/redatawfiki/Documents/UNSW/Github/unsw-intradaymeasures-project/Code/ADAGE\ library/","DC_BHPAX_20190717_trade_partial")
#>>>>>>> Stashed changes
#>>>>>>> Stashed changes
##-------------------------------------------------------------------------------------------------------------------



#################################################################
#################################################################
## AGGREGATING ADAGE TIMESERIES
# Author Kun Lu
#################################################################
#################################################################
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

aggreFunLst2 <- function(x) {
  c(data.frame(coredata(x))%>%nrow(),
    mean(x$Price),
    tail(x$Price,1))
}

colLast <- function(x) {
  tail(x,1)
}

calAggreMeasures <- function(google, stockprices_xts, ExchangeCode, tradeType, lengthPeriod, unitPeriod) {
  
  if (google == TRUE) {
    #modified by Alfred Lu
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
    
    #modifed by Kun Lu, 20201223, exclude the rows where the value of Market.VWAP is NA 
    #and the value of Qualifiers contains "Off Book Trades".
    stockprices_xts <- stockprices_xts[!grepl(".*Off Book Trades.*",stockprices_xts$Qualifiers) | 
                                         !is.na(stockprices_xts$Market.VWAP),]
    
    intrameasures_xts <- calAggreMeasures(google,stockprices_xts,"all","all",lengthPeriod, unitPeriod)
    
    #modifed by Kun Lu, 20200628, calculate measures only regarding current exchanges and tradeCategories in the trading data.
    #exchangeCodes <- list("ASX","CHA")
    #(original)exchangeCodes <- unique(stockprices_xts[,"Ex.Cntrb.ID"])
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
        #(Original)tradetypes <- unique(stockprices_xts[stockprices_xts[,"Ex.Cntrb.ID"]==exchangeCode,"TradeCategory"])
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
        #modified by Alfred Lu, 20211111
        intrameasures_atradetype.xts <- calAggreMeasures(google,stockprices_xts, "all", tradetype,lengthPeriod, unitPeriod)
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


ad_xts_rds_to_ad_aggindexmeasures_xts <- function(path, filename,lengthPeriod,unitPeriod) {
  
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
  
  stockmeasures_xts.lst <- list()
  
  lapply(stock_xts.lst, function(stockprices_xts){
    
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
    intrameasures_xts <- period.apply(stockprices_xts, INDEX=period_lenthp,FUN=aggreFunLst2) %>% align.time(n=aligntimeoffset)
    
    colnames(intrameasures_xts) <- c("Count","AveragePrice","LstPrice")
    
    
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


#######################################################################
# COMPUTING RETURNS 
########################################################################

#This function takes as arguments
# returnMeasure: a string containing name of return measure to be computed
#                if = "AReturn", arithmetic returns need to be computed
# col: a column containing the prices

computeReturns <- function (returnMeasure,col,col_index = NULL){
  p_now <- (col[-(1)])
  p_prev <-(col[-length(col)])
  # AReturn measure: Arithmetic return
  if (returnMeasure=="AReturn"){
    result <- c(NA,(p_now - p_prev ) / p_now)
  }
  # LReturn measure: Log return
  else if(returnMeasure=="LReturn"){
    result <- c(NA, log(p_now/p_prev))
  }
  # AbReturn measure: Abnormal return
  else if(returnMeasure=="AbReturn"){
    result <- col - col_index
  }
  else message("Return measure not implemented:",returnMeasure)
  return(result)
}

#adds a number of returns columns to the timeseries
#takes as parameters
# ts: object containing the timeseries
# returnMeasure: the name of the measure to be computed
# prices: the names of the price columns that need to be used to compute returns
#
addReturnsColumn <- function(ts,returnMeasure,prices){
  frame <- coredata(ts)
  for (i in (1:length(prices))){
    price <- prices[i]
    #Find a column that has the name equal to variable price
    x <- grep(price, names(ts), value=TRUE)
    col <- frame[,x]
    returns <- computeReturns(returnMeasure,as.double(col))
    #add the new column to the frame
    frame_result <- cbind(frame,AReturn_Close=returns)
    #rename last column inserted
    colnames(frame_result)[ncol(frame_result)] <- paste0(paste0(returnMeasure,"."),price)
    frame <- frame_result
  }
  return(frame_result)
}

#adds column to all timeseries in an ADAGE object
addMeasure_ADAGE <- function(daily_prices,returnMeasure,price,col_index = NULL){
  
  ts_out <- list()
  
  for (idx in 1:length(daily_prices)){
    ts<- daily_prices[[idx]]
    if(returnMeasure == "AbReturn"){
      # ts$AbReturn <- ts[,col2]-ts[,col1]
      ts$AbReturn <- computeReturns("AbReturn",ts[,price],ts[,col_index])
      ts_out[[idx]] <- xts(ts,order.by = index(ts))
    }
    else{
      ts_result <- addReturnsColumn(ts,returnMeasure,price)
      ts_out[[idx]] <-  xts(ts_result,order.by = index(ts))
    }
    
    #copy attributes
    xtsAttributes(ts_out[[idx]])$RIC <- xtsAttributes(ts)$RIC
    xtsAttributes(ts_out[[idx]])$GMTOffset <- xtsAttributes(ts)$GMTOffset
    xtsAttributes(ts_out[[idx]])$EventType <- xtsAttributes(ts)$EventType
    xtsAttributes(ts_out[[idx]])$Market <- xtsAttributes(ts)$Market
    xtsAttributes(ts_out[[idx]])$XTSName <- xtsAttributes(ts)$XTSName 
    
  }
  return(ts_out)
}

#Example: ts_withreturns <- addMeasure_ADAGE(ts,c("AReturn"),c("VWAP"))

########################################
# BUILD EVENT DATASET
# Author: Weisi Chen
########################################

BuildEventStudyDataset <- function(input1, input2, WindowSize, EstimationWindowSize = 0){
  #BuildEventStudyDataset takes 3 arguments and works as follows
  
  #Starts with an empty event dataset
  result_list <- list()
  
  # The total period needed = (-(WindowSize+EstimationWindowSize),WindowSize) by default.
  totalWindowSize <- WindowSize + EstimationWindowSize
  
  # for each ric in input2 list, filter input1 for timeseries whose RIC is equal to ric
  #                (the test is "xtsAttributes(xts_object)$RIC==ric")
  #                we call ts_list the list of event dates associated with ric 
  #               we call all filtered timeseries input1_filtered
  # in the example, the first RIC is "BHP.AX", 
  #                 ts_list=list(as.Date("01/03/2020")) 
  #                 and input1_filtered contains the BHP.AX xts object
  #
  #    for each ts in ts_list
  #    do
  #      find an xts_object in input1_filtered which satisfies 2 conditions
  #      1. ts has to be one of the indices in index(xts_object), at line position FoundLineNb
  #      2. There has to be WindowSize indices before and after FoundLineNb
  #    end do
  #
  #    if xts_object could not be found, then raise an error
  #    else change xts_object as follows:
  #          1. Remove all rows except WindowSize rows before and after the row FoundLineNb
  #          2. Add new column, first row will contain (-WindowSize) and last row WindowSize
  #             (FoundLineNb will have value 0)
  #   add(xts_object) to result_list
  # end for
  
  for(i in seq(length(input2))){
    # input1_filtered: for each RIC in input2, input1 is filtered by the current RIC.
    input1_filtered <- Filter(function(x) xtsAttributes(x)$RIC == input2[[i]][[1]], input1)
    
    # The current RIC in input2 doesn't exist in input1.
    if(length(input1_filtered) == 0) 
      message(paste0("Warning: RIC is not found in the first input time series: ", input2[[i]][[1]]))
    else{
      # Removing the first item of input2[[1]] (RIC), to get the event dates.
      ts_list <- tail(input2[[i]], -1)
      for(ts in ts_list){
        eventdate <- as.Date(ts)
        eventdatefound <- FALSE
        # Event date is not found in input1
        if(nrow(input1_filtered[[1]][eventdate]) == 0){
          # Looping for the following 10 days to find the closest working day after the specified event date.
          for(j in 1:10){
            if(nrow(input1_filtered[[1]][eventdate + j]) > 0){
              message(paste0("Warning: The original event date ", ts, " for RIC ", input2[[i]][[1]], " is not found in the time series. Using ", j, " day(s) after it."))
              eventdate <- eventdate + j
              eventdatefound <- TRUE
              break # the closest day is found - break the current loop.
            }
          }
          if(eventdatefound <- FALSE){
            message(paste0("Warning: Event date as well as 1 to 10 days after is not found in the time series. Please check your data.\nRIC: ", input2[[i]][[1]], ", Event date: ", ts))
          }
        }
        print(paste0("Event Date for RIC ", input2[[i]][[1]], " is ", eventdate))
        # Sufficient data is found before the event date.
        if(nrow(input1_filtered[[1]][paste0("/",eventdate)])>=totalWindowSize
           # Sufficient data is found after the event date.
           & nrow(input1_filtered[[1]][paste0(eventdate,"/")])>=WindowSize){ 
          input1_filtered_with_event <- c(xts::last(input1_filtered[[1]][paste0("/",eventdate)],totalWindowSize+1),
                                          xts::first(input1_filtered[[1]][paste0(eventdate+1,"/")],totalWindowSize))
          input1_filtered_with_event$EventDateRef <- -totalWindowSize:totalWindowSize
          result_list <- c(result_list,list(input1_filtered_with_event))
        }
        else{
          message(paste0("Insufficient data for RIC: ", input2[[i]][[1]], ", Event date: ", ts))
        }
          
      }
    }
  }
  return(result_list)
}
################
#function mergeEventDataset
# input 1 will be an event set with list of stocks with a column containing returns called return1Col
# input 2 will be an event set with list of indices with a column containing returns called returns2Col
# newCol is a list of column names to be copied from input 2
# we assume input 2 has only 1 timeseries

#the output will be same as input 1 but all columns in newCol have been added to each input2_dataset i
#each new column will be prefixed with xtsAttributes(input2_dataset1)$RIC

mergeEventDataset <- function(input1, input2, newCol){
  if(length(input1) == 0 | length(input2) == 0)
    message(paste0("Warning: at least one of the input time series is empty!"))
  else{
    for(i in 1:length(input2)){
      input2_newCol <- input2[[i]][,newCol]
      colnames(input2_newCol) <- paste0(xtsAttributes(input2[[i]])$RIC,".",newCol)
      for(j in 1:length(input1)){
        input1[[j]] <- merge(input1[[j]],input2_newCol,join='left')
      }
    }
  }
  return(input1)
}

#We need a new version where the length of input1 is same as length of input2
#           Otherwise, an error will be raised
# One column from input_2[i] will be added to input_1[i]
# For example
# input1 conatins 2 companies at dates d1 and d2
# input2 conatins same index at dates d1 and d2
# merge produces: company1 and index at date d1
#                 company2 and index at date d2

#######################################################################
#Helper functions for Perform event study

#adds column to all timeseries in an ADAGE object
addExpectedReturns_ADAGE <- function(adagexts,returnCompany,returnIndex,estimationWindow){
  
  ts_out <- list()
  
  for (idx in 1:length(adagexts)){
    ts<- adagexts[[idx]]
    newts <- computeExpectedReturns(ts,returnIndex,(ts[,returnCompany])[1:estimationWindow],
                                    (ts[,returnIndex])[1:estimationWindow])
    ts_out[[idx]] <-  newts
    #copy attributes
    xtsAttributes(ts_out[[idx]])$RIC <- xtsAttributes(ts)$RIC
    xtsAttributes(ts_out[[idx]])$GMTOffset <- xtsAttributes(ts)$GMTOffset
    xtsAttributes(ts_out[[idx]])$EventType <- xtsAttributes(ts)$EventType
    xtsAttributes(ts_out[[idx]])$Market <- xtsAttributes(ts)$Market
    xtsAttributes(ts_out[[idx]])$XTSName <- xtsAttributes(ts)$XTSName 
  }
  
  return(ts_out)
}

#This function creates a new column in the timeseries for expected returns
computeExpectedReturns <- function(ts,returnIndexColName,compReturns,indexReturns){
  
  
  #Create a model for estimating compReturns based on indexReturns
  model <- lm(compReturns~indexReturns)
  message("Model generated for company ",xtsAttributes(ts)$RIC)
  print(model)
  
  #Extract coefficients
  intercept <- summary(model)$coefficients[1,1]
  slope <- summary(model)$coefficients[2,1]
  
  #Use the model to create predictions
  #frame <- coredata(ts)
  ts <- addExpReturnsColumn (ts,returnIndexColName,intercept,slope)
  
  return(ts)
}

addExpReturnsColumn <- function(ts,indexPriceColName,intercept,slope){
  frame <- coredata(ts)
  #Find a column that has the name equal to variable price
  x <- grep(indexPriceColName, names(ts), value=TRUE)
  col <- frame[,x]
  newCol <- col * slope + intercept
  #add the new column to the frame
  frame_result <- cbind(frame,Return.Expected=newCol)
  new_ts <-  xts(frame_result,order.by = index(ts))
  #copy attributes
  xtsAttributes(new_ts)$RIC <- xtsAttributes(ts)$RIC
  xtsAttributes(new_ts)$GMTOffset <- xtsAttributes(ts)$GMTOffset
  xtsAttributes(new_ts)$EventType <- xtsAttributes(ts)$EventType
  xtsAttributes(new_ts)$Market <- xtsAttributes(ts)$Market
  xtsAttributes(new_ts)$XTSName <- xtsAttributes(ts)$XTSName 
  return(new_ts)
}

#Create average abnormal returns
computeAvAbRets <- function(adagets)
{
  result <- data.frame(matrix(ncol=length(adagets), nrow=nrow(adagets[[1]])))
  for (idx in seq(length(adagets))){
    df <- coredata(adagets[[idx]])
    result[,idx] <- df[,"AbReturn"]
    colnames(result)[idx] <- paste0("AbReturn.",idx)
  }
  result_df <- result
  #for (idx in seq(nrow(df))){
  #  names(result_df)[idx] <- paste0("AbReturn.",idx)
  #}
  result_df$AbReturn.Mean <- apply(result_df,MARGIN=1,FUN=mean)
  result_df$AbReturn.EventDateRef <- (adagets[[1]])$EventDateRef
  return(result_df)
}
#######################################################################
#######################################################################
# EXPORT FUNCTIONS
#######################################################################
#######################################################################

################################################################
# INSPECT ADAGE FILES
#################################################################

inspectADAGEFile <- function(result,i)
{
#Display information about ith timeseries in result
  #$ RIC: company code un RIC format
  #$ GMTOffset: GMT Offset, needs to be added to get local time
  #$ EventType: can be "Intraday Measures", "Daily Measures" etc.
  #$ Market: represents the market (using Reuters codes)
  #$ XTSName: name of the timeseries
ts1 <- result[[i]]
message("Information on timeseries ",i," in file ",xtsAttributes(ts1)$XTSName)
message("The type of the time index=",class(index(ts1)))
message("RIC=",xtsAttributes(ts1)$RIC)
message("GMT Offset=",xtsAttributes(ts1)$GMTOffset)
message("EventType=",xtsAttributes(ts1)$EventType)
message("Number of observations=",length(ts1))
message("The data frame has column names=")
colnames(coredata(ts1))
}

################################################################
# CONVERT ADAGE TO CSV
# Author: Kun Lu
#################################################################
#Input datasource = Adage
#input dataset structure = Any
#input format = RDS
#Output datasource = Adage
#Output format = CSV

ad_xts_to_ad_csv <- function(path, filename) {
  library(xts)
  xtsobj.lst<- readRDS(paste0(path,filename))
  print(length(xtsobj.lst))
  csvFolderPath <- paste0(path,sub('.rds','',filename),"-csv")
  
  if(dir.exists(csvFolderPath)==TRUE){
    # get all files in the directories, recursively
    #f1 <- list.files(csvFolderPath, include.dirs = F, full.names = T, recursive = T)
    # remove the files
    #file.remove(f1)
    #}else{
    unlink(csvFolderPath, recursive = TRUE, force = TRUE)
  }
  
  dir.create(csvFolderPath)
  lapply(xtsobj.lst,FUN=xtsRds2Csv,path1=paste0(csvFolderPath,"/"))
  originalPath <- getwd()
  setwd(path)
  #On Mac/Linux
  #zip(zipfile = paste0(sub('.rds','',filename),"-csv"), files = paste0(sub('.rds','',filename),"-csv"))
  #On Windows
  #zip(zipfile = paste0(sub('.rds','',filename),"-csv"), files = paste0(sub('.rds','',filename),"-csv"), flags = " a -tzip", zip = "C:\\Program Files\\7-Zip\\7Z")
  
  library(zip)
  zip::zipr(zipfile = paste0(sub('.rds','',filename),"-csv.zip"), files = paste0(sub('.rds','',filename),"-csv"))
  setwd(originalPath)
  unlink(csvFolderPath, recursive = TRUE, force = TRUE)
  
  xtsobj <- xtsobj.lst[[1]]
  data.frame(RIC=xtsAttributes(xtsobj)$RIC, Market=xtsAttributes(xtsobj)$Market, EventType=xtsAttributes(xtsobj)$EventType, Datetime=index(xtsobj), GMTOffset=xtsAttributes(xtsobj)$GMTOffset, xtsobj, row.names=NULL)
}


#helper function
xtsRds2Csv <- function(xtsobj, path1) {
  dframe <- data.frame(RIC=xtsAttributes(xtsobj)$RIC, Market=xtsAttributes(xtsobj)$Market, EventType=xtsAttributes(xtsobj)$EventType, Datetime=index(xtsobj), GMTOffset=xtsAttributes(xtsobj)$GMTOffset, xtsobj, row.names=NULL)
  #write.csv(dframe, file= paste0(path1,xtsAttributes(xtsobj)$XTSName,".csv"), row.names = F)
  #20210117, Kun Lu, remove any punctuations from the file name.
  write.csv(dframe, file= paste0(path1,gsub("[:punct:]", "", paste0(xtsAttributes(xtsobj)$XTSName, " ", rnorm(1))), ".csv"), row.names = F)
}
################################################################
# VISUALIZATION
#################################################################
#VIEW SINGLE MEASURE
viewMeasure <- function(measureName,xLabName,isAcc,idx,vec){
  AccMeasure=as.double(cumsum(na.zero(vec)))
  if (isAcc) {Measure<-xts(AccMeasure,order.by = index(vec))} else {Measure<-vec}
  message("Length of x axis=",length(idx))
  message("Length of y axis=",length(Measure))
  title <- ifelse(isAcc,
                  paste0(paste0("Cumulated ",measureName)," over period"),
                  paste0(measureName," over period"))
  yLab <- ifelse(isAcc,
                  paste0("Accumulated ",measureName),
                  measureName)
  plot.default(x=idx,
               y=Measure,
               type = "l",
               col="red",
               xlab=xLabName,
               ylab = measureName,
               main=title)
  legend(idx[1], coredata(Measure)[length(Measure)], legend=c(xtsAttributes(vec)$RIC),
         col=c("red"), lty=1:2, cex=0.8)
}

#Example 1: viewMeasure("VWAP","Date",FALSE,index(ts1),ts1$VWAP)
#Example 1: viewAccMeasure("VWAP","Date",TRUE,index(ts1_new),ts1_new$AReturn.VWAP)
#################################################################
# CREATING A TIMELINE FROM A NEWS FILE

#import libraries
library(ggplot2)
library(scales)
library(lubridate)

createTimeline <- function(data1,timeline_start,timeline_end){
  #data1 is a frame with 3 columns:
  # start_date : date
  # event: text in time line
  # displ: heights
  #timeline_start and timeline_end
  
  library(ggplot2)
  library(dplyr)
  library(ggalt)
  library(cowplot)
  library(tibble)
  library(lubridate)
  
  #Function to shift x-axis to 0 adapted from link shown above
  
  shift_axis <- function(p, xmin, xmax, y=0){
    g <- ggplotGrob(p)
    dummy <- data.frame(y=y)
    ax <- g[["grobs"]][g$layout$name == "axis-b"][[1]]
    p + annotation_custom(grid::grobTree(ax, vp = grid::viewport(y=1, height=sum(ax$height))), 
                          ymax=y, ymin=y) +
      annotate("segment", y = 0, yend = 0, x = xmin, xend = xmax, 
               arrow = arrow(length = unit(0.1, "inches"))) +
      theme(axis.text.x = element_blank(), 
            axis.ticks.x=element_blank())
    
  }
  
  #Conditionally set whether text will be above or below the point
  vjust = ifelse(data1$displ > 0, -1, 1.5)
  
  #plot
  p1 <- data1 %>% 
    ggplot(aes(start_date, displ)) +
    geom_lollipop(point.size = 1) +
    geom_text(aes(x = start_date, y = displ, label = event), data = data1,
              hjust = 0, vjust = vjust, size = 2.5) +
    theme(axis.title = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank(),
          axis.line = element_blank(),
          axis.text.x = element_text(size = 8)) +
    expand_limits(x = c(timeline_start, timeline_end), y = 1.2) +
    scale_x_date(breaks = scales::pretty_breaks(n = 9))
  
  #and run the function from above
  #timeline <- shift_axis(p1, ymd(20151201), ymd(20180501))
  timeline <- shift_axis(p1, timeline_start, timeline_end)
  return(timeline)
}
#########################################################################################
#  NEWS PROCESSING
#########################################################################################

#COMPUTING Sentiment scores

library(sentimentr)

addSentimentScoresFromSentimentR <- function(df,text_vec){
  #1. Computing sentiment using Sentiwordnet and add it to original frame
  sentiword_df <- sentiment_by(as.character(text_vec),polarity_dt = lexicon::hash_sentiment_sentiword)
  df$sentiwordnet.sd <- sentiword_df$sd
  df$sentiwordnet.ave_sentiment <- sentiword_df$ave_sentiment
  #2. Computing sentiment using Jockers and add it to original frame
  jockers_df <- sentiment_by(as.character(text_vec),polarity_dt = lexicon::hash_sentiment_jockers)
  df$jockers.sd <- jockers_df$sd
  df$jockers.ave_sentiment <- jockers_df$ave_sentiment
  #3. Computing sentiment using nrcnet and add it to original frame
  nrc_df <- sentiment_by(as.character(text_vec),polarity_dt = lexicon::hash_sentiment_nrc)
  df$nrc.sd <- nrc_df$sd
  df$nrc.ave_sentiment <- nrc_df$ave_sentiment
  #4. Computing sentiment using huliu and add it to original frame
  huliu_df <- sentiment_by(as.character(text_vec),polarity_dt = lexicon::hash_sentiment_huliu)
  df$huliu.sd <- huliu_df$sd
  df$huliu.ave_sentiment <- huliu_df$ave_sentiment
  #5. Computing sentiment using Loughran and McDonald Dictionary and add it to original frame
  loughran_df <- sentiment_by(as.character(text_vec),polarity_dt = lexicon::hash_sentiment_loughran_mcdonald)
  df$loughran.sd <- loughran_df$sd
  df$loughran.ave_sentiment <- loughran_df$ave_sentiment
  return(df)
}
##########################################################################################
### READ AFR News
###

library(xml2)

# Each news item has the following head:
# <head>
#   <NEWSPAPERCODE>FIN</NEWSPAPERCODE>
#   <SECTIONCODE>CM</SECTIONCODE>
#   <SECTION>Companies and Markets</SECTION>
#   <SUBSECTION/>
#   <SUBSECTIONCODE/>
#   <STREAMS>print</STREAMS>
#   <STORYNAME/>
#   <COPYRIGHT/>
#   <STORYNAME/>
#   <PUBLICATIONDATE>20200131</PUBLICATIONDATE>
#   <NEWSPAPER>Australian Financial Review</NEWSPAPER>
#   <EDITION>First</EDITION>
#   <PAGENO>21</PAGENO>
#   </head>

#extracting
extract_AFR_field <- function (docs,xpath){
  nodeset=lapply(docs,function (x) xml_find_first(x,xpath))
  return(lapply(nodeset,xml_text))
}

#adds a column to the frame
#news attributes in vector nattrib
#
createFrame <- function(news,nattribs){
  #initialise the frame with first column
  frame <- data.frame(unlist(extract_AFR_field(news,nattribs[1])))
  colnames(frame)[1] <- nattribs[1]
  #add rest of columns
  for (i in (2:length(nattribs))){
    nattrib <- nattribs[i]
    new_col <- unlist(extract_AFR_field(news,nattrib))
    frame <- cbind(frame,newcol=new_col)
    #rename last column inserted
    colnames(frame)[ncol(frame)] <- nattrib
  }
  return(frame)
}



AFRNews_XML_to_AD_XTS <- function(filename,nattribs){
  #reading the news
  x <- read_xml(filename)
  news <- xml_find_all(x,"//dcdossiers/dcdossier")
  
  #building the dataframe
  afr <- createFrame(news,nattribs)
  
  dates <- lapply(extract_AFR_field(news,"document/head/PUBLICATIONDATE"),
                  function (x) as.Date(x,format="%Y%m%d"))
  dates_v <- unlist(dates)
  
  #creating the xts object
  res_xts <- xts(afr,order.by=as.Date(dates_v))
  #Adding the attributes
  xtsAttributes(res_xts) <- list(RIC="Undefined",
                                 GMTOffset=0,
                                 EventType="News",
                                 Market="AU",
                                 XTSName="AFR News")
  
  
  return(list(res_xts))
}




