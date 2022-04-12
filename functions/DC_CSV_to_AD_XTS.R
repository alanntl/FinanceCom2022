
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
  
  #modified by Kun Lu, 20200629, combine duplicate items in current qualifiers.
  unicurqualifiers <- unique(curqualifiers[,c("Qualifiers")])
  curqualifiers1 <- c()
  sapply(unicurqualifiers, function(uniqualifier){
    curqualifiers1 <<- rbind(curqualifiers1,
                    c(uniqualifier, paste(curqualifiers[curqualifiers[,"Qualifiers"]==uniqualifier,"Ex.Cntrb.ID"], collapse = " or ")))
  })
  curqualifiers <- curqualifiers1
  
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
dc_tick_csv_to_ad_trades_xts_helper <- function(tradeData, filename1){
  
  ### Creating the index
  ### The next step is to create the **index** of the XTS object, which can be derived from column *Date.Time* of `stockTickData`.
  
  #Extract the values of Date.Time column, store them in a vector named timeidx.
  timeidx <- tradeData[,"Date.Time"]
  #Use "UTC" as time zone
  timeidx <- as.POSIXct(timeidx, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
  
  ###The values of column *X.RIC* and *GMT.Offset* are always the same for all the trading records, so we extract them in the following code and will add them to the XTS object as attributes later.
  x_ric <- tradeData[1,"X.RIC"]
  gmt_offset <- tradeData[1,"GMT.Offset"]
  
  #And we will also add the following variables as attributes of the XTS object
  xts_name <- paste0(filename1,"_",x_ric)
  event_type <- "Intraday Measures"
  
  #add the Maket name as an new attribute of the XTS object
  tmparr <- strsplit(x_ric,"[.]")
  market_name <- tmparr[[1]][2]
  
  #currentqualifiers <- unique(tradeData[,"Qualifiers"])
  currentqualifiers <- unique(tradeData[,c("Qualifiers","Ex.Cntrb.ID")])
  qua_tradecat_hm <- create_qualifier_tradecategory_hashmap(curqualifiers=currentqualifiers)
  
  ### Creating core data
  ### Then we will use column *Price*, *Volume* and *Market.VWAP* of `stockTickData` to construct the **data** of the XTS object. We use fuction `select` in R package `dplyr` to extract all the columns except *X.RIC*, *Date.Time*, *GMT.Offset*, *Type* and *Domain*.
  tradeData <- tradeData %>%
    select(Price, Volume, Market.VWAP, Qualifiers, Ex.Cntrb.ID) %>%
    mutate(TradeCategory=qua_tradecat_hm[[Qualifiers]])
  
  #We add three attributes (RIC, GMTOffset, EventType and XTSName) in constructor xts(). 
  stock_data.xts <- xts(tradeData, order.by = timeidx, RIC=x_ric, GMTOffset=gmt_offset, EventType = event_type, Market = market_name, XTSName=xts_name)
  
  return(stock_data.xts)
  
}

dc_tick_csv_to_ad_trades_xts <- function(path, filename) {
  
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

stockTickData <- stockTickData %>% filter(Type=="Trade" & Price>0)

stockTickDatalist <- split(stockTickData, stockTickData$X.RIC)

library(xts)

### Creating the List which contains XTS objects
### We now create a List object to hold XTS objects so that we can store multiple XTS objects in one RDS file.
stock_xts.lst <- list()

lapply(stockTickDatalist, function(tickData){
  
  #modifed by Kun Lu, 20200628, remove "\"" from original Qualifers.
  tickData[,"Qualifiers"] <- gsub("\"","",tickData[,"Qualifiers"])
  
  stock_xts_obj <- dc_tick_csv_to_ad_trades_xts_helper(tradeData=tickData, filename1=filename)
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
  
  #add the Maket name as an new attribute of the XTS object
  tmparr <- strsplit(x_ric,"[.]")
  market_name <- tmparr[[1]][2]
  
  ### Creating core data
  ### Then we will use column *Price*, *Volume* and *Market.VWAP* of `stockTickData` to construct the **data** of the XTS object. We use fuction `select` in R package `dplyr` to extract all the columns except *X.RIC*, *Date.Time*, *GMT.Offset*, *Type* and *Domain*.
  quoteData <- quoteData %>%
    select(Bid.Price,Bid.Size,Ask.Price, Ask.Size, QuotedSpread, PercentageSpread, QuotedDollarDepth, QuotedShareDepth, Date.Time, TimeExisting)
    #select(Bid.Price,Bid.Size,Ask.Price, Ask.Size, QuotedSpread, PercentageSpread, QuotedDollarDepth, QuotedShareDepth, Date.Time, TimeExisting,'Time*QuotedSpread', 'Time*PercentageSpread', 'Time*DollarDepth', 'Time*ShareDepth')
  
  ### Constructing the XTS object
  ### With the core data, index and attributes, now we can use `xts` constructor to create the XTS object like this:
  #We add three attributes (RIC, GMTOffset, EventType and XTSName) in constructor xts(). 
  stock_data.xts <- xts(quoteData, order.by = order, RIC=x_ric, GMTOffset=gmt_offset, EventType = event_type, Market = market_name, XTSName=xts_name)
  
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
    
    #mutate("Time*QuotedSpread" = QuotedSpread*TimeExisting) %>%
    #mutate("Time*PercentageSpread" = PercentageSpread*TimeExisting) %>%
    #mutate("Time*DollarDepth" = QuotedDollarDepth*TimeExisting) %>%
    #mutate("Time*ShareDepth" = QuotedShareDepth*TimeExisting) %>%
    mutate("Bid.Size" = as.double(Bid.Size))%>%
    mutate("Ask.Size" = as.double(Ask.Size))
  
  quotes_detailed <- quotes_detailed %>%
    filter(as.double(QuotedSpread) >= 0)
  
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

  