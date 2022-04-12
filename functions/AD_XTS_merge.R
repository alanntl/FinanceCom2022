#Input arguments:
#-ADAGE XTS File  (containing ts1, ts2 with RIC1 and RIC2)
#-join=[DEFAULT,inner,left,right]
#-fill=N/A or value
#-Suffix1=string (default “”)
#-Suffix2=string (default=””)
#-RIC=string (default RIC1:RIC2)

Merge2_ad_xts <- function(path, filename, join="DEFAULT", fill="N/A", Suffix1="", Suffix2="", RIC) {
  library(xts)
  xtsobj.lst<- readRDS(paste0(path,filename,".rds"))
  
  if(length(xtsobj.lst)!=2)
    stop(paste0("The input file ", filename, ".rds must contain 2 timeseries."))
  
  if(xtsAttributes(xtsobj.lst[[1]])$EventType!=xtsAttributes(xtsobj.lst[[2]])$EventType)
    stop(paste0("The two timeseries must have the same event type."))
  
  if(xtsAttributes(xtsobj.lst[[1]])$GMTOffset!=xtsAttributes(xtsobj.lst[[2]])$GMTOffset)
    stop(paste0("The two timeseries must have the same GMT offset."))
  
  if(join=="DEFAULT")
    joinstr <- "outer"
  else
    joinstr <- join
  
  if(fill=="N/A")
    fillnum <- NA
  else
    fillnum <- as.numeric(fill)
  
  colnames(xtsobj.lst[[1]]) <- paste0(colnames(xtsobj.lst[[1]]),Suffix1)
  colnames(xtsobj.lst[[2]]) <- paste0(colnames(xtsobj.lst[[2]]),Suffix2)
  
  xts_merged <- merge(xtsobj.lst[[1]], xtsobj.lst[[2]], join=joinstr, fill=fillnum)
  
  xtsAttributes(xts_merged)$RIC <- RIC
  xtsAttributes(xts_merged)$GMTOffset <- xtsAttributes(xtsobj.lst[[1]])$GMTOffset
  xtsAttributes(xts_merged)$EventType <- "Intraday Measures"
  xtsAttributes(xts_merged)$Market <- "AUX"
  xtsAttributes(xts_merged)$XTSName <- paste0("0_",RIC,"_measures")
  
  stockmeasures_xts.lst <- list()
  stockmeasures_xts.lst[[1]] <- xts_merged
  
  filename_output <- paste0(path,filename,"_merged.rds")
  saveRDS(stockmeasures_xts.lst, file=filename_output)
  return(filename_output)
}


#Input arguments:
#-ADAGE XTS File 1  (containing ts1_1, ts1_2 … ts1_n with RIC1_1 and RIC1_2 … RIC1_n)
#-ADAGE XTS File 2  (containing ts2_1, ts2_2 … ts2_n with RIC2_1 and RIC2_2 … RIC2_n)
#-join=[DEFAULT,inner,left,right]
#-fill=N/A or value
#-Suffix1=string (default “”)
#-Suffix2=string (default=””)
#-RIC must be list of strings (of size n) or just ""
Merge_ad_xts_with_ad_xts <- function(path, filename1, filename2, join="DEFAULT", fill="N/A", Suffix1="", Suffix2="", RIC="") {
  library(xts)
  xtsobj1.lst<- readRDS(paste0(path,filename1,".rds"))
  xtsobj2.lst<- readRDS(paste0(path,filename2,".rds"))
  
  
  if(length(xtsobj1.lst)!=length(xtsobj2.lst))
    stop(paste0("The two input RDS files must contain the same number of timeseries."))
  
  for(i in 1: length(xtsobj1.lst)){
    if(xtsAttributes(xtsobj1.lst[[i]])$EventType!=xtsAttributes(xtsobj2.lst[[i]])$EventType){
      stop(paste0("Timeseries ",i," in both lists must have the same event type."))
    }
  }
    
  for(i in 1: length(xtsobj1.lst)){
    if(xtsAttributes(xtsobj1.lst[[i]])$GMTOffset!=xtsAttributes(xtsobj2.lst[[i]])$GMTOffset){
      stop(paste0("Timeseries ",i," in both lists must have the same GMT offset."))
    }
  }
  
  if(typeof(RIC)=="list"){
    if(length(RIC)!=length(xtsobj1.lst)){
      stop("The number of RICs passed through the argument doesn't match the numbers of timeseries")
    }
  }
  
  if(join=="DEFAULT")
    joinstr <- "outer"
  else
    joinstr <- join
  
  if(fill=="N/A")
    fillnum <- NA
  else
    fillnum <- as.numeric(fill)
  
  stockmeasures_xts.lst <- list()
  for(i in 1: length(xtsobj1.lst)){
    
    xtsobj1 <- xtsobj1.lst[[i]]
    colnames(xtsobj1) <- paste0(colnames(xtsobj1),Suffix1)
    xtsobj2 <- xtsobj2.lst[[i]]
    colnames(xtsobj2) <- paste0(colnames(xtsobj2),Suffix2)
    xts_merged <- merge(xtsobj1, xtsobj2, join=joinstr, fill=fillnum)
    
    if(typeof(RIC)=="list"){
      ricstr = RIC[[i]]
    }else{
      if(xtsAttributes(xtsobj1.lst[[i]])$RIC!=xtsAttributes(xtsobj2.lst[[i]])$RIC){
        ricstr = paste0(xtsAttributes(xtsobj1.lst[[i]])$RIC,":",xtsAttributes(xtsobj2.lst[[i]])$RIC)
      }else{
        ricstr = xtsAttributes(xtsobj1.lst[[i]])$RIC
      }
    }
    
    xtsAttributes(xts_merged)$RIC <- ricstr
    xtsAttributes(xts_merged)$GMTOffset <- xtsAttributes(xtsobj1.lst[[i]])$GMTOffset
    xtsAttributes(xts_merged)$EventType <- "Intraday Measures"
    xtsAttributes(xts_merged)$Market <- "AUX"
    xtsAttributes(xts_merged)$XTSName <- paste0("0_",ricstr,"_mergedmeasures")
    
    stockmeasures_xts.lst[[length(stockmeasures_xts.lst)+1]] <- xts_merged
  }
  
  filename_output <- paste0(path,filename1,"_", filename2, "_merged.rds")
  saveRDS(stockmeasures_xts.lst, file=filename_output)
  return(filename_output)
}

