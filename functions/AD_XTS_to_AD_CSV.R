#APPLICATION 1
#Name: Convert ADAGE XTS format to CSV format 
#From GUI we have
#Input datasource = Adage
#input dataset structure = Any
#input format = RDS
#Output datasource = Adage
#Output format = CSV

ad_xts_to_ad_csv <- function(path, filename) {
  library(xts)
  xtsobj.lst<- readRDS(paste0(path,filename))
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
  #data.frame(RIC=xtsAttributes(xtsobj)$RIC, Market=xtsAttributes(xtsobj)$Market, EventType=xtsAttributes(xtsobj)$EventType, Datetime=index(xtsobj), GMTOffset=xtsAttributes(xtsobj)$GMTOffset, xtsobj, row.names=NULL)
  df<- data.frame(RIC=xtsAttributes(xtsobj)$RIC, Market=xtsAttributes(xtsobj)$Market, EventType=xtsAttributes(xtsobj)$EventType, Datetime=index(xtsobj), GMTOffset=xtsAttributes(xtsobj)$GMTOffset, xtsobj, row.names=NULL) # by Alan Ng 20220327
  return (df)
}


#helper function
xtsRds2Csv <- function(xtsobj, path1) {
  dframe <- data.frame(RIC=xtsAttributes(xtsobj)$RIC, EventType=xtsAttributes(xtsobj)$EventType, Datetime=index(xtsobj), GMTOffset=xtsAttributes(xtsobj)$GMTOffset, xtsobj, row.names=NULL)
  write.csv(dframe, file= paste0(path1,xtsAttributes(xtsobj)$XTSName,".csv"), row.names = F)
}