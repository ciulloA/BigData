directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Download"))

# Package for downloading HF-Data from BOVESPA:
if (!require(GetHFData)) install.packages("GetHFData")
suppressPackageStartupMessages(library("GetHFData"))


available_data <- as.matrix(ghfd_get_ftp_contents(type.market = "equity", type.data = "trades"))
dates = available_data[,"dates"]

# if (!require(foreach)) install.packages("foreach")
if (!require(plyr)) install.packages("plyr")
if (!require(doParallel)) install.packages("doParallel")

nodes <- detectCores()
cl <- makeCluster(nodes)
registerDoParallel(cl)

# Download the data in parallel:
aaply(.data = c(available_data[,"link"],dates), 
      .fun = function(x,filename,data) {
          suppressPackageStartupMessages(library("GetHFData"))
          ghfd_download_file(my.ftp = x, 
                             out.file = paste (filename[which(data[,"link"] == x)],".zip", sep = ""), 
                             dl.dir = "Data")
        }, dates,available_data,
      .parallel = TRUE, 
      .margins = 1
)
