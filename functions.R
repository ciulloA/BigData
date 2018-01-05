###########################
######## FUNCTIONS ########
###########################

# Here we collect all the functions created for the project

ticker_count <- function(filename, directory) {
  gc()
  suppressPackageStartupMessages(library("data.table"))
  setwd(paste0(directory,"/Download"))
  # Reading file
  sample <- fread(paste0("unzip -p ",filename), skip = 1, sep = ";", fill = TRUE, select = c(1,2,6))
  sample <- sample[-dim(sample)[1],]
  # colnames(sample) <- c("Date", "Symbol", "Trade Number", "Price", "Quantity", "Time", "Trade Indicator", "Buy Order Date", "Sequential Buy Order Number", "Secondary Order ID - Buy Order",	"Aggressor Buy Order Indicator", "Sell Order Date", "Sequential Sell Order Number", "Secondary Order ID - Sell Order", "Aggressor Sell Order Indicator", "Cross Trade Indicator", "Buy Member", "Sell Member")
  
  # Cutting useless columns
  colnames(sample) <- c("Date", "Symbol","Time")
  # sample <- sample[, c("Date", "Symbol","Time")]
  date <- sample$Date[1]
  sample[,1] <- as.POSIXct(paste(sample$Date, sample$Time), format = "%Y-%m-%d %H:%M:%S", tz="UTC")
  colnames(sample)[1] <- "Datetime"
  sample <- sample[!duplicated(sample, by = c("Symbol","Datetime")),] # remove duplicate lines
  
  ticker = as.matrix(sample$Symbol)
  separator <- which(duplicated(sample$Symbol)*1 == 0)
  count = separator[-1]-separator[-length(separator)]
  last <- length(ticker)-tail(separator,1)+1
  count <- c(count, last)
  rm(list = c("separator", "last","sample"))
  
  # Create file with noumber of observation for each ticker and for each day
  output <- cbind(date, unique(ticker), count)
  colnames(output) <- c("Date","Ticker","Counter")
  setwd(paste0(directory,"/Clean"))
  fwrite(as.data.table(output), file = paste0(date,"_count.csv"))
  rm("sample")
  gc()
}
# END ----------------------------------------------------------------------------------------------------
