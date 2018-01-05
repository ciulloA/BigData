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
  sample <- sample[-dim(sample)[1],] # remove last row (Trailer)
  # colnames(sample) <- c("Date", "Symbol", "Trade Number", "Price", "Quantity", "Time", "Trade Indicator", "Buy Order Date", "Sequential Buy Order Number", "Secondary Order ID - Buy Order",	"Aggressor Buy Order Indicator", "Sell Order Date", "Sequential Sell Order Number", "Secondary Order ID - Sell Order", "Aggressor Sell Order Indicator", "Cross Trade Indicator", "Buy Member", "Sell Member")
  colnames(sample) <- c("Date", "Symbol","Time")
  date <- sample$Date[1]
  sample[,1] <- as.POSIXct(paste(sample$Date, sample$Time), format = "%Y-%m-%d %H:%M:%S", tz="UTC")
  colnames(sample)[1] <- "Datetime"
  sample <- sample[!duplicated(sample, by = c("Symbol","Datetime")),] # remove duplicate lines
  
  ticker = as.matrix(sample$Symbol)
  separator <- which(duplicated(sample$Symbol)*1 == 0)
  count = separator[-1]-separator[-length(separator)]
  last <- length(ticker)-tail(separator,1)+1
  count <- c(count, last)
  rm(list = c("separator", "last", "sample"))
  gc()
  
  # Create file with noumber of observation for each ticker and for each day
  output <- cbind(date, unique(ticker), count)
  colnames(output) <- c("Date","Ticker","Counter")
  setwd(paste0(directory,"/Clean"))
  fwrite(as.data.table(output), file = paste0(date,"_count.csv"))
  rm("Ticker")
}




clean_data <- function(filename, directory) {
  gc()
  suppressPackageStartupMessages(library("data.table"))
  suppressPackageStartupMessages(library("doParallel"))
  suppressPackageStartupMessages(library("plyr"))
  
  setwd(paste0(directory,"/Clean"))
  # Reading good tickers
  tickers <- fread("good_tickers.txt", header = FALSE)

  setwd(paste0(directory,"/Download"))
  # Reading file
  temp <- fread(paste0("unzip -p ",filename), skip = 1, sep = ";", fill = TRUE, select = c(1,2,4,6))
  temp <- temp[-dim(temp)[1],]
  colnames(temp) <- c("Date", "Symbol","Price","Time")
  date <- temp$Date[1] # (used at the end for output filename)
  # Convert time and date in a POSIXct object
  temp[,1] <- as.POSIXct(paste(temp$Date, temp$Time), format = "%Y-%m-%d %H:%M:%S", tz="UTC")
  colnames(temp)[1] <- "Datetime"
  set(temp, j = "Time", value = NULL) # = temp[,Time:= NULL]
  # For some ticker, the time res is < than second, however, when time is converted to POSIXct we loose
  # any resolution below the second. (eg. 10:00:00.371 and 10:00:00.580 will become both 10:00:00). We 
  # decide to discard these "duplicated" prices and take only the 1st one. (in the example above we 
  # delete 10:00:00.580 and we consider the price at 10:00:00.371 to be the price at 10:00:00).
  temp <- temp[!duplicated(temp, by = c("Symbol","Datetime")),]

  #Cleaning data
#  nodes <- 8L
#  cl <- makePSOCKcluster(nodes)
#  registerDoParallel(cl)
  ind <- alply(.data = tickers,
               .fun = function(x, data) {
                 as.integer(grep(x, data))
               }, temp$Symbol,
               .margins = 1,
               .parallel = FALSE)
  ind <- as.integer(unlist(ind)) # we couldn't use directly aaply because the output lenght of .fun is different for each element
  # Remove useless tickers from data
  temp <- temp[ind,] 
  temp$Datetime <- as.character.POSIXt(temp$Datetime)
  # Save output file of cleaned data
  setwd(paste0(directory,"/Clean"))
  fwrite(temp, file = paste0(date,"_clean.txt"))
}


#(temp[grep(k,temp$Symbol),])[duplicated(temp[grep(k,temp$Symbol),], by = "Datetime"),]

# For each symbol compute the average Price among lines with the same exact Datetime. We need this because
# for some ticker, the time res is < than second, however, when time is converted to POSIXct we loose
# any resolution below the second. (eg. 10:00:00.371 and 10:00:00.580 will become both 10:00:00) 
#  temp[, Price := mean(Price), by="Symbol,Datetime"]
##  for (k in unique(temp$Symbol)) set(temp, i = duplicated(temp[grep(k,temp$Symbol),], by = "Datetime"), j = "Price", value = mean("Price"))
