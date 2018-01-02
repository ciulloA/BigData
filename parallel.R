rm (list = ls())
# Libraries needed for this script:
if (!require(data.table)) install.packages("data.table")
if (!require(plyr)) install.packages("plyr")
if (!require(doParallel)) install.packages("doParallel")

# Function -----------------------------------------------------------------------------------------------
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

suppressPackageStartupMessages(library("doParallel"))
suppressPackageStartupMessages(library("plyr"))

# Specify working directory: (The wd MUST contain 2 subfolders: "Downalod" and "Clean")
directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Download"))
# Getting filenames
filename <- system("ls", intern = TRUE)
filename <- filename[1:100] # (only for tests! to be removed)

# Inspect files size
system("ls -sh > temp")
objects_size <- fread("temp", skip = 1, header = FALSE)
system("rm -f temp")
colnames(objects_size) <- c("Size","File")
# search for files (in Downaload) with dimension of Kilobytes, these will probably be empty files (i.e. missing data)
missing_data <- objects_size$File[grep("K",objects_size$Size)]
# Remove missing data from filename
filename <- setdiff(filename,missing_data)

available_days <- as.POSIXct(sub(".zip","",filename), format = "%Y-%m-%d", tz="UTC")
all_days <- seq.POSIXt(from = available_days[1], to = last(available_days), by = "days")
missing_days <-  all_days[!all_days %in% available_days]
# Remove from missing_day Saturday and Sunday
missing_days <- missing_days[wday(missing_days)!=7 & wday(missing_days)!=1]
# we observe that most of these missing days that are not weekend, are the same each year, so we'll
# assume that are holidays.

nodes <- min(detectCores(), 6L)
cl <- makePSOCKcluster(nodes)
registerDoParallel(cl)

a_ply(.data = filename,
      .fun = ticker_count, directory,
      .parallel = TRUE,
      .margins = 1,
      .progress = TRUE)

stopCluster(cl)



############### TRASH ##################################################################

# system("cd ~/Desktop/BigData/data/Download")
# 
# lines_counter <- function(x) {
#   lines <- as.integer(system(paste("zgrep -vE 'RH NEG|RT NEG'", x,"| wc -l"), intern = TRUE)) # number of lines excluding the problematic ones
#   }
# 
# n <- system.time(aaply(.data = filename,
#                        .fun = lines_counter,
#                        .parallel = F,
#                        .margins = 1))
# 
# 
# n = rep(0,length(filename))
# for (i in 1:length(filename)) {
#   n[i] = system(paste("zgrep -vE 'RH NEG|RT NEG'", filename[i],"| wc -l"), intern = TRUE)
# }
# 
# 
# line <- fread(paste0("unzip -p ",filename[1]), sep = ";", fill = TRUE), select = c(1,2,3,4))

# setwd("/home/antonio/Desktop/BigData/data/Clean")
# list <- system("ls",intern =TRUE)
# setwd("/home/antonio/Desktop/BigData/data/Download")
# list2 <- system("ls",intern =TRUE)[1:100]

