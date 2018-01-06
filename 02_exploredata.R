rm (list = ls())
directory = "/home/antonio/Desktop/BigData/data"
linux_dir = "cd ~/Desktop/BigData/data/Download"

# EXPLORING THE DATA
if (!require(data.table)) install.packages("data.table")
suppressPackageStartupMessages(library("data.table"))
setwd(paste0(directory,"/Download"))

# getting filenames
system(linux_dir)
filename <- system("ls", intern = TRUE)

# Reading file    2015-11-03.zip 2017-03-29.zip
sample <- fread(paste0("unzip -p ",filename[1]), skip = 1, sep = ";", fill = TRUE)
sample <- sample[-dim(sample)[1],]
colnames(sample) <- c("Date", "Symbol", "Trade Number", "Price", "Quantity", "Time", "Trade Indicator", "Buy Order Date", "Sequential Buy Order Number", "Secondary Order ID - Buy Order",	"Aggressor Buy Order Indicator", "Sell Order Date", "Sequential Sell Order Number", "Secondary Order ID - Sell Order", "Aggressor Sell Order Indicator", "Cross Trade Indicator", "Buy Member", "Sell Member")

# Cutting useless columns
sample <- sample[, c("Date", "Symbol", "Price", "Quantity", "Time", "Trade Indicator")]
date <- sample$Date[1]
# Tickers <- as.matrix(unique(sample[,"Symbol"])) # All tickers available

sample[,1] <- as.POSIXct(paste(sample$Date, sample$Time), format = "%Y-%m-%d %H:%M:%S", tz="UTC")
colnames(sample)[1] <- "Datetime"
sample[,Time:=NULL]
sample[, Price := mean(Price), by="Symbol,Datetime"] # for each symbol it computes the average Price among lines with the same Datetime
sample <- sample[!duplicated(sample, by = c("Symbol","Datetime")),] # remove duplicate lines

ticker = as.matrix(sample$Symbol)
separator <- which(duplicated(sample$Symbol)*1 == 0)
count = separator[-1]-separator[-length(separator)]
last <- length(ticker)-tail(separator,1)+1
count <- c(count, last)
rm(list = c("separator", "last"))

output <- cbind(date, unique(ticker), count)
colnames(output) <- c("Date","Ticker","Counter")
setwd(paste0(directory,"/Clean"))
fwrite(as.data.table(output), file = paste0(date,"_count.csv"))

# Visualizing the time resolution
library(ggplot2)
ggplot(data = sample) + geom_bar(aes(x = Symbol, colour = factor(Symbol))) + theme(legend.position="none")
# from this plot we can see that for most of the tickers, the time resolution is way smaller than "second" (in the sense that can be minute) 




a <- sample[Symbol == Tickers[4] | Symbol == Tickers[1] | Symbol == Tickers[147],]
sample[, Price:=Price/Price[1], by="Symbol"]

library(ggplot2)
# p <- ggplot(data = sample) + geom_step(aes(x = Datetime, y = Price, colour = factor(Symbol)))

q <- ggplot(data = sample) + geom_bar(aes(x = Symbol, colour = factor(Symbol))) + theme(legend.position="none")


