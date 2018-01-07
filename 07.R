rm (list = ls())
suppressPackageStartupMessages(library("data.table"))
suppressPackageStartupMessages(library("ggplot2"))

directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Clean"))

filename <- list.files(pattern = ".*txt$")[-grep("_clean",list.files(pattern = ".*txt$"))]
file <- fread(filename[1], nrows = 100000)

file$Datetime <- as.POSIXct(file$Datetime, tz = "UTC")
# p <- ggplot(data = file) + geom_step(aes(x = Datetime, y = Price))
setkeyv(file, "Datetime")


days <- unique(as.Date(file$Datetime))
p <- ggplot(data = file[grep(days[2], as.Date(file$Datetime)),]) + geom_step(aes(x = Datetime, y = Price))
