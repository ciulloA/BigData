rm (list = ls())
suppressPackageStartupMessages(library("data.table"))

# Specify working directory: (The wd MUST contain 2 subfolders: "Downlaod" and "Clean")
directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Clean"))

# Getting filenames
filename <- list.files(pattern = ".*txt$")[grep("_clean",list.files(pattern = ".*txt$"))]


# tmp <- seq(as.POSIXct('2015-12-29 10:00', tz = "UTC"), as.POSIXct('2015-12-29 18:00', tz = "UTC"), by='30 min')
# window <- data.table(tmp[-length(tmp)], tmp[-1])
# colnames(window) <- c("Start", "End")
# rm("tmp")



# Tfae but the 1st faster
system.time(selection <-temp[Datetime >= window$Start[2] & Datetime <= window$End[2]])
# system.time(selection <- subset(temp, temp$Datetime > window$Start[8] & temp$Datetime <= window$End[8]))

selection[, CP:=Price/Price[1], by="Symbol"]

selection <-temp[Datetime >= window$Start[j] & Datetime <= window$End[j]]

n=0
for (i in unique(selection$Symbol)) {
  ind = (n+1):(n+16)
  window$Symbol[ind] = i
  for (j in 1:16) {0
    selection <-temp[Datetime >= window$Start[j] & Datetime <= window$End[j]]
    window$open[ind[1]+j-1] = first(selection[Symbol==i])$Price
    window$close[ind[1]+j-1] = ifelse(is.null(length(selection[Symbol==i]$Price)), NA, last(selection[Symbol==i])$Price)
  }
  n=dim(window)[1]
}





k=1
for (i in unique(selection$Symbol)) {
  window$open[k] = first(selection[Symbol==i])$Price
  window$close[k] = last(selection[Symbol==i])$Price
  k=k+1
}


CP = first(selection[Symbol==temp$Symbol[1]])$Price
CPn = last(selection[Symbol==temp$Symbol[1]])$Price


ggplot(selection[Symbol==temp$Symbol[1]]) + geom_step(aes(x = Datetime, y = Price))

