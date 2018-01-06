###########################
####### DESCRIPTION #######
###########################

# In this script we import the data created with "Parallel.R", we merge them
# and we finally select the tickers that have at least 100 and an average 
# of 480 observation per day.

rm (list = ls())
suppressPackageStartupMessages(library("data.table"))
suppressPackageStartupMessages(library("ggplot2"))

# Specify working directory: (The wd MUST contain 2 subfolders: "Downalod" and "Clean")
directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Clean"))

# Getting filenames
filename <- system("ls", intern = TRUE)
# Aggregate data on observation counter
V <- fread(filename[1])
for (i in 1:length(filename)-1) {
  Vi <- fread(filename[i+1])
  V = rbind(V,Vi)
}
rm("Vi")
system(paste("rm -f", paste(filename, collapse = " "))) # delete all files (we won't need them in the future)

V$Date <- as.POSIXct(V$Date, format = "%Y-%m-%d", tz="UTC")
# Sort by ticker, then by date
setkeyv(V, c("Ticker","Date"))

# Ticker available before selection:
all_tickers <- unique(V$Ticker)

# visualize the minimum number of available (per day) observation for each ticker
min_obs <- copy(V)
min_obs[, Counter:=as.integer(min(Counter)), by = "Ticker"]
# ggplot(data = unique(min_obs[, Counter:=min(Counter), by = "Ticker"])) + geom_step(aes(x = Date, y = Counter, color = as.factor(Ticker))) + theme(legend.position="none")
min_obs <- min_obs[!duplicated(min_obs, by = c("Ticker","Counter")),]
ggplot(data =  min_obs[,-1], aes(x = Ticker, y = Counter)) + geom_col() + theme(legend.position="none") #+ theme_few()

#selection1 <- min_obs[min_obs$Counter>=480,] # select ticker that have at least 480 observation per day, each day
mean_obs <- copy(V)
mean_obs[, Counter:=as.integer(mean(Counter)), by = "Ticker"]
mean_obs <- mean_obs[!duplicated(mean_obs, by = c("Ticker","Counter")),]
selection <- min_obs[mean_obs$Counter>=480 & min_obs$Counter>=100]
# visualize the minimum number of available (per day) observation for the selected tickers
ggplot(data =  selection, aes(x = Ticker, y = Counter)) + geom_col() + theme(legend.position="none") #+ theme_few()
# visualize the average number of available (per day) observation for the selected tickers
ggplot(data =  mean_obs[mean_obs$Counter>=480 & min_obs$Counter>=100], aes(x = Ticker, y = Counter)) + geom_col() + theme(legend.position="none") #+ theme_few()

rm("min_obs", "mean_obs")

write(selection$Ticker, "good_tickers.txt")