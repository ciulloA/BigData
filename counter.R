###########################
####### DESCRIPTION #######
###########################

# In this script we import the data created with "Parallel.R", we merge them
# and we can finally select the tickers that have at least a certain number of
# observation per day.

rm (list = ls())
suppressPackageStartupMessages(library("data.table"))
suppressPackageStartupMessages(library("ggplot2"))

# Specify working directory: (The wd MUST contain 2 subfolders: "Downalod" and "Clean")
directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Clean"))

# Getting filenames
filename <- system("ls", intern = TRUE)

V <- fread(filename[1])
for (i in 1:length(filename)-1) {
  Vi <- fread(filename[i+1])
  V = rbind(V,Vi)
}
rm("Vi")
V$Date <- as.POSIXct(V$Date, format = "%Y-%m-%d", tz="UTC")

# Sort by ticker, then by date
setkeyv(V, c("Ticker","Date"))
# Visualize available observation for a specific ticker (select a number instead of 30)
p <- ggplot(data = V[Ticker==unique(V$Ticker)[30],]) + geom_step(aes(x = Date, y = Counter, color = as.factor(Ticker))) + theme(legend.position="none")
p

# visualize the minimum number of available observation for each ticker
min_obs <- copy(V)
ggplot(data = unique(min_obs[, Counter:=min(Counter), by = "Ticker"])) + geom_step(aes(x = Date, y = Counter, color = as.factor(Ticker))) + theme(legend.position="none")
min_obs <- min_obs[!duplicated(min_obs, by = c("Ticker","Counter")),]
ggplot(data =  min_obs[,-1], aes(x = Ticker, y = Counter)) + geom_col() + theme(legend.position="none")
selection <- min_obs[min_obs$Counter>=480,] # select ticker that have at least 480 observation per day, each day
ggplot(data =  selection, aes(x = Ticker, y = Counter)) + geom_col() + theme(legend.position="none")
#rm("min_obs")
# visualize the average number of available observation for each ticker
mean_obs <- copy(V)
ggplot(data = unique(mean_obs[, Counter:=as.integer(mean(Counter)), by = "Ticker"])) + geom_step(aes(x = Date, y = Counter, color = as.factor(Ticker))) + theme(legend.position="none")
rm("mean_obs")

# total_observations <- unique(V[, Counter:=sum(Counter), by = "Ticker"])


