rm (list = ls())
# Libraries needed for this script:
if (!require(data.table)) install.packages("data.table")
if (!require(plyr)) install.packages("plyr")
if (!require(doParallel)) install.packages("doParallel")

suppressPackageStartupMessages(library("doParallel"))
suppressPackageStartupMessages(library("plyr"))

source("/home/antonio/Desktop/BigData/codes/functions.R") # load the functions

# Specify working directory: (The wd MUST contain 2 subfolders: "Downalod" and "Clean")
directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Download"))
# Getting filenames
filename <- list.files(getwd(), pattern = ".*zip$")
#filename <- system("ls", intern = TRUE) #(using ubuntu terminal)

# Inspect files size
system("ls -sh > temp")
objects_size <- fread("temp", skip = 1, header = FALSE)
system("rm -f temp")
colnames(objects_size) <- c("Size","File")
# search for files (in Downaload) with dimension of Kilobytes, these will probably be empty files (i.e. missing data)
missing_data <- objects_size$File[grep("K",objects_size$Size)]
# Check whether these file are indeed empty
n = 0
for (i in 1:length(missing_data)) {
  n[i] <- as.integer(system(paste("zgrep -vE 'RH NEG|RT NEG'", missing_data[i], "| wc -l"), intern = TRUE)) # number of lines excluding Header and Trailer
}
if (sum(n)==0) {show("The file in 'missing_data' are indeed empty")}
# Remove missing data from filename
filename <- setdiff(filename, missing_data)

# Now we want to compare the available days with all the days from the first to the 
# last day in our dataset, in order to see for which and for how many days we don't
# have any data.
available_days <- as.POSIXct(sub(".zip","",filename), format = "%Y-%m-%d", tz="UTC")
all_days <- seq.POSIXt(from = available_days[1], to = last(available_days), by = "days")
missing_days <-  all_days[!all_days %in% available_days]
# From here it seems we are missing lots of days, however the market is closed on Saturday and Sunday!

# Remove from missing_day Saturday and Sunday
missing_days <- missing_days[wday(missing_days)!=7 & wday(missing_days)!=1]
# We further observe that most of these missing days which are not weekend, they are 
# more or less the same each year, therefore we assume that those are holidays.

# Finally, if we sum the number of available days for our 2 year sample, we get
# 249 days for 2016 and 246 for 2017 which are fairly close to 252 (average number 
# of trading days per year) 
sum(grepl("2016", available_days))
sum(grepl("2017", available_days))


# At this point we can launch the function which will create new files used for 
# a deeper analysis* of our dataset. (Note this will take several minutes)

nodes <- min(detectCores(), 6L)
cl <- makePSOCKcluster(nodes)
registerDoParallel(cl)

a_ply(.data = filename,
      .fun = ticker_count, directory,
      .parallel = TRUE,
      .margins = 1)

stopCluster(cl)

# *
# More specifically, since we have 'tick by tick' data, we want to count how many
# observation we have for each ticker per day, to better understand what kind of 
# time resolution (seconds, minutes, hours...) we can use for our analysis or to 
# select only the tickers that have a certain number of observation each day. 
