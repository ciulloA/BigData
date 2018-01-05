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
filename <- system("ls", intern = TRUE)
# filename <- filename[1:10] # (only for tests! to be removed)

# Inspect files size
system("ls -sh > temp")
objects_size <- fread("temp", skip = 1, header = FALSE)
system("rm -f temp")
colnames(objects_size) <- c("Size","File")
# search for files (in Downaload) with dimension of Kilobytes, these will probably be empty files (i.e. missing data)
missing_data <- objects_size$File[grep("K",objects_size$Size)]
# Remove missing data from filename
filename <- setdiff(filename, missing_data)
filename <- filename[1:25] # (only for tests! to be removed)

#Cleaning data
nodes <- min(detectCores(), 6L)
cl <- makePSOCKcluster(nodes)
registerDoParallel(cl)

system.time(a_ply(.data = filename,
      .fun = clean_data, directory,
      .parallel = TRUE,
      .margins = 1))

stopCluster(cl)
