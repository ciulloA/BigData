###########################
####### DESCRIPTION #######
###########################

# Description goes here.

rm (list = ls())
suppressPackageStartupMessages(library("plyr"))
suppressPackageStartupMessages(library("doParallel"))

source("/home/antonio/Desktop/BigData/codes/functions.R") # load the functions

# Specify working directory: (The wd MUST contain 2 subfolders: "Downalod" and "Clean")
directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Clean"))

# Getting filenames
filename <- list.files(getwd(), pattern = ".*txt$")
#filename = filename[1:50] # only for test!

# Aggregate Data
nodes <- detectCores()
cl <- makePSOCKcluster(nodes)
registerDoParallel(cl)

a_ply(.data = filename,
      .fun = aggregate_data,
      .parallel = TRUE,
      .margins = 1)

stopCluster(cl)