rm (list = ls())
suppressPackageStartupMessages(library("plyr"))
suppressPackageStartupMessages(library("doParallel"))

source("/home/antonio/Desktop/BigData/codes/functions.R") # load the functions

# Specify working directory: (The wd MUST contain 2 subfolders: "Downlaod" and "Clean")
directory = "/home/antonio/Desktop/BigData/data"
setwd(paste0(directory,"/Clean"))

# Getting filenames
filename <- list.files(pattern = ".*txt$")[grep("_clean",list.files(pattern = ".*txt$"))]

#Creating new output
nodes <- detectCores()
cl <- makePSOCKcluster(nodes)
registerDoParallel(cl)

system.time(a_ply(.data = filename,
      .fun = vol_price,
      .margins = 1,
      .parallel = T))



# # ----- var-covar matrix:
# system.time({pdata = list()
# for (i in unique(temp$Symbol)) {
#   pdata[[i]] = as.xts.data.table(temp[Symbol==i, c("Datetime","Price")])
# }
# })
# rRTSCov(pdata)
# # -----
# 
# rdata = matrix()
# for (i in unique(temp$Symbol)) {
#   rdata[[i]] = as.xts.data.table(temp[Symbol==i, c("Datetime","Price")])
# }
# 
