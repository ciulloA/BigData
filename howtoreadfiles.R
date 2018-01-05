# Different way of reading our datasets

suppressPackageStartupMessages(library("data.table"))
setwd("/home/antonio/Desktop/BigData/data")

# 1) -----------------------------------------------------------------------
n <- as.integer(system("zgrep -vE 'RH NEG|RT NEG' 2017-03-29.zip | wc -l", intern = TRUE)) # number of lines excluding the problematic ones
sample <- fread(paste0("unzip -p ","2017-03-29.zip"), skip = 1, nrows = n, sep = ";", fill = TRUE)

# 2) -----------------------------------------------------------------------
# This way we avoid using the system command to determine the number of lines, moreover fread 
# automaically skip the 1st line (it recognises that cannot be a data or header) and the
# fill option allow us to read the last line which was problematic. We finally need to cut 
# the last row. We can also specify the parameter "select" to import only the columns we
# are interted in.
sample <- fread(paste0("unzip -p ","2017-03-29.zip"), sep = ";", fill = TRUE)

# 3) -----------------------------------------------------------------------
# shoudn't be the efficient way of reading the files.
system("cd ~/Desktop/BigData/data")
system(command = "zgrep -vE 'RH NEG|RT NEG' 2017-03-29.zip > temp")
sample <- fread("temp", sep = ";")
system("rm -f temp")
# --------------------------------------------------------------------------

colnames(sample) <- c("Session Date", "Instrument Symbol", "Trade Number", "Trade Price", "Traded Quantity", "Trade Time", "Trade Indicator", "Buy Order Date", "Sequential Buy Order Number", "Secondary Order ID - Buy Order",	"Aggressor Buy Order Indicator", "Sell Order Date", "Sequential Sell Order Number", "Secondary Order ID - Sell Order", "Aggressor Sell Order Indicator", "Cross Trade Indicator", "Buy Member", "Sell Member")