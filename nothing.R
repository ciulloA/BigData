fread("2015-12-29_clean.txt") -> A
A$Datetime <- as.POSIXct(A$Datetime, tz = "UTC")


fread(paste0("unzip -p ","2015-12-29_clean.csv.zip")) -> A
as.POSIXct(A$Datetime, format = "%Y-%m-%d %H:%M:%S", tz="UTC") -> A$Datetime



system(for file in *.zip; do zip "${file}"; done > md5sum.txt