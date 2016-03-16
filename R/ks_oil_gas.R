# Download and combine annual data for oil and gas production from
# the Kansas Geological Survey at http://www.kgs.ku.edu/PRS/petro/state.html
#
# Saves both original files from the website and the processed data.
#
# Author: Stephen Johnson
# Licence: GPL-2


# load libraries
library(dplyr)
library(tidyr)
library(stringr)
library(XLConnect)

# Remove all objects from the environment for a clean start
rm(list = ls(all = TRUE))

# Define folders and create if needed
rawdatafolder <- "KGS_downloaded_data"
if (!file.exists(rawdatafolder)) dir.create(rawdatafolder)
processeddatafolder <- "KGS_processed_data"
if (!file.exists(processeddatafolder)) dir.create(processeddatafolder)

# Data to 2009 is provided in xls format
xls_decades <- c("1950", "1960", "1970", "1980", "1990", "2000")
# Data since 2010 is provided in xlsx format
xlsx_decades <- c("2010")
decades <- c(xls_decades, xlsx_decades)


# Construct list of filenames to download from KGS
fnames <- vector()
for (decade in xls_decades) {
        fname=paste("cnty_ann_", decade, "s.xls", sep="")
        fnames=c(fnames, fname)
}
for (decade in xlsx_decades) {
        fname=paste("cnty_ann_", decade, "s.xlsx", sep="")
        fnames=c(fnames, fname)
}


# Download data from KGS website
# All data prior to 1950 is in a single file called history.xls
dest <- file.path(rawdatafolder, "history.xls")
download.file("http://www.kgs.ku.edu/PRS/County/history.xls", destfile=dest, method="wget", quiet = FALSE, mode = "w",cacheOK = TRUE, extra = getOption("download.file.extra"))
# All data after 1950 is in a single file per decade
for (fname in fnames) {
        url <- paste("http://www.kgs.ku.edu/PRS/County/", fname, sep="")
        dest <- file.path(rawdatafolder, fname)
        download.file(url, destfile=dest, method="wget", quiet = FALSE, mode = "w",cacheOK = TRUE, extra = getOption("download.file.extra"))
}

# Load downloaded data up to 1950 into a dataframe
fileToGet <- file.path(rawdatafolder, "history.xls")
KS_oil_pre1950.df <- readWorksheetFromFile(fileToGet, sheet=1)
names(KS_oil_pre1950.df)[names(KS_oil_pre1950.df)=="YEAR"] <- "Year"  # Change case to agree with post-1950 data
KS_oil_pre1950.df$OIL_PROD <- KS_oil_pre1950.df$X.OIL  #+ KS_oil_pre1950.df$X.UNATTRIB_OIL
KS_oil_pre1950.df$GAS_PROD <- KS_oil_pre1950.df$X.GAS  #+ KS_oil_pre1950.df$X.UNATTRIB_GAS
KS_oil_pre1950.df$OIL_WELLS <- NA  # No data before 1950 but include column for later merge
KS_oil_pre1950.df$GAS_WELLS <- NA  # No data before 1950 but include column for later merge
KS_oil_pre1950.df$COUNTY <- "Statewide"  # No data by county before 1950
KS_oil_pre1950.df <- dplyr::select(KS_oil_pre1950.df, COUNTY, Year, OIL_PROD, GAS_PROD)  # Drop redundant columns
KS_oil_pre1950.df <- dplyr::filter(KS_oil_pre1950.df, as.numeric(Year) < 1950)  # Drop data from after 1950. Will be replaced by county data
KS_oil_pre1950.df <- tidyr::gather(KS_oil_pre1950.df, "Measure", "Value", -COUNTY, -Year)  # Reshape to long format
KS_oil_pre1950.df$Value <- as.numeric(KS_oil_pre1950.df$Value)
KS_oil_pre1950.df$Year <- as.character(KS_oil_pre1950.df$Year)
KS_oil_pre1950.df$COUNTY <- as.character(KS_oil_pre1950.df$COUNTY)

# Load data from post 1950
KS_oil_post1950.df <- data.frame(COUNTY=character())
for (fname in fnames) {
        fileToGet <- file.path(rawdatafolder, fname)
        df <- readWorksheetFromFile(fileToGet, sheet=1)
        df$COUNTY <- as.character(df$COUNTY)
        df$COUNTY <- gsub("State Total", "Statewide", df$COUNTY)  # Switched from Statewide to State Total in 1970
        df$COUNTY <- gsub("Mcpherson", "McPherson", df$COUNTY)  # Fix case
        KS_oil_post1950.df <- dplyr::full_join(KS_oil_post1950.df, df, by="COUNTY")
}

KS_oil_post1950.df <- dplyr::filter(KS_oil_post1950.df, !is.na(COUNTY))  # Remove rows where no county name
KS_counties <- KS_oil_post1950.df$COUNTY  # Extract a list of KS oil and gas producing counties


# Tidy the data into a long format
KS_oil_post1950.df <- tidyr::gather(KS_oil_post1950.df, "Metric", "Value", -COUNTY)  # Reshape to long format
KS_oil_post1950.df <- tidyr::separate(KS_oil_post1950.df, "Metric", into=c("Year", "Measure"), sep="_", extra="merge", remove=TRUE)  # e.g. "X1950_OIL_PROD" to "X1950" + "OIL_PROD"
KS_oil_post1950.df$Year <- stringr::str_sub(as.character(KS_oil_post1950.df$Year), -4, -1)  # Extract year from e.g. "X1950"  to "1950"
KS_oil_post1950.df$COUNTY <- as.factor(KS_oil_post1950.df$COUNTY)
KS_oil_post1950.df$Value <- as.numeric(KS_oil_post1950.df$Value)
KS_oil_post1950.df$Measure <- as.factor(KS_oil_post1950.df$Measure)

# Join pre and post 1950 data into one data frame
KS_oil.df <- dplyr::full_join(KS_oil_pre1950.df, KS_oil_post1950.df, by=c("COUNTY", "Year", "Measure", "Value"))

# Save the combined data as a csv and R files
dest=file.path(processeddatafolder, "KS_oil_and_gas_annual_production.csv")
write.csv(KS_oil.df,dest,row.names=FALSE)

dest=file.path(processeddatafolder, "KS_oil_and_gas_producing_counties.csv")
write.csv(KS_counties, dest,row.names=FALSE)

dest=file.path(processeddatafolder, "KS_oil_and_gas_annual_production_data.R")
dump(c("KS_oil.df", "KS_counties"),file=dest)
