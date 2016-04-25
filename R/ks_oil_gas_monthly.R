#       Download and combine monthly data for KS oil and gas production
#       Copyright (C) 2016 Stephen Johnson
#
#       This program is free software: you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation, either version 3 of the License, or
#       (at your option) any later version.
#
#       This program is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#       GNU General Public License for more details.
#
#       You should have received a copy of the GNU General Public License
#       along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#
#       Download and combine monthly data for oil and gas production from
#       the Kansas Geological Survey at http://www.kgs.ku.edu/PRS/petro/state.html
#       then saves both original files from the website and the processed data.


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

# Data to 1990 is provided in zip format
zip_decades <- c("1970", "1980", "1990")
# Data to 2010 is provided in xls format
xls_years <- c("2000":"2010")
# Data since 2011 is provided in xlsx format
xlsx_years <- c("2011":"2015")
years <- c(xls_years, xlsx_years)


# Construct list of filenames to download from KGS
# http://www.kgs.ku.edu/PRS/County/Years/1970s.zip
fnames <- vector()
for (decade in zip_decades) {
        fname <- paste(decade, "s.zip", sep="")
        fnames <- c(fnames, fname)
}
for (year in xls_years) {
        fname <- paste(year, ".xls", sep="")
        fnames <- c(fnames, fname)
}
for (year in xlsx_years) {
        fname <- paste(year, ".xlsx", sep="")
        fnames <- c(fnames, fname)
}


# Download data from KGS website
# All data after 1970 is in a single file per decade
for (fname in fnames) {
        url <- paste("http://www.kgs.ku.edu/PRS/County/Years/", fname, sep="")
        dest <- file.path(rawdatafolder, fname)
        download.file(url, destfile=dest, method="wget", quiet = FALSE, mode = "w",cacheOK = TRUE, extra = getOption("download.file.extra"))
}

# # Annual data prior to 1950 is in a single file called history.xls
# dest <- file.path(rawdatafolder, "history.xls")
# download.file("http://www.kgs.ku.edu/PRS/County/history.xls", destfile=dest, method="wget", quiet = FALSE, mode = "w",cacheOK = TRUE, extra = getOption("download.file.extra"))
#
# # Load downloaded data up to 1950 into a dataframe
# fileToGet <- file.path(rawdatafolder, "history.xls")
# KS_oil_pre1950.df <- readWorksheetFromFile(fileToGet, sheet=1)
# names(KS_oil_pre1950.df)[names(KS_oil_pre1950.df)=="YEAR"] <- "Year"  # Change case to agree with post-1950 data
# KS_oil_pre1950.df$OIL_PROD <- KS_oil_pre1950.df$X.OIL  #+ KS_oil_pre1950.df$X.UNATTRIB_OIL
# KS_oil_pre1950.df$GAS_PROD <- KS_oil_pre1950.df$X.GAS  #+ KS_oil_pre1950.df$X.UNATTRIB_GAS
# KS_oil_pre1950.df$OIL_WELLS <- NA  # No data before 1950 but include column for later merge
# KS_oil_pre1950.df$GAS_WELLS <- NA  # No data before 1950 but include column for later merge
# KS_oil_pre1950.df$COUNTY <- "Statewide"  # No data by county before 1950
# KS_oil_pre1950.df <- dplyr::select(KS_oil_pre1950.df, COUNTY, Year, OIL_PROD, GAS_PROD)  # Drop redundant columns
# KS_oil_pre1950.df <- dplyr::filter(KS_oil_pre1950.df, as.numeric(Year) < 1950)  # Drop data from after 1950. Will be replaced by county data
# KS_oil_pre1950.df <- tidyr::gather(KS_oil_pre1950.df, "Measure", "Value", -COUNTY, -Year)  # Reshape to long format
# KS_oil_pre1950.df$Value <- as.numeric(KS_oil_pre1950.df$Value)
# KS_oil_pre1950.df$Year <- as.character(KS_oil_pre1950.df$Year)
# KS_oil_pre1950.df$COUNTY <- as.character(KS_oil_pre1950.df$COUNTY)

# Unzip files
unzippedFiles <- vector()
for (decade in zip_decades) {
        fname <- paste(decade, "s.zip", sep="")
        fileToGet <- file.path(rawdatafolder, fname)
        filesFromzip <- unzip(fileToGet, list = TRUE, junkpaths = TRUE)
        unzippedFiles <- c(unzippedFiles, filesFromzip$Name)
        print(fileToGet)
}
unzippedFiles <- gsub("19[7-9][0-9]s/", "", unzippedFiles)  # Strip off unneeded directory information
unzippedFiles <- unzippedFiles %>% str_subset("19[7-9][0-9].xls")  # Lose non-data files
fnames <- c(unzippedFiles, fnames)
fnames <- fnames %>% str_subset("[1-2][0-9][0-9][0-9].xls")
for (decade in zip_decades) {
        fname <- paste(decade, "s.zip", sep="")
        fileToUnzip <- file.path(rawdatafolder, fname)
       # print(fileToUnzip)
        unzip(fileToUnzip, list = FALSE, junkpaths = TRUE, exdir = rawdatafolder)
}

# Load data from post 1970
KS_oil_monthly.df <- data.frame(COUNTY=character())
for (fname in fnames) {
        fileToGet <- file.path(rawdatafolder, fname)
        df <- readWorksheetFromFile(fileToGet, sheet=1)
        df$COUNTY <- as.character(df$COUNTY)
        df$COUNTY <- gsub("State Total", "Statewide", df$COUNTY)  # Switched from Statewide to State Total in 1970
        df$COUNTY <- gsub("Mcpherson", "McPherson", df$COUNTY)  # Fix case
        KS_oil_monthly.df <- dplyr::bind_rows(KS_oil_monthly.df, df)
}

KS_oil_monthly.df <- dplyr::filter(KS_oil_monthly.df, !is.na(COUNTY))  # Remove rows where no county name
names(KS_oil_monthly.df) <- gsub("X.", "", names(KS_oil_monthly.df))
names(KS_oil_monthly.df) <- gsub("PRODUCTION", "PROD", names(KS_oil_monthly.df))
names(KS_oil_monthly.df) <- gsub("YEAR", "Year", names(KS_oil_monthly.df))
names(KS_oil_monthly.df) <- gsub("MONTH", "Month", names(KS_oil_monthly.df))
KS_counties <- unique(KS_oil_monthly.df$COUNTY)  # Extract a list of KS oil and gas producing counties


# Tidy the data into a long format
KS_oil_monthly.df <- tidyr::gather(KS_oil_monthly.df, "Measure", "Value", -c(COUNTY, Year, Month))  # Reshape to long format
KS_oil_monthly.df$COUNTY <- as.factor(KS_oil_monthly.df$COUNTY)
KS_oil_monthly.df$Value <- as.numeric(KS_oil_monthly.df$Value)
KS_oil_monthly.df$Measure <- as.factor(KS_oil_monthly.df$Measure)
KS_oil_monthly.df$Year <- as.character(KS_oil_monthly.df$Year)

# # Join pre and post 1950 data into one data frame
# KS_oil_pre1950.df$Month <- 1
# KS_oil_pre1950.df <- KS_oil_pre1950.df[c("COUNTY", "Year", "Month", "Measure", "Value")]
# KS_oil_monthly.df <- dplyr::full_join(KS_oil_pre1950.df, KS_oil_monthly.df, by=c("COUNTY", "Year", "Month", "Measure", "Value"))

# Add a date column
KS_oil_monthly.df$Date <- as.Date(paste(KS_oil_monthly.df$Year, KS_oil_monthly.df$Month, 1, sep="-"))

# Save the combined data as a csv and R files
dest=file.path(processeddatafolder, "KS_oil_and_gas_monthly_production.csv")
write.csv(KS_oil_monthly.df,dest,row.names=FALSE)

dest=file.path(processeddatafolder, "KS_oil_and_gas_producing_counties.csv")
write.csv(KS_counties, dest,row.names=FALSE)

dest=file.path(processeddatafolder, "KS_oil_and_gas_monthly_production_data.Rdata")
save(list=c("KS_oil_monthly.df", "KS_counties"),file=dest)

