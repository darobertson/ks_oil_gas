library(dplyr)
library(ggplot2)

df <- KS_oil_monthly.df %>%
        filter(COUNTY == "Douglas" &
                       Measure == "OIL_PROD"
               )

ggplot(df) +
        geom_point(aes(x=Date, y=Value, color=Measure)) +
        geom_line(aes(x=Date, y=Value, color=Measure)) +
        # facet_grid(Measure ~ .)
        theme_classic()
