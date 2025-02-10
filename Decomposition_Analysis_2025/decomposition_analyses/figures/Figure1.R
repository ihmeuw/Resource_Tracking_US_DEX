## ==================================================
## Author(s): Drew DeJarnatt
## Purpose: This script is used to generate the figure for the decomposition manuscript map.
## ==================================================

# packages
library(usmap)
library(data.table)
library(arrow)
library(tidyverse)

Sys.umask(mode = 002)

## -----------------------------------
## Arguments and set up
## -----------------------------------
# specify scaled_version 
if(interactive()){
  scaled_version <- "69"
  draws <- T
} else {
  args <- commandArgs(trailingOnly = TRUE)
  scaled_version <- args[1]
  draws <- args[2]
}


# data paths to pull from
county_data_path <- paste0("FILEPATH")

out_dir <- paste0("FILEPATH")

if(!dir.exists(out_dir)){
  dir.create(out_dir, recursive = T)
}

## Estimates
df <- open_dataset(county_data_path) %>% 
  filter(year_id == 2019, payer != "oth") %>%
  mutate(mcnty = as.numeric(location)) %>%
  group_by(mcnty) %>%
  summarize(spend = sum(mean_spend)) %>%
  collect() %>% setDT()

# county fips
county_names <- fread("FILEPATH")[, .(mcnty, fips = cnty, cnty_name, state_name)]

# county_pop
pop <- fread("FILEPATH") %>%
  filter(year_id == 2019) %>%
  group_by(mcnty, year_id) %>%
  summarize(pop = sum(pop))

# Use full payer name in plot titles and labels
payer_list <- list("mdcr" = "Medicare", 
                   "mdcd" = "Medicaid", 
                   "priv" = "Private insurance", 
                   "oop" = "Out-of-pocket")

#----------------------------------
# County specific maps
#----------------------------------
# merge to county names for fips (needed for county mapping in plot_usmap())
county_df <- merge(county_names, df, by = "mcnty")
county_df <- merge(county_df, pop, by = "mcnty")
county_df[, spend_pc := spend/pop]

# set breaks and labels for 7 bins
brks <- sapply(seq(0, 1, by = 1/7), function(x) quantile(county_df$spend_pc, x))
labs <- paste0("$",format(scales::comma(round(brks[-length(brks)]))), " - $", format(scales::comma(round(brks[-1]))))
brks[1] <- brks[1]-1 #decrease the min by a dollar so the lowest value isn't cutoff from the rounding above

# cut data into 7 bins
county_df$plot_val <- cut(county_df$spend_pc, breaks = brks, labels = labs)

# assign colors to bins
cols <- c("#ff9809",
          "#ffb567",
          "#ffd3ab",
          "#f1f1f1",
          "#d4b9dc",
          "#b682c7",
          "#964bb1")

# county and state shapefiles
mcnty_shapefile <- readRDS("FILEPATH")
state_shapefile <- readRDS("FILEPATH")

plot_df <- merge(mcnty_shapefile, county_df, by = "mcnty")

plot <- ggplot(data = plot_df) + 
  geom_sf(aes(fill = plot_val, geometry = geometry), color = NA) + # counites w/o border
  geom_sf(data = state_shapefile, fill = NA, linewidth = .5) + # state borders
  labs(title = paste0("Figure 1\nSpending per capita by US county (2019)")) +
  scale_fill_manual(values = cols,
                    breaks = levels(factor(county_df$plot_val))) +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.direction = "horizontal",
        legend.box = "horizontal",
        legend.title = element_blank(),
        title = element_text(size = 12),
        text = element_text(size = 12),
        axis.ticks = element_blank(),
        axis.text = element_blank(),
        panel.background = element_blank()) +
  guides(fill = guide_legend(nrow = 1))

ggsave(paste0(out_dir,"figure_1.pdf"), x, height = 9, width = 11)

