## ==================================================
## Author(s): Drew DeJarnatt
## 
## Purpose: Capstone figure 2 - county spending maps
## ==================================================

# packages
library(usmap)
library(data.table)
library(arrow)
library(tidyverse)
library(usmap)
library(gridExtra)
library(scales)
library(ggpubr)
library(grid)

Sys.umask(mode = 002)

## -----------------------------------
## Arguments and set up
## -----------------------------------
# specify scaled_version 
scaled_version <- "XX"

# data path to pull from
data_path <- paste0("FILEPATH",scaled_version,"FILEPATH")

out_dir <- paste0("FILEPATH", scaled_version,"FILEPATH")
if(!dir.exists(out_dir)){
  dir.create(out_dir, recursive = T)
}

## Estimates
df <- open_dataset(data_path) %>%
  filter(year_id == 2019, toc == "all") %>%
  mutate(mcnty = as.numeric(location)) %>%
  collect() %>% 
  group_by(mcnty, payer, year_id) %>%
  summarize(mean_value = mean(value),
            lower_value = quantile(value, 0.025),
            upper_value = quantile(value, 0.975))%>%
  setDT()

# Drop counties with uncertianty range > 1
df[, uc_range := upper_value - lower_value]
df[, mask := ifelse(uc_range > mean_value, 1, 0)]

df[, mean_value := ifelse(mask == 1, NA, mean_value)]
setnames(df, "mean_value", "value")

# county fips
county_names <- fread("FILEPATH")[, .(mcnty, fips = cnty)]

# Use full payer name in plot titles and labels
payer_list <- list("mdcr" = "Medicare", 
                   "mdcd" = "Medicaid", 
                   "priv" = "Private insurance", 
                   "oop" = "Out-of-pocket")

## county and state shaefiles
mcnty_shapefile <- readRDS("FILEPATH/mcnty_sf_shapefile.rds")
state_shapefile <- readRDS("FILEPATH/state_sf_shapefile.rds")

#----------------------------------
# County specific maps
#----------------------------------
# merge to county names for fips (needed for county mapping in plot_usmap())
county_stndz <- merge(county_names, df[payer == "all"], by = "mcnty")

# set breaks and labels for 7 bins
brks <- sapply(seq(0, 1, by = 1/7), function(x) quantile(county_stndz$value, x))
brks[1] <- brks[1]-1
labs <- paste0("$",format(comma(round(brks[-length(brks)]))), " - $", format(comma(round(brks[-1]))))

# cut data into 7 bins
county_stndz$plot_val <- cut(county_stndz$value, breaks = brks, labels = labs)
# assign colors to bins
cols <- c("#ff9809",
          "#ffb567",
          "#ffd3ab",
          "#f1f1f1",
          "#d4b9dc",
          "#b682c7",
          "#964bb1")
cols = c("#f1f1f1", "#e9d3eb", "#e0b5e4", "#d696de", "#cb77d7", "#be56d0", "#b12bc9")

# merge with county shapefile 
county_stndz <- merge(mcnty_shapefile %>% filter(mcnty %in% unique(county_stndz$mcnty)), county_stndz, by = "mcnty")

# Map!
county_map <-  ggplot(data = county_stndz) +
  geom_sf(aes(fill = plot_val, geometry = geometry), color = NA) + # counties w/o border
  geom_sf(data = state_shapefile, fill = NA, linewidth = .4) +
  labs(title = paste0("Spending per capita by US county (2019)"),
       fill = "") +
  scale_fill_manual(values = cols,
                    breaks = levels(factor(county_stndz$plot_val))[levels(factor(county_stndz$plot_val)) != "NA"],
                    na.value = "#838484") +
  theme(legend.position = "bottom",
        legend.justification = "center",
        legend.direction = "horizontal",
        legend.box = "horizontal",
        title = element_text(size = 12),
        text = element_text(size = 10),
        plot.margin = margin(0.5, 0, 1, 0, "cm"),
        axis.ticks = element_blank(),
        axis.text = element_blank(),
        panel.background = element_blank()) +
  guides(fill = guide_legend(nrow = 1))



#----------------------------------
# Payer specific maps
#----------------------------------
payer_colors <- list("priv" =	c("#f1f1f1", "#f5dbbc", "#f4c788", "#efb353", "#E69F00"),
                     "mdcr" =	c("#f1f1f1","#c2c9e0", "#93a4d0","#6080bf","#0E5EAE"),
                     "mdcd" =	c("#f1f1f1","#c1dcd0","#91c8b0","#5db391","#009E73"),
                     "oop" =	c("#f1f1f1","#e8c3b7","#da967f","#c6694b","#ae3918"))

# Spend per beneficiary maps
plot_data <- merge(df[payer != "all"], county_names, by = "mcnty")
plot_list <- list()
for(p in c("mdcr", "mdcd", "priv", "oop")){
  if(length(plot_list) >= 4){
    plot_list = list()
  }
  print(p)
  map_df <- plot_data[payer == p]
  
  brks <- c(quantile(map_df$value, .0, na.rm = TRUE),
            quantile(map_df$value, .2, na.rm = TRUE),
            quantile(map_df$value, .4, na.rm = TRUE),
            quantile(map_df$value, .6, na.rm = TRUE),
            quantile(map_df$value, .8, na.rm = TRUE),
            quantile(map_df$value, 1, na.rm = TRUE))
  labs <- paste0("$",format(comma(round(brks[-length(brks)]))), " - $", format(comma(round(brks[-1]))))
  
  
  map_df$plot_val <- cut(map_df$value, breaks = c(brks), labels = labs)
  cols = payer_colors[[p]]
  payer_title <- payer_list[[p]]
  if(p == "oop"){
    denom = "capita"
  } else {
    denom = "beneficiary"
  }
  
  #make sf object with county shapefile
  map_df <- merge(mcnty_shapefile, map_df, by = "mcnty")
  
  map <- ggplot(data = map_df)+
    geom_sf(aes(fill = plot_val, geometry = geometry), color = NA)+
    geom_sf(data = state_shapefile, fill = NA, linewidth = .4) +
    labs(title = paste0(payer_title, " spending per ",denom),
         fill = "") +
    scale_fill_manual(values = cols, 
                      breaks = levels(factor(map_df$plot_val))[levels(factor(map_df$plot_val)) != "NA"],
                      na.value = "#838484")+
    theme(legend.position = "bottom",
          legend.justification = "top",
          legend.margin = margin(t = -10, unit = "pt"),  # Adjust top margin of the legend to pull it closer
          legend.spacing.y = unit(0, "cm"),
          plot.margin = margin(0, 0, 1, 0, "cm"),
          title = element_text(size = 12),
          text = element_text(size = 10),
          axis.ticks = element_blank(),
          axis.text = element_blank(),
          panel.background = element_blank()) 
  plot_list[[length(plot_list) + 1]] <- map
}

## Arranging PDF layout of maps
payer_maps <- arrangeGrob(grobs = plot_list, nrow = 2, ncol = 2) 
title_grob <- text_grob("Figure 2. Age/sex standardized health care spending per capita and per beneficiary by US county in 2019", size = 16)

# make county map a grob object to make compatible with arrangeGrob
county_map_grob <- ggplotGrob(county_map)

layout <- arrangeGrob(title_grob, county_map_grob, payer_maps, nrow=3, ncol=1, heights=c(0.05, 1, 1.5))

# Save out
file_name <- "Figure_2.pdf"
full_path <- paste0(out_dir, file_name)

pdf(file = full_path, width = 14, height = 16)
grid.draw(layout)
dev.off()


