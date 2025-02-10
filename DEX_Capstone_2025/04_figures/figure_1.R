## ==================================================
## Author(s): Drew DeJarnatt
## 
## Purpose: Capstone figure 1 - Age pyramid 
## ==================================================

library(data.table)
library(tidyverse)
library(gridExtra)
library(scales)
library(usmap)
library(arrow)
.libPaths( c( "FILEPATH" , .libPaths() ) )
library(ggcharts)
library(patchwork)
library(ggpubr)
library(ggpol)
library(grid)
Sys.umask(mode = 002)

## -----------------------------------
## Arguments and set up
## -----------------------------------
# specify scaled_version 
scaled_version <- "XX"

# data paths to pull from
national_data_path <- "FILEPATH"

out_dir <- paste0("FILEPATH", scaled_version,"FILEPATH")
if(!dir.exists(out_dir)){
  dir.create(out_dir, recursive = T)
}

# colors and labels to use 
# Use full payer name in plot titles and labels
payer_list <- list("mdcr" = "Medicare", 
                   "mdcd" = "Medicaid", 
                   "priv" = "Private Insurance", 
                   "oop" = "Out-of-Pocket")
# payer colors
payer_colors <- list("priv" =	"#E69F00", 
                     "mdcr" =	"#0E5EAE", 
                     "mdcd" =	"#009E73", 
                     "oop" =	"#ae3918")

#colors for type of care
toc_colors <- c(
  "ED" = "#9B110E",
  "AM" = "#0B775E",
  "HH" = "#F1B9B9",
  "IP" = "#35274A",
  "NF" = "#6e9ab5",
  "DV" = "#E69F00",
  "RX" = "#BC87E4")
# long names for types of care
toc_labels = c(
  "ED" = "Emergency Department",
  "AM" = "Ambulatory",
  "IP" = "Inpatient",
  "HH" = "Home Health",
  "NF" = "Nursing Facility",
  "DV" = "Dental",
  "RX" = "Pharmaceutical"
)

# age range labels
age_labels = c(
  `0` = "0 - <1",
  `1` = "1 - <5",
  `5` = "5 - <10",
  `10` = "10 - <15",
  `15` = "15 - <20",
  `20` = "20 - <25",
  `25` = "25 - <30",
  `30` = "30 - <35",
  `35` = "35 - <40",
  `40` = "40 - <45",
  `45` = "45 - <50",
  `50` = "50 - <55",
  `55` = "55 - <60",
  `60` = "60 - <65",
  `65` = "65 - <70",
  `70` = "70 - <75",
  `75` = "75 - <80",
  `80` = "80 - <85",
  `85` = ">= 85"
)

## -----------------------------------
## Reading in population
## -----------------------------------

## Population
pop <- fread("FILEPATH/pop_age_sex.csv")[year_id == 2019 & geo == "national"]

# Read in data used for payer age pyramids
pyramid_data1 <- open_dataset(national_data_path) %>% 
  filter(year_id == 2019, payer != "oth") %>%
  group_by(payer, age_group_years_start, sex_id, draw) %>%
  summarize(spend = sum(spend)) %>%
  group_by(payer, age_group_years_start, sex_id) %>%
  summarize(spend = mean(spend)) %>%

pyramid_data1[, age_label := factor(age_labels[as.character(age_group_years_start)],
                                    levels = c(unname(age_labels)))]
pyramid_data1[, group_spend := sum(spend), .(age_group_years_start, sex_id)]

# making pyramid
pyramid1 <- pyramid_data1 %>%
  mutate(spend = ifelse(sex_id == 1, spend*-1, spend),
         sex = ifelse(sex_id == 1, "Male", "Female"),
         sex = factor(sex, levels = c("Male", "Female")),
         spend_bil = spend/1e9) %>%
  ggplot(aes(as.factor(age_label), spend_bil, fill = factor(payer, levels = c("mdcr", "mdcd", "priv", "oop")))) +
  facet_share(~ sex, scales = "free_x", reverse_num = TRUE) +
  geom_bar(stat = "identity") +
  coord_flip() +
  scale_fill_manual(values = payer_colors, labels = payer_list, name = "Payer") +
  scale_y_continuous(breaks = seq(-160, 160, 40), labels = seq(-160, 160, 40)) +
  theme_classic() +
  labs(y = "Estimated total Spending (US$ billions, 2019 dollars)",
       title = "Panel A: Estimated spending by age, sex, and payer",
       subtitle = "Total health care spending in 2019: $2.4 trillion") +
  theme(axis.title.y = element_blank(),
        text = element_text(size = 12),
        plot.subtitle = element_text(size = 10))

# Read in data used for type of care age pyramids
pyramid_data2 <- open_dataset(national_data_path) %>%
  filter(year_id == 2019, payer != "oth") %>%
  group_by(toc, age_group_years_start, sex_id, draw) %>%
  summarize(spend = sum(spend)) %>%
  group_by(toc, age_group_years_start, sex_id) %>%
  summarize(spend = mean(spend)) %>%
  collect()

pyramid_data2 <- left_join(pyramid_data2, pop, by = c("age_group_years_start", "sex_id")) %>%
  mutate(spend_pc = spend/pop)
setDT(pyramid_data2)
pyramid_data2[, age_label := factor(age_labels[as.character(age_group_years_start)],
                                    levels = c(unname(age_labels)))]

## National spend per capita for pyramid 2 subtitle
nat_per_cap <- round(sum(pyramid_data1$spend)/sum(pop$pop))

# need to add a title a maybe make some aesthetic changes to facet labels
pyramid2 <- pyramid_data2 %>%
  mutate(spend_pc = ifelse(sex_id == 1, (spend_pc*-1)/1e3, spend_pc/1e3),
         sex = ifelse(sex_id == 1, "Male", "Female"),
         sex = factor(sex, levels = c("Male", "Female"))) %>%
  ggplot(aes(age_label, spend_pc, fill = factor(toc, levels = c("DV", "ED", "HH", "RX", "NF", "IP", "AM")))) +
  facet_share(~ sex, scales = "free_x", reverse_num = TRUE) +
  geom_bar(stat = "identity") +
  coord_flip() +
  scale_fill_manual(values = toc_colors, labels = toc_labels, name = "Type of Care") +
  theme_classic() +
  labs(y = "Estimated spending per capita (US$ thousands, 2019 dollars)",
       title = "Panel B: Estimated spending per capita by age, sex, and type of care",
       subtitle = paste0("National average spending per capita in 2019: $", scales::comma(nat_per_cap))) +
  theme(axis.title.y = element_blank(),
        text = element_text(size = 12),
        plot.subtitle = element_text(size = 10))

# layout - to cleanly add title to top of figure
pyramids <- arrangeGrob(grobs = list(pyramid1, pyramid2), nrow = 1, ncol = 2) 
title_grob <- text_grob("Figure 1. Age pyramids of total spending by payer and spending per capita by type of care in 2019", size = 16)
layout <- arrangeGrob(title_grob, pyramids, nrow=2, ncol=1, heights=c(0.1, 1))

# setting file path
file_name <- "Figure_1.pdf"
full_path <- paste0(out_dir, file_name)

pdf(file = full_path, width = 16, height = 5)
grid.draw(layout)
dev.off()