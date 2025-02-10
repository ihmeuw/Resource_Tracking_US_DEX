library(tidyverse)
library(data.table)

map_by_mcnty <- function(df, fill_var){
  mcnty_shape <- fread("/FILEPATH/mcnty_map_shape_df.csv")
  state_shape <- fread("/FILEPATH/state_map_shape_df.csv")
  
  data <- merge(df, mcnty_shape, by="mcnty", allow.cartesian = TRUE)
  setnames(data, fill_var, "fill_var")
  
  g <- ggplot(data) + 
    geom_polygon(aes(x = long, y = lat, group = group, fill = fill_var)) + 
    geom_path(data=state_shape, aes(x=long, y=lat, group=group), colour="black", size=0.05) + 
    scale_x_continuous("", breaks=NULL, expand=c(0,0)) +
    scale_y_continuous("", breaks=NULL, expand=c(0,0)) +
    coord_fixed(ratio=1) + 
    theme_bw(base_size=10) +
    # scale_fill_viridis_d(option = "E") +
    theme(panel.border=element_blank(), axis.title=element_blank(),
          legend.position="bottom", legend.title=element_text(size=8),
          title=element_text(hjust=0.05)) + 
    labs(fill = fill_var)
  
  return(g)
}

