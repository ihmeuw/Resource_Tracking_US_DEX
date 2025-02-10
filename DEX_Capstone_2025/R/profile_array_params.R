#' --------------------------------------------------------------
#' FUNCTION: profile_array_params
#' PURPOSE: Supply a job id of an SLURM array job and return a data table
#'          with all COMPLETED job ids and the memory and time used.
#'          *Note that it will not return failed or OOM/OOT job ids.
#'          Can then be used to merge on your array job parameters 
#'          to determine exact memory and time each job requires. 
#'          Note that this function uses sacct so it can be utilized
#'          while the job is still running, it will just return only 
#'          the completed jobs.
#' AUTHOR: Azalea Thomson
#' LAST UPDATE: 12/13/24
#' --------------------------------------------------------------


profile_array_params <- function(jid){
  pacman::p_load(data.table,dplyr,stringr,lubridate) #lubridate for converting time to numeric

  array <-  system(paste0("sacct -j ", jid," -o jobid%20,JobName,State,MaxRSS,Elapsed --units=GB"),intern = T) %>% as.data.table()
  # Remove the header and separator lines
  array <- array[-c(1,2)]
  # Split the single column into multiple columns based on one or more spaces
  setnames(array,'.','data')
  array <- array[data %like% 'COMPLETED' & data %like% 'batch']
  # array[,data:=gsub(paste0(jid,'_'),'',data)]
  array[,data:=sub("^\\s+", "", data)]
  split_columns <- strsplit(array$data, "\\s{1,}", perl = TRUE)
  
  # Convert the list into a data.table
  output <- rbindlist(lapply(split_columns, function(x) as.list(x)), use.names = F)
  output$V2 <- NULL
  setnames(output, c('V1','V3','V4','V5'), c('task','status','mem','time'))
  
  output[,task:=as.numeric(str_extract(task, "(?<=_)[^\\.]+"))] ## "task" column must be present in the params for merging
  output[,mem_num:=as.numeric(gsub('G','',mem))] ## Numeric value for memory in GB
  output[,time_num_hrs:= period_to_seconds(hms(time)) / 3600] ## Numeric value for time in hours
  return(output)
}





