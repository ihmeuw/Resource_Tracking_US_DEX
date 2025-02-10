## ==================================================
## Author(s): Sawyer Crosby, Haley Lescinsky 
## Date: Jan 31, 2025
## Purpose: Functions to submit jobs on a SLURM cluster 
## ==================================================

## =================================================
## Array Job 
## =================================================
library(stringr)

SUBMIT_ARRAY_JOB <- function(
    name, # string
    script, # string filepath
    queue = "all.q", # string "all.q" or "long.q"
    memory = "5G", # string "#G"
    threads = "3", # string "#"
    time = "1:00:00", # string "##:##:##"
    n_jobs, # string "#"
    throttle = "500", # string "#", do not exceed 500
    archive = F, # boolean
    r_shell = "/FILEPATH/execRscript.sh", # string
    img = NULL, # default to most recent image
    error_dir = NULL,  # default to FILEPATH/{user}
    output_dir = NULL, # default to FILEPATH/{user}s
    hold = NULL, ## string - jid to wait on
    user_email = NULL,
    test = FALSE, ## if TRUE, subsets to only first task and runs that
    args = NULL ## extra arguments to add as vector, if you want
){
  
  
  if(length(error_dir)==0){
    error_dir = paste0("/FILEPATH/", Sys.info()[['user']], "/errors")
  }
  if(length(output_dir)==0){
    output_dir = paste0("/FILEPATH/", Sys.info()[['user']], "/output")
  }
  
  
  if (test == TRUE){
    n_jobs <- 1
  }
  
  ## make sbatch
  command <-  paste("sbatch",
                    "-J", name, 
                    "--mem", memory,
                    "-c", threads,
                    "-t", time,
                    "-A proj_dex",
                    "-p", queue,
                    ifelse(archive, "-C archive", ""),
                    "-a", paste0("1-", n_jobs, "%", throttle),
                    "-e", paste0(error_dir, "/%x_%A_%a.e"),  # name_masterjid_taskid.e
                    "-o", paste0(output_dir, "/%x_%A_%a.o"),  # name_masterjid_taskid.o
                    "-D ./", # set working directory
                    "--parsable",
                    sep = " ") 
  
  if(length(user_email)> 0){
    command <- paste0(command, " --mail-type=BEGIN,END"," --mail-user=", user_email) #email sent when job begins and ends
  }
  
  ## add hold if exists
  if(length(hold) > 0){
    command <- paste0(command, " -d ", paste0(hold, collapse = ","))
  }
  
  ## if specified image, format correctly
  if(length(img)>0){
    img <- paste("-i", img)
  }
  
  ## launch job
  jid <- system(paste(command, r_shell, img, "-s", script, paste(args, collapse = " ")), intern = T)
  
  ## clean JID
  jid <- str_extract(jid, "[:digit:]+")
  
  ## and return jid
  return(jid)
}


## =================================================
## One Job  
## =================================================

SUBMIT_JOB <- function(
    name, # string
    script, # string filepath
    queue = "all.q", # string "all.q" or "long.q"
    memory = "5G", # string "#G"
    threads = "3", # string "#"
    time = "1:00:00", # string "##:##:##"
    archive = F, # boolean
    r_shell = "/FILEPATH/execRscript.sh", # string
    img = NULL, # default to most recent image
    error_dir = NULL,  # default to FILEPATH/{user}
    output_dir = NULL, # default to FILEPATH/{user}
    hold = NULL, ## string - jid to wait on
    args = NULL ## extra argument to add as vector, if you want
){
  
  
  
  if(length(error_dir)==0){
    error_dir = paste0("/FILEPATH/", Sys.info()[['user']], "/errors")
  }
  if(length(output_dir)==0){
    output_dir = paste0("/FILEPATH/", Sys.info()[['user']], "/output")
  }
  
  ## make sbatch
  command <-  paste("sbatch",
                    "-J", name, 
                    "--mem", memory,
                    "-c", threads,
                    "-t", time,
                    "-A proj_dex",
                    "-p", queue,
                    ifelse(archive, "-C archive", ""),
                    "-e", paste0(error_dir, "/%x_%j.e"), # name_jid.e
                    "-o", paste0(output_dir, "/%x_%j.o"), # name_jid.o
                    "-D ./",
                    "--parsable",
                    sep = " ") 
  
  ## add hold if exists
  if(length(hold) > 0){
    command <- paste0(command, " -d ", paste0(hold, collapse = ","))
  }
  
  ## if specified image, format correctly
  if(length(img)>0){
    img <- paste("-i", img)
  }
  
  ## launch job
  jid <- system(paste(command, r_shell, img,"-s", script, paste(args, collapse = " ")), intern = T)
  
  ## and return jid
  return(jid)
  
}

