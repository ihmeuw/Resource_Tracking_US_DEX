# Function to make a new versioned folder by number; ex v1 v2
# Function to make a folder best
# Author: Haley Lescinsky


versioned_folder <- function(replace_last = F,   #boolean, if T will find the last version (numerically) and return that. Otherwise will determine new folder
                             dir,  # directory to hold previous and future versioned folders
                             make_folder = T,   #boolean, do you want to make the new folder now? 
                             string = "v"  # folders will be versioned by string#, so v1, v2, v3 if string = "v"
                             ){
  # assumes numeric versioning after some string character
  versions <- list.dirs(dir, recursive = F, full.names = F)
  versions <- versions[grepl(paste0(string, "[0-9]"), versions)]
  versions <- as.numeric(gsub(string, "", versions))
  
  if(replace_last){
    if(length(versions)==0){
      stop("cannot replace last folder since there is no versioned folder")
    }
    new_version <- paste0(string, max(versions))
  }else{
    if(length(versions)==0){
      new_version <- paste0(string, 1)
    }else{
      new_version <- paste0(string, max(versions) + 1)
    }
  }
  if(make_folder){
    dir.create(paste0(dir, "/", new_version), recursive = T)
  }
  
  return(new_version)
}


mark_version_best <- function(dir, version, label = "best"){
  
  current_wd <- getwd()
  setwd(dir)
  
  # remove any previous symlink with the label
  tryCatch(expr = {system(paste0("unlink ", label))})
  
  # add the label
  system(paste0("ln -s ", version, " ", label))
  
  setwd(current_wd)
}


which_version_best <- function(dir, label = "best"){
  
  dir <- gsub(label, "", dir)
  
  version_dir <- system(paste0("readlink -f ", dir, "/", label, "/"), intern = T)
  best_version <- gsub("^.*\\/", "", version_dir)
  
  return(best_version)
  
}


confirm_delete_directory <- function(dir){
  
  if(dir.exists(dir)){
    
    message(" MANUAL STEP TO PROCEED !!!")
    
    check <- readline(paste0('Are you sure you want to delete: \n', dir, "?  \n type 'yes' to proceed"))
    if (check == 'yes'){
      print("OK - deleting contents")
      system(paste0("rm -r ", dir))
    }else{
      stop("Must enter 'yes' to continue")
    }
  }
  
}


confirm_version_correct <- function(version, version_type){
  
  message(" MANUAL STEP TO PROCEED !!!")
  
  check <- readline(paste0('Running with \n ', version_type, "=", version, " \n type 'yes' to proceed"))
  if (check != 'yes'){
    stop("Must enter 'yes' to continue")
  }
  
}




