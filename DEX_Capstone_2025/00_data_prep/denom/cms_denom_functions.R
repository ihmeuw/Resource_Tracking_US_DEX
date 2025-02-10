#########################################
# Functions for CMS denominators
#########################################

## --------------------
## Function to test  zip imputation validation
## and save out results
## --------------------
zip_validation<- function(df, pop, source, timestamp, state) {
  test<-copy(df)
  pop1<-copy(pop)
  if (source == 'MDCD_MAX'){
    setnames(test, "age", "AGE")
    setnames(test, "lang_race_cd", "race_cd")
    setnames(test, "EL_RSDNC_ZIP_CD_LTST", "ZIP_CD")
    setnames(pop1, "EL_RSDNC_ZIP_CD_LTST", "ZIP_CD")
  }
  if (source == 'MDCD_TAF'){
    setnames(test, "age", "AGE")
    setnames(test, "zip", "ZIP_CD")
    setnames(pop1, "zip", "ZIP_CD")
  }
  setnames(df, 'race_cd_imp', 'race_cd', skip_absent=TRUE)
  
  test <- test[race_cd %in% c('WHT','BLCK','HISP','API','AIAN'), .(year_id, AGE, race_cd, ZIP_CD, sex_id, st_resi)]
  if (test[,.N]>0){
    #make age_bins
    test[AGE<25,age_bin:='0-25']
    test[AGE>=25 & AGE <65,age_bin:='25-65']
    test[AGE>=65,age_bin:='65+']
    #keep only valid zipcodes
    test$ZIP_CD[test$ZIP_CD == '00000'] <- NA
    test[,ZIP_CD:=as.character(ZIP_CD)]
    test <- test[!is.na(ZIP_CD)]
    if (test[,.N]>0){
      message('Assigning race')
      setDT(test)
      test[, group := .GRP, by = c('age_bin', 'sex_id', 'ZIP_CD')]
      group_key <- unique(test[,.(ZIP_CD,age_bin,sex_id,group)])
      
      pop1 <- merge(group_key,pop1, by = c('age_bin', 'sex_id', 'ZIP_CD'), all.x=T)
      zero_pop_zips <- unique(pop1[is.na(weight)]$group)
      
      ## Can't assign the age-sex-zips where pop is zero
      test <- test[!(group %in% zero_pop_zips)]
      
      if (test[,.N]>0){
        assign_race <- function(x){
          dt <- copy(test[group==x])
          sample_pop <- copy(pop1[group==x])
          dt[,zip_race_cd:= sample(x = sample_pop$race, 
                                   size = nrow(dt), 
                                   replace = T,
                                   prob = sample_pop$weight)]
          return(dt)} 
        
        test <- rbindlist(lapply(unique(test$group), assign_race))
        test[,`:=`(group=NULL)]
        test[, match := ifelse(race_cd == zip_race_cd, 1, 0 )]
        #rename race_cd
        setnames(test, "race_cd", "orig_race_cd")
        test <- test[, .(count = .N), by = .(year_id, st_resi, age_bin, orig_race_cd, zip_race_cd, match)]
        test[, source := source]
        
        #save out
        write_dataset(test,
                      existing_data_behavior = c("overwrite"),
                      path = paste0('FILEPATH'),
                      basename_template = paste0(state, "_part{i}.parquet"),
                      partitioning = c("source","year_id"))
      }
    }
  }
  message('Done saving validation data')
}