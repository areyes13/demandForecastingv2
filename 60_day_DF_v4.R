# 1A - LOAD PACKAGES -----------------------------------------------------------
#set Java heap size to max
options(java.parameters = "-Xmx8g")
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jdk1.8.0_102')

#set working directory (all output / input files are stored here)
setwd("~/Demand Forecasting II/Archive")

#load packages
library(randomForest)
library(rJava)
library(RJDBC)
library(dplyr)
library(data.table)
library(lubridate)
library(tidyr)
library(zoo)
library(stringr)
library(cowsay)

#set random seed for consistent RF results
set.seed(666)

#set current day for naming
current.run <- Sys.Date()

# 2A - LOAD DATA ---------------------------------------------------------------
load('fresh.data.saved')


## clark edit: added a space after 'data in progress' so it reads 'data in progress '
if(fresh.data) {
  load(paste0('data in progress ', current.run, '.saved'))
  #CLARK EDIT: defined master as equivelant to data
  master <- data
  ## CLARK EDIT need to load candidate data
}

if(!fresh.data) {
  #query new data: connection details to PMP
  jcc = JDBC("com.ibm.db2.jcc.DB2Driver",
             "C:/Users/SCIP2/Documents/DB2 Driver/db2jcc4.jar")
  load('credentials.saved')
  
  conn = dbConnect(jcc,
                   as.character(credentials[3]),
                   user=as.character(credentials[1]),
                   password=as.character(credentials[2]))
  rm(credentials)
  )
  
  #OPEN SEAT TABLE
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.OPNSET_T")
  OPNSET_T <- fetch(rs,-1)
  
  #couldn't get SQL filters to work in original DB2 query without blowing it up ;__;
  OPNSET_T <- tbl_df(OPNSET_T) %>%
    filter(SET_TYP_CD == 'MP', WRK_CNTRY_CD %in% c('US', 'CA'))
  
  #OPEN POSITION TABLE
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.OPNSET_POS_T")
  OPNSET_POS_T <- fetch(rs,-1)
  
  save(OPNSET_POS_T, file = paste0('OPNSET_POS_T_', current.run, '.saved'))
  
  #OPEN DETAIL TABLE
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.OPNSET_DTL_T")
  OPNSET_DTL_T <- fetch(rs,-1)
  
  #STATUS CODE REF TABLE
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.POS_STAT_RESN_T")
  POS_STAT_RESN_T <- fetch(rs,-1)
  
  #CONTRACT ORG REF TABLE
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.CNTRCT_OWNG_ORG_T")
  CNTRCT_OWNG_ORG_T <- fetch(rs,-1)
  
  CNTRCT_OWNG_ORG_T$Unit <- with(CNTRCT_OWNG_ORG_T, as.factor(ifelse(
    grepl('GBS', CNTRCT_OWNG_ORG_NM),
    'GBS',
    ifelse(grepl('GTS', CNTRCT_OWNG_ORG_NM), 'GTS',
           'OTHER')
  )))
  
  #CONTRACT TYPE
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.CNTRCT_TYP_T")
  CNTRCT_TYP_T <- fetch(rs,-1)
  
  #CONTRACT TYPE
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.INDSTR_T")
  INDSTR_T <- fetch(rs,-1)
  INDSTR_T$DEL_FLG <- NULL
  
  #COUNTRY SECUTIRY CLEARANCE CODE REF
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.CNTRY_SEC_CLRNCE_T")
  CNTRY_SEC_CLRNCE_T <- fetch(rs,-1)
  
  #COUNTRY SECUTIRY CLEARANCE CODE REF
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.SEC_CLRNCE_TYP_T")
  SEC_CLRNCE_TYP_T <- fetch(rs,-1)
  
  
  #OPNSET_POS_CAND_T
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.OPNSET_POS_CAND_T")
  OPNSET_POS_CAND_T <- fetch(rs,-1)
  
  
  #OPNSET_POS_CAND_STAT
  rs <-
    dbSendQuery(conn, "SELECT * FROM BCSPMP.OPNSET_POS_CAND_STAT_T")
  OPNSET_POS_CAND_STAT_T <- fetch(rs,-1)
  
  #CAND_STAT_NM
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.CAND_STAT_T")
  CAND_STAT_T <- fetch(rs,-1)
  
  #CAND_T
  rs <- dbSendQuery(conn, "SELECT * FROM BCSPMP.CAND_T")
  CAND_T <- fetch(rs,-1)
  setnames(CAND_T, 'CNTRY_CD', 'cand.ctry')
  
  #save(OPNSET_POS_CAND_T, OPNSET_POS_CAND_STAT_T, CAND_STAT_T, file = 'candidate tbls.saved')
  
  #MERGES
  master <- left_join(OPNSET_T, OPNSET_POS_T, by = "OPNSET_ID")
  
  master <- left_join(master, OPNSET_DTL_T, by = "OPNSET_ID")
  
  master <-
    left_join(master, POS_STAT_RESN_T[c('STAT_RESN_CD', 'STAT_RESN_DESC')], by = "STAT_RESN_CD")
  
  master <-
    left_join(master, CNTRCT_OWNG_ORG_T[c("CNTRCT_OWNG_ORG_ID", "CNTRCT_OWNG_ORG_NM", "Unit")], by = 'CNTRCT_OWNG_ORG_ID')
  
  master <-
    left_join(master, CNTRCT_TYP_T[c('CNTRCT_TYP_ID', 'CNTRCT_TYP_DESC')], by = 'CNTRCT_TYP_ID')
  
  master <- left_join(master, INDSTR_T, by =  "INDSTR_ID")
  
  security <-
    left_join(CNTRY_SEC_CLRNCE_T, SEC_CLRNCE_TYP_T, by = "SEC_CLRNCE_TYP_ID")
  security <-
    security[c('CNTRY_SEC_CLRNCE_ID', 'SEC_CLRNCE_TYP_DESC')]
  
  master <- left_join(master, security, by = "CNTRY_SEC_CLRNCE_ID")
  
  #recode some pos with STATUS CODES and FULFILLMENT CHANNEL that indicates SUBK
  master <- tbl_df(master) %>%
    mutate(
      STAT_RESN_DESC.og = STAT_RESN_DESC,
      STAT_RESN_DESC = replace(
        STAT_RESN_DESC,
        STAT_RESN_CD %in% c('WB', 'WE', 'WG') &
          PREF_FULFLMNT_CHNL_CD == 'SUBC',
        'Staffed by contractor/other'
      )
    )
  
  #map candidate features
  candidate <- tbl_df(OPNSET_POS_CAND_T) %>%
    filter(OPNSET_ID %in% master$OPNSET_ID)
  
  candidate.type <-
    left_join(OPNSET_POS_CAND_STAT_T,
              CAND_STAT_T[1:2],
              by = c('CAND_OPNSET_STAT_ID' = 'CAND_STAT_ID'))
  
  candidate <-
    left_join(candidate, candidate.type, by = 'OPNSET_POS_CAND_ID')
  
  candidate <-
    left_join(candidate, CAND_T[c('CAND_ID', 'cand.ctry', 'CAND_SRC_CD')], by = 'CAND_ID')
  
  save(master, file = paste0('master ', current.run, '.saved'))
  
  
  # 2B - DATA REFRESH ----------------------------------------------------
  #bring up the last date the model was run
  load('60 day last run.saved')
  
  paste0('Last model run: ', current.run - last.run, ' day(s) ago')
  #load last test set
  load(paste0('60 day testing', last.run, '.saved'))
  #get position ids from last test set
  
  last.pos <- unique(testing$OPNSET_POS_ID)
  

  
  #grab fields to update in testing data from last query
  actualizer <- tbl_df(master) %>%
    filter(OPNSET_POS_ID %in% last.pos) %>%
    select(OPNSET_POS_ID, STAT_RESN_DESC, WTHDRW_CLOS_T) %>%
    mutate(
      STAT_RESN_DESC = ifelse(
        is.na(STAT_RESN_DESC),
        F,
        STAT_RESN_DESC == 'Staffed by contractor/other'
      ),
      WTHDRW_CLOS_T = ymd(str_sub(WTHDRW_CLOS_T, 1, 10)),
      #set status to 'false' if position did not close
      STAT_RESN_DESC = replace(STAT_RESN_DESC, is.na(WTHDRW_CLOS_T), F)
    )
  
  #drop vars about to be updated
  test.old <- tbl_df(testing) %>%
    select(-STAT_RESN_DESC,-WTHDRW_CLOS_T)
  
  #update the values by merging the 2 tables
  actualizer <-
    merge(test.old, actualizer, 'OPNSET_POS_ID', all.x = T)
  #now there's 2 paths:
  #A) position was CLOSED / WITHDRAWN since the last run: we want to STACK with OLD TRAINING
  #B) positions is STILL OPEN since last run: we want to stack FROZEN VIEW with OLD TRAINING and feed last query data to NEW TESTING
  
  #LOAD LAST TRAINING:
  load(paste0('60 day train', last.run, '.saved'))
  #stack last run's testing set
  train <- rbind(train, actualizer)
  save(train, file = paste0('60 day train', current.run, '.saved'))
  
  #select POSITIONS STILL OPEN FROM LAST UPDATE
  open.test <- filter(actualizer, is.na(WTHDRW_CLOS_T)) %>%
    select(OPNSET_POS_ID)
  open.test <- merge(open.test, master, by = 'OPNSET_POS_ID', all.x = T)
  
  #filter query to select only new data for analysis (STUFF NOT ALREADY IN 'TRAINING')
  new.pos <- master %>%
    filter(!OPNSET_POS_ID %in% train$OPNSET_POS_ID)
  
  #stack open pos from LAST RUN and new data from CURRENT QUERY
  tbls <- list(open.test, new.pos)
  tbls <- lapply(tbls, function(x)
    as.data.frame(lapply(x, as.character)))
  library(plyr)
  tbls <- rbind.fill(tbls)
  detach("package:plyr", unload = TRUE)
  
  #this is the new data that needs to flow thru the next sections to generate features
  #we will need to split into testing/training AFTER all features are built and then stack where needed
  save(tbls, file = paste0('60 day new positions raw', current.run, ',saved'))
  master <- tbls
  
  # 2C - DATA PREP ----------------------------------------------------------
  start <- proc.time()
  save(start, file = 'start time.saved')
  rm(start)
  #select which variables to keep
  vars <-
    c(
      "OPNSET_ID",
      "OPNSET_POS_ID",
      "STAT_RESN_DESC",
      "INDSTR_NM",
      "STRT_DT",
      "END_DT",
      "CRE_T",
      "LST_UPDT_T",
      'OPN_T',
      "WTHDRW_CLOS_T",
      "CNTRCT_OWNG_ORG_NM",
      "Unit",
      "BND_LOW",
      "BND_HIGH",
      "PAY_TRVL_IND",
      "WRK_RMT_IND",
      "CNTRCT_TYP_DESC",
      "FULFILL_RISK_ID",
      "SEC_CLRNCE_TYP_DESC",
      "OWNG_CNTRY_CD",
      "OPPOR_OWNR_NOTES_ID",
      "WRK_CNTRY_CD",
      "JOB_ROL_TYP_DESC",
      "SKLST_TYP_DESC",
      "SET_TYP_CD",
      "WRK_CTY_NM",
      "URG_PRIRTY_IND",
      "PREF_FULFLMNT_CHNL_CD",
      "NEED_SUB_IND"
    )
  
  #drop variables
  data <- master[vars]
  
  #set proper data types
  #get rid of the &^%!@#%$@ trailing spaces in JRSS fields....ugh
  #(factors)
  facts <-
    c(
      "OPNSET_POS_ID",
      "STAT_RESN_DESC",
      "INDSTR_NM",
      "CNTRCT_OWNG_ORG_NM",
      "OPPOR_OWNR_NOTES_ID",
      "Unit",
      "PAY_TRVL_IND",
      "WRK_RMT_IND",
      "CNTRCT_TYP_DESC",
      "FULFILL_RISK_ID",
      "BND_LOW",
      "BND_HIGH",
      "SEC_CLRNCE_TYP_DESC",
      "OWNG_CNTRY_CD",
      "WRK_CNTRY_CD",
      "JOB_ROL_TYP_DESC",
      "SKLST_TYP_DESC",
      "SET_TYP_CD",
      "WRK_CTY_NM",
      'URG_PRIRTY_IND'
    )
  data[facts] <-
    lapply(data[facts], function(x)
      as.factor(str_trim(x)))
  rm(facts)
  
  #recode relevant variables to binary
  #urgent = TRUE
  data$URG_PRIRTY_IND <- with(data, URG_PRIRTY_IND == 'Y')
  
  #contractor = TRUE (double check AFFL / SUBC interpretation valid)
  data$PREF_FULFLMNT_CHNL_CD <-
    with(data, PREF_FULFLMNT_CHNL_CD == 'SUBC')
  
  #need subk = TRUE
  data$NEED_SUB_IND <- with(data, NEED_SUB_IND == 'Y')
  
  #contractor position = TRUE; this is target
  data$STAT_RESN_DESC <-
    with(data,
         ifelse(
           is.na(STAT_RESN_DESC),
           F,
           STAT_RESN_DESC == 'Staffed by contractor/other'
         ))
  
  #pay travel / lodging
  data$PAY_TRVL_IND <- with(data, PAY_TRVL_IND == 'Y')
  
  #Work remotely
  data$WRK_RMT_IND <- with(data, WRK_RMT_IND == 'Y')
  
  #### THESE ARE MISSING ;__;
  #candidates in play  = TRUE (don't know where this is)
  #data$Has.Candidates.in.Play <- data$Has.Candidates.in.Play == 'Y'
  #revenue impact = TRUE (don't know where this one is)
  #data$Revenue.Impact <- with(data, Revenue.Impact == 'Y')
  
  #also can't find "FULFILL_RISK_ID" in the current data dictionary but it is in OPNSET_T
  #WILL RUN AS FACTOR FOR NOW
  
  # 2D - DATE CLEANUP -------------------------------------------------------
  #use black magick and blood sacrifices to deal with projecting / scanning into other years (dec 15 -> jan 16)
  month.vars <-
    c("STRT_DT",
      "END_DT",
      "CRE_T",
      "LST_UPDT_T",
      'OPN_T',
      "WTHDRW_CLOS_T")
  
  data[month.vars] <-
    lapply(data[month.vars], function(x)
      ymd(str_sub(x, 1, 10)))
  
  #might not use these (??) - resets these to the first of the month for any projection stuffs
  data$Created.floor <- with(data, floor_date(CRE_T, 'month'))
  #in Natalie's 2015 file this was 'original start date'. Assuming this is the same thing (???)
  data$OG.Start.floor <- with(data, floor_date(STRT_DT, 'month'))
  data$Close.floor <- with(data, floor_date(WTHDRW_CLOS_T, 'month'))
  data$Close.week <- with(data, floor_date(WTHDRW_CLOS_T, 'week'))
  
  #this is AGE OF RECORD (how long has it been sitting in the system?)
  to.today <- function(date) {
    today <- current.run
    days <- as.integer(round(difftime(today, date, units = 'days'),
                             digits = 0))
    return(days)
  }
  data$record.age <- to.today(data$CRE_T)
  
  #create lead days var (time btwn creation to expected start date)
  lead.time <- function(created, start) {
    days <- as.integer(round(difftime(start, created, units = 'days'),
                             digits = 0))
    return(days)
  }
  
  data$Lead.time.days <- with(data, lead.time(CRE_T, STRT_DT))
  
  #position requests created AFTER work started (???)
  time.vortex <- filter(data, CRE_T > STRT_DT)
  
  #needed w/in 30 days
  from.today <- function(date) {
    today <- current.run
    days <- as.integer(round(difftime(date, today, units = 'days'),
                             digits = 0))
    return(days)
  }
  data$needed30.days <- data$Lead.time.days <= 30
  
  #length of project
  day.diff <- function(start, end) {
    days <- as.integer(round(difftime(end, start, units = 'days'),
                             digits = 0))
    return(days)
  }
  
  data$project.duration <- with(data, day.diff(STRT_DT, END_DT))
  
  
  #put data for prediction into separate df and save for later
  filter30 <- function(start.date) {
    day30 <- current.run + days(30)
    flag <- start.date <= day30
    return(flag)
  }
  
  filter60 <- function(start.date) {
    day30 <- current.run + days(31)
    day60 <- day30 + days(30)
    flag <- start.date %within% interval(day30, day60)
    return(flag)
  }
  
  
  # 2E - CANDIDATE MAPPING --------------------------------------------------
  #set up helper function for NA values from casting
  replacer <- function(x) {
    values <- replace(x, is.na(x), 0)
    return(values)
  }
  
  candidate$OPNSET_ID <- as.factor(as.character(candidate$OPNSET_ID))
  #get start date into candidate table
  candidate <-
    left_join(candidate, unique(data[c('OPNSET_ID', 'OG.Start.floor')]), by = 'OPNSET_ID')
  
  #filter to grab the last available status 1 MONTH BEFORE START DATE
  candidate <- candidate %>%
    mutate(CAND_OPNSET_STAT_T = ymd_hms(CAND_OPNSET_STAT_T),
           start.hms = ymd_hms(paste(OG.Start.floor, '00:00:00'))) %>%
    group_by(OPNSET_POS_CAND_ID, OPNSET_ID) %>%
    filter(CAND_OPNSET_STAT_T < start.hms,
           CAND_OPNSET_STAT_T == max(CAND_OPNSET_STAT_T))
  
  candidate <- unique(candidate)
  
  #how many candidates per SEAT
  candidate.count <- candidate %>%
    group_by(OPNSET_ID, OG.Start.floor) %>%
    summarise(candidate.count = length(unique(OPNSET_POS_CAND_ID)),
              csa.src = sum(CAND_SRC_CD == 'C')) %>%
    mutate(csa.src = replacer(csa.src))
  
  #how many candidates per SEAT DESCRIPTION CODE
  candidate.type <- candidate %>%
    group_by(OPNSET_ID, OG.Start.floor, CAND_STAT_NM) %>%
    summarise(candidate.type = length(unique(OPNSET_POS_CAND_ID))) %>%
    group_by(OPNSET_ID, OG.Start.floor) %>%
    spread(CAND_STAT_NM, candidate.type) %>%
    mutate_each(funs(replacer))
  
  candidate.count <-
    left_join(candidate.count,
              candidate.type,
              by = c('OPNSET_ID', 'OG.Start.floor'))
  candidate.count <- candidate.count %>%
    mutate(active.cands = (candidate.count - Confirmed - Withdrawn - `<NA>`) /
             candidate.count)
  
  #map back into the data
  data <-
    left_join(data, candidate.count, by = c('OPNSET_ID', 'OG.Start.floor'))
  candidate.vars <-
    setdiff(colnames(candidate.count), c('OPNSET_ID', 'OG.Start.floor'))
  
  data[candidate.vars] <- lapply(data[candidate.vars], replacer)
  
  save(data, file = paste0('data in progress ', current.run, '.saved'))
}

# 3A - JOB ROLE FEATURES ------------------------------------------------
#collapse by JR to create new features
jr.ft <- tbl_df(data) %>%
  filter(!is.na(WTHDRW_CLOS_T)) %>%
  group_by(JOB_ROL_TYP_DESC, OG.Start.floor) %>%
  summarise(jr.count = length(OPNSET_POS_ID))

month.vec <- seq.Date(ymd('2000-01-01'), floor_date(current.run, 'month'), 'month')

month.expanded <- with(jr.ft, CJ(unique(as.factor(JOB_ROL_TYP_DESC)), month.vec))
setnames(month.expanded, colnames(month.expanded), c('JOB_ROL_TYP_DESC', 'OG.Start.floor'))

month.expanded <- left_join(month.expanded, jr.ft, by = c('JOB_ROL_TYP_DESC', 'OG.Start.floor')) %>%
  group_by(JOB_ROL_TYP_DESC) %>%
  arrange(desc(OG.Start.floor)) %>%
  mutate(jr.count = lead(jr.count, 2),
         jr.count = replace(jr.count, is.na(jr.count), 0))

# 3B - JRSS ACTUAL WEEKLY DEMAND TABLE ---------------------------------------------
#how many positions were ACTUALLY CLOSED/WITHDRAWN each month

jrss.week <- tbl_df(data) %>%
  filter(!is.na(Close.floor), WTHDRW_CLOS_T >= CRE_T) %>%
  #filter(year(Close.floor) >= 2012) %>% #in case we want to limit data size
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, Close.week) %>%
  summarise(actual = length(OPNSET_POS_ID),
            sub.actual = sum(STAT_RESN_DESC)) %>%
  mutate(JRSS = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - '))

week.vec <- with(jrss.week, seq.Date(min(Close.week), floor_date(current.run, 'week'), 'week'))

week.expanded <- with(jrss.week, CJ(unique(c(JRSS)), week.vec))
setnames(week.expanded, colnames(week.expanded), c('JRSS', 'Close.week'))

#map in the separate JRs and SSssssss's to this dummy table
mapper <- unique(jrss.week[c('JRSS', 'JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC')])
week.expanded <- left_join(week.expanded, mapper, by = 'JRSS') %>%
  select(-JRSS)

#now get the data in there and set NAs to 0
week.expanded <- left_join(week.expanded, jrss.week, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'Close.week'))  %>%
  select(-JRSS) %>%
  mutate(actual.wk = replace(actual, is.na(actual), 0),
         sub.actual.wk = replace(sub.actual, is.na(sub.actual), 0)) %>%
  select(-actual, -sub.actual)

#need this bc cumsum doesn't handle NAs
cumsum.NA <- function(x) {
  value <- cumsum(ifelse(is.na(x), 0, x)) + x*0
  return(value)
}

#sliding 4 week cumulative sum function
cumul.msum <- function(x) {
  #need to split eval in 2 due to sliding window not generating values until width is matched
  first <- cumsum.NA(x[1:4])
  roller <- rollapply(data = x,
                      width = 4,
                      FUN = cumsum.NA,
                      align = 'right',
                      fill = NA)[,4]
  #the first 4 observations (width) are NA, replace with regular cumsum output
  roller[1:4] <- first
  return(roller)
} 

#big dplyr chain to get ACTUAL JRSS features
week.expanded <- tbl_df(week.expanded) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  #filter(JOB_ROL_TYP_DESC == 'Application Developer', SKLST_TYP_DESC == 'COBOL') %>%
  #we need to 'lag' the data so that we only use the info we had AT THAT TIME (no peeking into future)
  mutate(tot.lag = lag(actual.wk, n = 8),
         sub.lag = lag(sub.actual.wk, n = 8),
         tot.4wk = cumul.msum(tot.lag),
         sub.4wk = cumul.msum(sub.lag),
         tot.4lag = lag(tot.4wk, n = 4),
         sub.4lag = lag(sub.4wk, n = 4),
         tot.delta = tot.4wk - tot.4lag,
         sub.delta = sub.4wk - sub.4lag,
         actual.wk = tot.lag,
         sub.actual.wk = sub.lag) %>%
  #drop the lagged data
  select(-tot.lag, -sub.lag)

# 3C - JRSS ACTUAL MONTHLY DEMAND TABLE  ----------------------------------------
#FIRST WE GET OUR MONTHLY JRSS ACTUALS
#how many positions were ACTUALLY CLOSED/WITHDRAWN each month
jrss.counter <- tbl_df(data) %>%
  filter(!is.na(Close.floor), WTHDRW_CLOS_T >= Created.floor) %>%
  #filter(year(Close.floor) >= 2012) %>% #in case we want to limit data size
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, Close.floor) %>%
  summarise(actual = length(OPNSET_POS_ID),
            sub.actual = sum(STAT_RESN_DESC)) %>%
  mutate(JRSS = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - '))

#there are no entries for months without observations. we need to map these to get rolling avgs
#create a sequence of all months between the earliest and latest available date
date.vec <- with(jrss.counter, seq.Date(min(Close.floor), max(Close.floor), 'month'))

#create dummy table with all JRSS/month combinations
jrss.expanded <- with(jrss.counter, CJ(unique(c(JRSS)), date.vec))
setnames(jrss.expanded, colnames(jrss.expanded), c('JRSS', 'Close.floor'))

#map in the separate JRs and SSssssss's to this dummy table
mapper <- unique(jrss.counter[c('JRSS', 'JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC')])
jrss.expanded <- left_join(jrss.expanded, mapper, by = 'JRSS') %>%
  select(-JRSS)

#dump into a placeholder
expanded <- jrss.expanded

#now get the data in there and set NAs to 0
jrss.expanded <- left_join(jrss.expanded, jrss.counter, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'Close.floor'))  %>%
  select(-JRSS) %>%
  mutate(actual = replace(actual, is.na(actual), 0),
         sub.actual = replace(sub.actual, is.na(sub.actual), 0))

#sliding 12 month cumulative sum function
cumul.yrsum <- function(x) {
  #need to split eval in 2 due to sliding window not generating values until width is matched
  first <- cumsum.NA(x[1:12])
  roller <- rollapply(data = x,
                      width = 12,
                      FUN = cumsum.NA,
                      align = 'right',
                      fill = NA)[,12]
  #the first 12 observations (width) are NA, replace with regular cumsum output
  roller[1:12] <- first
  return(roller)
} 

#big dplyr chain to get ACTUAL JRSS features
jrss.expanded <- tbl_df(jrss.expanded) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  #we need to 'lag' the data so that we only use the info we had AT THAT TIME (no peeking into future)
  mutate(tot.lag = lag(actual, n = 2),
         sub.lag = lag(sub.actual, n = 2),
         #we create a rolling 2 period avg based on the lagged values
         r2tot = rollapply(data = tot.lag, width = 2,FUN = mean,
                           align = 'right', fill = NA, na.rm = F),
         r2sub = rollapply(data = sub.lag, width = 2, FUN = mean,
                           align = 'right', fill = NA, na.rm = F),
         #these will create a cumulative sum of actual demand (sliding 12 month window)
         tot.12cs = cumul.yrsum(tot.lag),
         sub.12cs = cumul.yrsum(sub.lag)) %>%
  #drop the lagged data
  select(-tot.lag, -sub.lag)

# 3D - JRSS PROJECTED MONTHLY DEMAND TABLE --------------------------------
#use the 'expanded' object we made in previous section
setnames(expanded, 'Close.floor', 'OG.Start.floor')

#use filters to get projection counts for all JRSS / MONTH combinations (ONLY using data available in previous month!)
jrss.projection <- tbl_df(data) %>%
  filter(!is.na(OG.Start.floor)) %>%
  #filter(year(Close.floor) == 2015) %>% #in case we want to limit data size
  #positions created BEFORE the start of PREVIOUS MONTH (which positions w start date in March were created BEFORE/ON FEB 1st?)
  filter(CRE_T <= OG.Start.floor %m-% months(2)) %>%
  #which positions were still OPEN at the start of the PREVIOUS MONTH? (which positions w start date in March were still open on FEB 1st?)
  mutate(open = ifelse(is.na(WTHDRW_CLOS_T), T, 
                       WTHDRW_CLOS_T > OG.Start.floor %m-% months(2))) %>%
  filter(open) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, OG.Start.floor) %>%
  #now collapse by JRSS and get counts
  summarise(prj.tot = length(OPNSET_ID))

#get table of positions by MONTH THEY ARE SUPPOSED TO START
jrss.projection <- left_join(expanded, 
                             jrss.projection, 
                             by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'OG.Start.floor'))

#create the projection features!
jrss.projection <- tbl_df(jrss.projection) %>%
  mutate(prj.tot = replace(prj.tot, is.na(prj.tot), 0)) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(r3prj = rollapply(data = prj.tot, width = 3,FUN = mean,
                           align = 'right', fill = NA, na.rm = F),
         #these will create a cumulative sum of actual demand (sliding 12 month window)
         prj.12cs = cumul.yrsum(prj.tot))

#LET'S BRING IT TOGETHER!
setnames(jrss.projection, 'OG.Start.floor', 'ref.month')
setnames(jrss.expanded, 'Close.floor', 'ref.month')
jrss.tbl <- left_join(jrss.expanded, jrss.projection, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'ref.month'))

#create a few more features
jrss.tbl <- tbl_df(jrss.tbl) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(m.tot.dmd = prj.12cs/12,
         m.tot.act = tot.12cs/12,
         m.sub.act =  sub.12cs/12,
         tot.heat12 = ifelse(m.tot.dmd == 0, 0, prj.tot/m.tot.dmd),
         tot.heat2 = ifelse(r2tot == 0, 0, prj.tot/r2tot))

#create top 50 JRSS feature
top.jrss <- tbl_df(jrss.expanded) %>%
  #filter(ref.month == max(ref.month)) %>%
  filter(ref.month == floor_date(current.run, 'month')) %>%
  select(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, tot.12cs, sub.12cs) %>%
  #group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(tot.rank = row_number(desc(tot.12cs)),
         sub.rank = row_number(desc(sub.12cs)))

#set up H/M/L tiers for subk demand based on PAST 12 MONTHS
jrss.tiers <- top.jrss %>%
  mutate(demand.tier = ifelse(sub.rank <=30, 'HIGH',
                              ifelse(sub.rank >30 & sub.rank <= 80, 'MEDIUM',
                                     'LOW'))) %>%
  select(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, demand.tier)

#split and select TOTAL  
top.tot <- top.jrss %>%
  filter(tot.rank <=50) %>%
  select(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(jrss.tot = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - '))
#split and select SUB
top.sub <- top.jrss %>%
  filter(sub.rank <=50) %>%
  select(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(jrss.sub = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - '))

#join back to single table
top.jrss <- full_join(top.tot, top.sub, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC'))
top.jrss <- full_join(top.jrss, jrss.tiers, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC'))

#map JRSS features back into POSITION DATA
#df <- tbl_df(data) %>%
#  filter(year(Close.floor) >= 2015)
df <- data

## CLARK edit added "file = to save statement
save(df, file = '60 day df pre filter.saved')

# 3E - TEST / TRAIN FILTER ------------------------------------------------
#need to split df by test/train to map in the last available JRSS values to prediction set
df <- tbl_df(df) %>%
  #exclude dates beyond the time period we're projecting
  filter(STRT_DT <= (current.run + days(60)), 
         !STRT_DT %within% interval(current.run, current.run + days(30)),
         (WTHDRW_CLOS_T >= OG.Start.floor - months(1) | is.na(WTHDRW_CLOS_T)),
         CRE_T <= OG.Start.floor - months(1)) %>%
  #start date may be up to 30 days in advance, but take any positions that are still open
  mutate(type = as.factor(
    ifelse(filter60(STRT_DT) & is.na(WTHDRW_CLOS_T),
           'TEST',
           'TRAIN')))

#create JRSS counts from testing
upcoming.tbl <- tbl_df(df) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, OG.Start.floor) %>%
  summarise(upcoming.open = sum(WTHDRW_CLOS_T >= OG.Start.floor | is.na(WTHDRW_CLOS_T))) %>%
  ungroup() %>%
  mutate(JRSS = paste0(JOB_ROL_TYP_DESC, ' - ', SKLST_TYP_DESC))

jrss.vector <- unique(upcoming.tbl$JRSS)
upcoming.expanded <- CJ(unique(jrss.vector), month.vec)
setnames(upcoming.expanded, colnames(upcoming.expanded), c('JRSS', 'OG.Start.floor'))
up.map <- unique(upcoming.tbl[c('JRSS', 'JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC')])
upcoming.expanded <- left_join(upcoming.expanded, up.map, by = 'JRSS') %>%
  select(-JRSS)

upcoming.expanded <- left_join(upcoming.expanded, upcoming.tbl, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', "OG.Start.floor"))
upcoming.expanded <- upcoming.expanded %>%
  mutate(upcoming.open = replace(upcoming.open, is.na(upcoming.open), 0)) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(upcoming.open = lag(upcoming.open, n = 1)) %>%
  select(-JRSS)

df <- left_join(df, upcoming.expanded, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'OG.Start.floor'))

#create pos counts from data (how many pos per seats? how many csa candidates per position?)
post.count <- df %>%
  group_by(OPNSET_ID, csa.src) %>%
  summarise(post.count = length(OPNSET_POS_ID)) %>%
  mutate(csa.posts = csa.src/post.count) %>%
  select(-csa.src)

df <- left_join(df, post.count, by = 'OPNSET_ID')

#otherwise they will get all NA vals from numeric features (since we don't have any actual data for dates that haven't happened, duh)
testing <- filter(df, type == 'TEST')
train <- filter(df, type == 'TRAIN')
train <- left_join(train, week.expanded, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'Close.week'))
train <- left_join(train, jrss.tbl, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'OG.Start.floor' = 'ref.month'))
train <- left_join(train, month.expanded, by = c('JOB_ROL_TYP_DESC', 'OG.Start.floor'))

#shift these new training positions to a temp df
train.new <- train
#load the training set USED IN PREVIOUS RUNS (this is the running training df)
load(paste0('60 day train', last.run, '.saved'))

train <- train %>%
  select(-jrss.tot, -jrss.sub, -demand.tier, -top.city, -top.owner, -Month, - Year)

#stack the new positions with features into master training set
#gotta make sure the master training set is LOCKED before we start running (ie, need same features in old & current)
train <- rbind(train, train.new)


#select last FULL week (don't include the current week)
latest.week <- week.expanded %>%
  filter(Close.week == floor_date(current.run, 'week') - weeks(1))
testing <- testing %>%
  mutate(dummy.week = floor_date(current.run, 'week') - weeks(1))
testing <- left_join(testing, latest.week, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'dummy.week' = 'Close.week'))
testing <- select(testing, -dummy.week)

#select last MONTH from JRSS.TBL
latest.month <- jrss.tbl %>%
  filter(ref.month == floor_date(current.run, 'month'))
testing <- testing %>%
  mutate(dummy.month = floor_date(current.run, 'month'))
testing <- left_join(testing, latest.month, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'dummy.month' = 'ref.month'))
testing <- select(testing, -dummy.month)

#select last MONTH from MONTH.EXPANDED
latest.jr <- month.expanded %>%
  filter(OG.Start.floor == floor_date(current.run, 'month'))
testing <- testing %>%
  mutate(dummy.month = floor_date(current.run, 'month'))
testing <- left_join(testing, latest.jr, by = c('JOB_ROL_TYP_DESC', 'dummy.month' = 'OG.Start.floor'))
testing <- select(testing, -dummy.month)


#restack for data type processing (we'll split them later)
df <- rbind(train, testing)

#create work city var
past.12 <- interval(floor_date(current.run, 'month') - months(12), floor_date(current.run, 'month'))
city <- tbl_df(df) %>%
  filter(OG.Start.floor %within% past.12) %>%
  group_by(WRK_CTY_NM) %>%
  summarise(count = length(unique(OPNSET_POS_ID))) %>%
  ungroup() %>%
  mutate(rank = row_number(desc(count)),
         top.city = TRUE) %>%
  filter(rank <= 30) %>%
  arrange(rank) %>%
  select(-count, -rank)

#create opp owner var
owner <- tbl_df(df) %>%
  filter(OG.Start.floor %within% past.12, OPPOR_OWNR_NOTES_ID != 'NONE') %>%
  group_by(OPPOR_OWNR_NOTES_ID) %>%
  summarise(count = length(unique(OPNSET_POS_ID))) %>%
  ungroup() %>%
  mutate(rank = row_number(desc(count)),
         top.owner = TRUE) %>%
  filter(rank <= 30) %>%
  arrange(rank) %>%
  select(-count, -rank)



#we map these variables AFTER STACKING - they change each run FOR EVERYONE
df <- select(df, -jrss.tot, -jrss.sub, -top.city, -top.owner)
#map the top 50 tot / sub most common JRSS in past 12 months
df <- left_join(df, top.jrss, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC'))

#map in top 30 cities with most requests from past 12 months
df <- merge(df, city, by = 'WRK_CTY_NM', all.x = T)

#map in top 30 cities with most requests from past 12 months
df <- merge(df, owner, by = 'OPPOR_OWNR_NOTES_ID', all.x = T)

#replace NAs with 'OTHER'
df[c('jrss.tot', 'jrss.sub', 'top.city', 'top.owner')] <- lapply(df[c('jrss.tot', 'jrss.sub', 'top.city', 'top.owner')], 
                                                                 function(x) replace(x, is.na(x), 'OTHER'))


load('start time.saved')
proc.time() - start
save(df, file = paste0('60-day observation tbl ', current.run, '.saved'))

# 4A - PREP DATA FOR RANDOM FORESTS ------------------------------------------------------
df$Month <- with(df, month(STRT_DT, label = T))
df$Year <- with(df, year(STRT_DT))

facts <- c("OPNSET_POS_ID", "STAT_RESN_DESC", "INDSTR_NM", "CNTRCT_OWNG_ORG_NM", "OPPOR_OWNR_NOTES_ID", "Unit",
           "PAY_TRVL_IND", "WRK_RMT_IND", "CNTRCT_TYP_DESC", "FULFILL_RISK_ID", "BND_LOW", "BND_HIGH", 'Month', 'Year',
           "SEC_CLRNCE_TYP_DESC", "OWNG_CNTRY_CD", "WRK_CNTRY_CD", "JOB_ROL_TYP_DESC", "SKLST_TYP_DESC", 
           "SET_TYP_CD", "WRK_CTY_NM", 'URG_PRIRTY_IND', 'PREF_FULFLMNT_CHNL_CD', 'NEED_SUB_IND', "needed30.days",
           'jrss.tot', 'jrss.sub', 'top.city', 'top.owner', 'demand.tier')

#set any missing factor values to 'NONE' so we can run them thru RFs
df[facts] <- lapply(df[facts], function(x) {
  val <- as.character(x)
  vals <- ifelse(is.na(val), 'NONE', val)
  vals <- as.factor(vals)
  return(vals)
})

#use continuous variables + prob output from factor forest
## clark edit post count, csa posts and candidate vars were not found in df, so I commented out

num.inputvars <- c('STAT_RESN_DESC', "Lead.time.days", "record.age","r2tot", "r2sub", "tot.12cs", "sub.12cs","prj.tot", 
                   "r3prj", "prj.12cs", "m.tot.dmd", "m.tot.act", "m.sub.act", "tot.heat12", 
                   "tot.heat2", "project.duration", "jr.count", "actual.wk", "sub.actual.wk",
                   "tot.4wk", "sub.4wk", "tot.4lag", "sub.4lag", "tot.delta", "sub.delta", 'upcoming.open')#, 'post.count', 'csa.posts', candidate.vars)

#recode NA as -100 (so we don't throw out any records just bc of NAs)
df[setdiff(num.inputvars, 'STAT_RESN_DESC')] <- lapply(df[setdiff(num.inputvars, 'STAT_RESN_DESC')], function(x) {
  val <- ifelse(x < 0, NA, x)
  val <- ifelse(is.na(val), -100, val)
  return(val)
})

#split into training / testing & save
#testing <- filter(df, type == 'TEST')
#train <- filter(df, type == 'TRAIN')

###Clark Edit, type not found, using differnt sytax
###
testing <- df[df$type == 'TEST', ]
train <- df[df$type == 'TRAIN', ]

save(testing, file = paste0('60 day testing', current.run, '.saved'))
save(train, file = paste0('60 day train', current.run, '.saved'))
#save last run date for using next refresh
last.run <- current.run
save(last.run, file = 'last run.saved')

# 4B - TRAIN FACTOR FOREST ------------------------------------------------
#grab position IDs and JRSS for mapping back into RF outputs
id <- data.frame(Position.ID = train$OPNSET_POS_ID, 
                 JR = train$JOB_ROL_TYP_DESC, 
                 SS = train$SKLST_TYP_DESC, 
                 country = train$WRK_CNTRY_CD,
                 Start.dt = train$OG.Start.floor)

#put the factor variable data into input table (remove id vars, we'll map these in later with 'id')
facts.input <- train[setdiff(facts, c('OPNSET_ID', 'OPNSET_POS_ID', 'JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', "OPPOR_OWNR_NOTES_ID"))]

################ UH OH, RF CAN'T HANDLE VARIABLES WITH 53+ LEVEL. NEED TO DROP WORK CITY ;__;
#code to check if any variables have more than 53 levels
#unlist(lapply(facts.input, function(x) length(levels(x))))
#probably could set up automatic check that drops any vars that violate this....
facts.input$WRK_CTY_NM <- NULL
facts.input$OWNG_CNTRY_CD <- NULL

detach("package:ggplot2", unload=TRUE)
detach("package:dplyr", unload=TRUE)

#drop time sensitive values - THESE MAY BE BIASING THE TRAINING DATA ON INFO WE DON'T HAVE 1 MONTH IN ADVANCE
facts.input$PREF_FULFLMNT_CHNL_CD <- NULL
facts.input$NEED_SUB_IND <- NULL

t <- proc.time()
fact.forest <- randomForest(STAT_RESN_DESC ~ ., 
                            data = facts.input,
                            mtry = 5,
                            importance = T,
                            #proximity = T,
                            do.trace = 100,
                            ntree = 500,
                            nodesize = 100,
                            na.action = na.omit)
proc.time()-t
print(fact.forest)
varImpPlot(fact.forest)

#grab probability predictions
fact.pred <- predict(fact.forest, type="prob")[, 2]

fact.out <- data.frame(predicted = fact.forest$predicted, 
                       actual = fact.forest$y,
                       prob = fact.pred)
fact.out <- cbind(id, fact.out)


# 4B - TRAIN CONTINUOUS FOREST --------------------------------------------
#get the numeric vars
num.input <- train[num.inputvars]
#include the factor forest probabilities as INPUTS to numeric forest
num.input$fact.out <- fact.out$prob
num.input$`<NA>` <- NULL

#run that shizznit
t <- proc.time()
num.forest <- randomForest(STAT_RESN_DESC ~ ., 
                           data = num.input,
                           mtry = 5,
                           importance = T,
                           #proximity = T,
                           do.trace = 100,
                           ntree = 500,
                           nodesize = 100,
                           na.action = na.omit)
proc.time()-t
print(num.forest)
varImpPlot(num.forest)

library(cowsay)
sink(paste0('60 day randomForest diagnostics ', current.run, '.txt'))
cat(say('Clark is a butt!', by='chicken', type = 'string'), sep = '\n')
print(fact.forest)
print(num.forest)
sink()

#training out: NUMERIC
num.pred <- predict(num.forest, type="prob")[, 2]

num.out <- data.frame(predicted = num.forest$predicted, 
                      actual = num.forest$y,
                      prob = num.pred)
num.out <- cbind(id, num.out)

#variable importance table (plot later)
n.imp <- as.data.frame(importance(num.forest))
n.imp$type <- 'NUMERIC'
f.imp <- as.data.frame(importance(fact.forest))
f.imp$type <- 'FACTOR'
importance.tbl <- rbind(n.imp, f.imp)
importance.tbl$var <- as.factor(rownames(importance.tbl))
rownames(importance.tbl) <- NULL
importance.tbl$var <- factor(importance.tbl$var, levels = importance.tbl$var[order(importance.tbl$MeanDecreaseAccuracy)])


# 5A - PREDICT ------------------------------------------------------------
#select factor variables for PREDICTION SET
test.facts <- testing[colnames(facts.input)]
#get FACTOR predictions
factor.predictions <- data.frame(pos.id = testing$OPNSET_POS_ID,
                                 pred = predict(fact.forest, newdata = test.facts, type = 'response'),
                                 prob = predict(fact.forest, newdata = test.facts, type = 'prob')[,2])

#select numeric variables for PREDICTION SET
test.num <- testing[num.inputvars]
#include factor ouput
test.num$fact.out <- factor.predictions$prob
#get NUMERIC predictions
num.predictions <- data.frame(pos.id = testing$OPNSET_POS_ID,
                              pred = predict(num.forest, newdata = test.num, type = 'response'),
                              prob = predict(num.forest, newdata = test.num, type = 'prob')[,2])

test.id <- data.frame(Position.ID = testing$OPNSET_POS_ID, 
                      JR = testing$JOB_ROL_TYP_DESC, 
                      SS = testing$SKLST_TYP_DESC, 
                      city = testing$WRK_CTY_NM, 
                      country = testing$WRK_CNTRY_CD,
                      actual = testing$STAT_RESN_DESC)
#map eval outcomes
final.pred <- cbind(test.id, num.predictions)
save(final.pred, file = paste0('60 day prediction output ', current.run, '.saved'))

#make pretty importance chart
#library(ggplot2)

#importance.plot <- ggplot(importance.tbl, aes(x = var, y = MeanDecreaseAccuracy, color = type))+
#  geom_point(size = 4)+
#  geom_point(size = 4, shape = 1, color = 'black')+
#  geom_vline(xintercept = 0)+
#  coord_flip()+
#  theme_bw()+
#  theme(panel.grid.major.x = element_blank(),
#        panel.grid.major.y =   element_line(colour = "grey70", size=.7, linetype = 'dashed'),
#        strip.text.x = element_text(size=10.5, face="bold"), 
#        strip.text.y = element_text(size=10.5, face="bold"),
#        axis.title.x=element_text(face="bold",size=14),
#        axis.title.y=element_text(face="bold",size=14),
#        axis.text.x=element_text(size=10),
#        axis.text.y=element_text(size=10),
#        plot.title=element_text(face="bold",size=24))

#ggsave(plot= importance.plot,
#       paste0("60 day Random Forest feature importance", current.run, '.png'),
#       h=250,
#       w=200,
#       unit="mm",
#       type="cairo-png",
#       dpi=300)

#importance.plot


# CLARK ERRORS ------------------------------------------------------------

binary.code <- function(n, probs) {
  #sample from binomial distribution N times, assuming size of 100, and base probability from random forest output
  nums <- rbinom(n, 100, probs)/100
  #count where random sample is less than or equal to base prob
  check <- nums >= runif(n, 0 , 1)
  return(check)
}

#apply to every row
simulator <- sapply(final.pred$prob, function(x) binary.code(100, x))
simulator <- as.data.frame(t(simulator))
#join back with id vars
simulator <- cbind(final.pred[c('Position.ID', 'JR', 'SS')], simulator)

library(dplyr)
library(tidyr)
#convert to vertical structure
simulator <- simulator %>%
  gather(run, value, -Position.ID, - JR, -SS)
#summarise by JRSS / simulation run
simulator <- tbl_df(simulator) %>%
  group_by(JR, SS, run) %>%
  summarise(count = sum(value))
#go back to wide structure
simulator <- tbl_df(simulator) %>%
  spread(run, count) %>%
  mutate(JR = paste(JR, SS, sep = ' - ')) %>%
  select(-SS)
setnames(simulator, colnames(simulator), c('JRSS', paste0('sim', 1:100)))

#error bounds
bounded <- apply(simulator[2:101], 1, function(x) round(quantile(x, c(.25, .75)), 0))
bounded <- as.data.frame(t(bounded))

bounded <- cbind(simulator$JRSS, bounded)

setnames(bounded, colnames(bounded), c('JRSS', 'Q1', 'Q3'))

limiter <- function(x) {
  x <- as.integer(x)
  value <- ifelse(x <= 4, x + round(runif(1, 1, 4),0), x)
  return(value)
}
bounded$Q3 <- apply(bounded, 1, function(x) limiter(x[3]))

#only for eval
outcome <- tbl_df(testing) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  summarise(count = sum(as.logical(STAT_RESN_DESC))) %>%
  mutate(JRSS = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - ')) %>%
  ungroup() %>%
  select(JRSS, count, -JOB_ROL_TYP_DESC, -SKLST_TYP_DESC)

outcome <- left_join(outcome, bounded, by = 'JRSS')

outcome$inbound <- with(outcome, count >= Q1 & count <= Q3)

model.guess <- final.pred %>%
  group_by(JR, SS) %>%
  summarise(roundsum = round(sum(prob), 0)) %>%
  mutate(JRSS = paste(JR, SS, sep = ' - ')) %>%
  ungroup() %>%
  select(-JR, -SS)

outcome <- left_join(outcome, model.guess, by = c('JRSS'))

outcome.plot <- ggplot(outcome, aes(x = count, color = inbound))+
  geom_abline(slope = 1, intercept = 0, linetype = 'dashed', color = 'gray30')+
  geom_errorbar(aes(ymax = high95, ymin = low5), alpha = 1/4, size = 1.5) +
  geom_point(aes(y = roundsum), shape = 1, alpha = 1/3, size = 4, stroke = 2,  fill = 'white')+
  scale_x_continuous(limits = c(0,18), breaks = seq(0,20, by = 2), name = 'Actual Subk Demand', expand = c(0,0)) +
  scale_y_continuous(limits = c(0,18), breaks = seq(0,20, by = 2), name = 'Predicted Subk Range', expand = c(0,0))+
  scale_color_discrete(name="Within Predicted Range")+
  coord_flip() +
  theme_bw() +
  guides(colour = guide_legend(override.aes = list(alpha = 1)))+
  theme(panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_blank(),
        strip.text.x = element_text(size=10.5, face="bold"), 
        strip.text.y = element_text(size=10.5, face="bold"),
        axis.title.x=element_text(face="bold",size=14),
        axis.title.y=element_text(face="bold",size=14),
        axis.text.x=element_text(size=10),
        axis.text.y=element_text(size=10),
        plot.title=element_text(face="bold",size=24),
        legend.position = "bottom")

ggsave(plot= outcome.plot,
       paste0("Eval - JRSS prediction bounds July", current.run, '.png'),
       h=200,
       w=200,
       unit="mm",
       type="cairo-png",
       dpi=300)

outcome.plot


# 60 DAY REGRESSION (UGHGHGHGHGHG) ---------------------------------------
run <- FALSE
if(run) {
  #summarise OOB training to JRSS
  train.eval <- num.out %>%
    group_by(JR,
             SS,
             #country,
             Start.dt) %>%
    filter(year(Start.dt) >= 2015) %>%
    summarise(
      N = length(Position.ID),
      actual = sum(as.logical(actual)),
      roundsum = round(sum(prob), 0),
      log.act = log(actual + 1),
      log.round = log(roundsum + 1),
      rt.act = sqrt(actual),
      rt.round = sqrt(roundsum)
    )
  train.eval$random <-
    with(train.eval, round(sum(actual) / sum(N) * N, 0))
  train.eval$rt.rand <- with(train.eval, sqrt(random))
  
  #summarise raw data to get actual closed demand per JRSS/MONTH
  demanded <- tbl_df(data) %>%
    #exclude dates beyond the time period we're projecting
    filter(
      STRT_DT <= (current.run + days(60)),!STRT_DT %within% interval(current.run, current.run + days(30)),
      (WTHDRW_CLOS_T >= OG.Start.floor | is.na(WTHDRW_CLOS_T)),!(filter60(STRT_DT) &
                                                                   is.na(WTHDRW_CLOS_T))
    ) %>%
    group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, OG.Start.floor) %>%
    summarise(Total.N = length(OPNSET_POS_ID),
              subks = sum(STAT_RESN_DESC, na.rm = T))
  
  
  #do stupid regressions
  lm.eval <- with(train.eval, lm(rt.act ~ rt.round))
  summary(lm.eval)
  
  rand.eval <- with(train.eval, lm(rt.act ~ rt.rand))
  summary(rand.eval)
  
  sink(paste0('60 day regression diagnostics ', current.run, '.txt'))
  cat(say('THE HORROR, THE HORROR!', by = 'cat', type = 'string'), sep = '\n')
  cat("\n")
  cat("\n")
  print("lm.eval:")
  summary(lm.eval)
  cat("\n")
  cat("\n")
  print("rand.eval:")
  summary(rand.eval)
  sink()
  
  #collapse RF testing output to JRSS predictions
  final.eval <- final.pred %>%
    group_by(JR, SS) %>%
    summarise(
      N = length(Position.ID),
      actual = sum(as.logical(actual)),
      roundsum = round(sum(prob), 0)
    )
  
  #GAZE INTO THE ABYSS
  minority.report <- function(roundsum, linear.model, err) {
    roundsum <- sqrt(roundsum)
    intercept <- coefficients(linear.model)[[1]]
    slope <- coefficients(linear.model)[[2]]
    prediction <- intercept + slope * roundsum + err
    prediction <- round(prediction ^ 2, 0)
    return(prediction)
  }
  
  err <-
    unname(sample(size = length(final.eval$roundsum), residuals(lm.eval)))
  final.eval$Expected.Demand <-
    minority.report(final.eval$roundsum, lm.eval, err)
  
  ggplot(final.eval, aes(roundsum, Expected.Demand)) +
    geom_point()
}

# EXCEL OUTPUT ------------------------------------------------------------
library(XLConnect)

#grab the vars we need for the DUMB REPORT
library(dplyr)
reporter <- data %>%
  filter(OPNSET_POS_ID %in% unique(testing$OPNSET_POS_ID)) %>%
  select(OPNSET_ID, OPNSET_POS_ID, Unit, JOB_ROL_TYP_DESC, SKLST_TYP_DESC, CRE_T, WRK_CTY_NM, WRK_CNTRY_CD,
         WTHDRW_CLOS_T, INDSTR_NM, OPPOR_OWNR_NOTES_ID, BND_LOW, BND_HIGH, STRT_DT, END_DT, project.duration, PAY_TRVL_IND, Unit) %>%
  mutate(Open.Dummy = 'OPEN',
         Low.Band = BND_LOW, 
         High.Band = BND_HIGH, 
         Opp.Industry = INDSTR_NM, 
         Business.Unit = Unit,
         Pay.Travel.Expenses = PAY_TRVL_IND)

#merge with DUMB PREDICTION OUTPUTS
reporter <- left_join(num.predictions, reporter, by = c('pos.id' = "OPNSET_POS_ID"))

#fix DUMB VARIABLE NAMES
setnames(reporter, 
         setdiff(colnames(reporter), c('Low.Band', 'High.Band', 'Opp.Industry', 'Business.Unit', 'Pay.Travel.Expenses')),
         c('Position.ID', 'Prediction', 'Probability', "Seat.ID", 'Unit', 'Job.Role', 
           'Skillset', 'Create.Date', 'Work.City', 'Work.Country', 'Close.Date', 'Industry', 'Opp.Owner.ID', 
           'Band-low', 'Band-high', 'Start.Date', 'End.Date', 'Engagement.Duration', 'Pay.Travel', 'Status'))

##clark edit: multiply percentage by 100
reporter$Probability.percentage <- with(reporter, paste0(round(Probability, 2)*100, '%'))
reporter$Subk.prob.flag <- with(reporter, ifelse(Probability >= .6, 'High Subk Likelihood',
                                                 ifelse(Probability < .6 & Probability > .3, 'Medium Subk Likelihood',
                                                        'Low Subk Likelihood')))
reporter <- reporter %>%
  mutate(JRSS = paste0(Job.Role, ' - ', Skillset))
reporter <- left_join(reporter, bounded, by = 'JRSS') %>%
  select(-Job.Role, -Skillset)
setnames(reporter, c('Q1','Q3'), c('Low.Estimate', 'High.Estimate'))

#regression


reporter$Expected.Demand <- '--'
reporter$Expected.Additional.Demand <- '--'

names(reporter) <- gsub(x = names(reporter),
                        pattern = "\\.",
                        replacement = " ")

#load workbook & figure out sheet names
setwd("~/Demand Forecasting II/Templates")

rep.arch <- 'C:/Users/SCIP2/Documents/Demand Forecasting II/Archive'

reports <- c(paste0('60 day - IBM Output Report ', current.run, '.xlsx'),
             paste0('60 day - Vendor-US Output Report ', current.run, '.xlsx'),
             paste0('60 day - Vendor-CA Output Report ', current.run, '.xlsx'))

for(i in 1:length(reports)) {
  sink(paste0(i+3, '.txt'))
  cat(paste(rep.arch, reports[i], sep = '/'))
  sink()
}

t <- proc.time()

wb <- loadWorkbook("60 Day Subk Demand Forecast Report BLANK TEMPLATE.xlsx")
getSheets(wb)
#this sheet name is DUMB. Need to update for final version
#delete data in the sheet
writeWorksheet(wb, reporter, sheet = "Data", startRow = 1, startCol = 1,
               header = TRUE)

#output IBM report
setwd("~/Demand Forecasting II/Archive")
saveWorkbook(wb, file = paste0('60 day - IBM Output Report ', current.run, '.xlsx'))
setwd("~/Demand Forecasting II/Output Reports")
saveWorkbook(wb, file = '60 day - IBM Output Report.xlsx')

#output US VENDOR report
setwd("~/Demand Forecasting II/Templates")
wb <- loadWorkbook("60 Day Vendor Subk Demand Forecast Report BLANK TEMPLATE.xlsx")
getSheets(wb)
writeWorksheet(wb, 
               reporter %>% 
                 filter(`Work Country` == 'US') %>% 
                 select(-`Opp Owner ID`), 
               sheet = "Data", startRow = 1, startCol = 1,
               header = TRUE)

setwd("~/Demand Forecasting II/Archive")
saveWorkbook(wb, file = paste0('60 day - Vendor-US Output Report ', current.run, '.xlsx'))
setwd("~/Demand Forecasting II/Output Reports")
saveWorkbook(wb, file = '60 day - Vendor-US Output Report.xlsx')

#output CA VENDOR report
setwd("~/Demand Forecasting II/Templates")
wb <- loadWorkbook("60 Day Vendor Subk Demand Forecast Report BLANK TEMPLATE.xlsx")
writeWorksheet(wb, 
               reporter %>% 
                 filter(`Work Country` == 'CA') %>% 
                 select(-`Opp Owner ID`), 
               sheet = "Data", startRow = 1, startCol = 1,
               header = TRUE)

setwd("~/Demand Forecasting II/Archive")
saveWorkbook(wb, file = paste0('60 day - Vendor-CA Output Report ', current.run, '.xlsx'))
setwd("~/Demand Forecasting II/Output Reports")
saveWorkbook(wb, file = '60 day - Vendor-CA Output Report.xlsx')

proc.time() - t

# run vbscript to refresh pivot table
shell.exec("C:/Users/SCIP2/Documents/Demand Forecasting II/Templates/Rscript.vbs") 

fresh.data <- F
save(fresh.data, file = 'fresh.data.saved')
setwd("~/Demand Forecasting II/Archive")
save.image("60 day ws.RData")

print('THE END...?')
#end of main script
