### CDSRG
### IDENTIFY RADIATION COURSES ALGORITHM 

################################################################################
### prepQuery()

prepQuery <- function(con, query=NULL, rows_at_time=attr(con, "rows_at_time")) {
  # test database connection and clear error log
  if (!RODBC:::odbcValidChannel(con)) {
    log_error("Invalid DB connection")
    return(FALSE)
  }
  tryCatch(
    odbcClearError(con),
    error=function(e) {
      log_error(e$message)
      return(FALSE)
    }
  )
  if (is.null(query)) {
    log_error("Missing value for 'query'")
    return(FALSE)
  }
  if (!is.character(query)) {
    log_warn("Converting from non-character value provided for 'query'")
    query <- as.character(query)
  }
  if (length(query) != 1) {
    log_error("Single value required for 'query'")
    return(FALSE)
  }
  if (nchar(query) < 1) {
    log_error("Empty 'query' provided")
    return(FALSE)
  }
  log_info("Prepping query: ", query)	
  if (.Call(RODBC:::C_RODBCQuery, attr(con, "handle_ptr"), query, as.integer(rows_at_time)) < 0) {
    log_error("Error evaluating query using provided DB connection")
    log_error(odbcGetErrMsg(con))
    return(FALSE)
  }
  return(TRUE)
}

################################################################################
### fetchQuery()

fetchQuery <- function(con, n=NULL, buffsize=1000, FUN=NULL, as.is=FALSE, ...) {
  # test database connection
  if (!RODBC:::odbcValidChannel(con)) {
    log_error("Invalid DB connection")
    return(FALSE)
  }
  cols <- .Call(RODBC:::C_RODBCNumCols, attr(con, "handle_ptr"))
  if (cols < 0L) {
    log_error("No data")
    return(FALSE)
  }
  cData <- .Call(RODBC:::C_RODBCColData, attr(con, "handle_ptr"))
  if (!is.numeric(n) | (length(n) != 1)) { 
    n <- 0
  }
  n <- max(0, floor(n), na.rm=TRUE)
  if (is.logical(as.is) & length(as.is) == 1) {
    as.is <- rep(as.is, length=cols)
  }
  else if (is.numeric(as.is)) {
    if (any(as.is < 1 | as.is > cols)) 
      log_warn("invalid numeric 'as.is' values: ", as.is[which(as.is < 1 | as.is > cols)])
    as.is <- as.is[which(as.is >= 1 & as.is <= cols)]
    i <- rep(FALSE, cols)
    i[as.is] <- TRUE
    as.is <- i
  }
  else if (is.character(as.is)) {
    as.is <- cData$names %in% as.is
  }
  if (length(as.is) != cols) {
    log_error("'as.is' has the wrong length ", length(as.is), " != cols = ", cols)
    return(FALSE)
  }
  as.is <- which(as.is)
  if (is.null(FUN)) {
    FUN <- return
    use.dots <- FALSE
  }
  else {
    tryCatch(
      FUN <- match.fun(FUN),
      error=function(e) {
        log_error(e$message)
        return(FALSE)				
      }
    )
    use.dots <- (...length() > 0L) & (length(formals(FUN)) > 1L)
  }
  counter <- 0
  nresults <- 0
  repeat {
    data <- .Call(RODBC:::C_RODBCFetchRows, attr(con, "handle_ptr"), n, buffsize, NA_character_, TRUE)
    if ((data$stat) < 0L) {
      if (counter > 0L) {
        log_info("Completed fetch (", nresults, " results)")
      }
      else if (data$stat == -1) {
        log_error(odbcGetErrMsg(con))
      }
      break
    }
    log_info("Fetching query results", 
             if (n > 0) { 
               paste(" (", floor(counter*n), "-", (counter+1)*n-1, ")", sep="") 
             }
             else {
               " (all)"
             }
    )
    counter <- counter + 1
    names(data$data) <- cData$names
    for (i in as.is) {
      tryCatch(
        switch(cData$type[i],
               int = data$data[[i]] <- as.integer(data$data[[i]]),
               smallint = data$data[[i]] <- as.integer(data$data[[i]]),
               decimal = data$data[[i]] <- as.numeric(data$data[[i]]),
               date = data$data[[i]] <- as.Date(data$data[[i]]),
               timestamp = data$data[[i]] <- as.POSIXct(data$data[[i]]),
               unknown = data$data[[i]] <- type.convert(data$data[[i]])
        ),
        error=function(e) {
          log_warn("Error converting ", cData$names[i], ": ", e$message)
        }
      )
    }
    tryCatch(
      if (use.dots) {
        forceAndCall(1, FUN, data$data, ...)
      }
      else {
        forceAndCall(1, FUN, data$data)
      },
      error=function(e) {
        log_error(e$message)
        log_error(odbcGetErrMsg(con))
        return(FALSE)
      }
    )
    nresults <- nresults + length(data$data[[1]])
  }
  return(TRUE)
}

################################################################################
### function for populating R hash environment with patient stay data

storeInHash <- function(x) {
  
  ### coerce datatypes; ICN must be char to use as hash key, and dates kept as char now and coerced to POSIX later
  
  x$PatientICN <- as.character(x$PatientICN)
  x$EventGeneralCategory  <- as.character(x$EventGeneralCategory)
  x$EventDateTime <- as.character(x$EventDateTime)
  x$NextEvent <- as.character(x$NextEvent)
  x$PrevEvent <- as.character(x$PrevEvent)
  
  
  temp <- function(ICN, EGC, EDT, EY, EM, ED, EW, EWd, tot, exp, hist, man, otv, phys, enc, hyp, ima, tx, plan, sim, se, nextE, prevE, DUNE, DSPE, HUNE, HSPE) {
    
    patdata <- patenv[[ICN]]
    
    if (is.null(patdata)) {
      
      patdata <- data.frame("PatientICN" = numeric(), "EventGeneralCategory" = character(), "EventDateTime" = character(), 
                            "EventYear" = numeric(), "EventMonth" = numeric(), "EventDay" = numeric(), "EventWeek" = numeric(), "EventWeekday" = numeric(),
                           "TotalNumEvents" = numeric(), "NumExposureEvents" = numeric(), "NumHistoryEvents" = numeric(), 
                           "NumManagementEvents" = numeric(), "NumOTVEvents" = numeric(), "NumPhysicsEvents" = numeric(), "NumEncounterEvents" = numeric(), 
                           "NumHyperthermiaEvents" = numeric(), "NumImagingEvents" = numeric(), "NumTreatmentEvents" = numeric(), 
                           "NumPlanningEvents" = numeric(), "NumSimEvents" = numeric(), "NumSideEffectEvents" = numeric(),
                           "NextEvent" = character(), "PrevEvent" = character(), "DaysUntilNextEvent" = numeric(), "DaysSincePrevEvent" = numeric(), 
                           "HoursUntilNextEvent" = numeric(), "HoursSincePrevEvent" = numeric())
      
    }
    
    newRow <- data.frame(as.numeric(ICN), EGC, EDT, EY, EM, ED, EW, EWd, tot, exp, hist, man, otv, phys, enc, hyp, ima, tx, plan, sim, se, nextE, prevE, DUNE, DSPE, HUNE, HSPE, stringsAsFactors = FALSE)
    colnames(newRow) <- c("PatientICN", "EventGeneralCategory", "EventDateTime", "EventYear", "EventMonth", "EventDay", "EventWeek", "EventWeekday", 
                          "TotalNumEvents", "NumExposureEvents", "NumHistoryEvents", "NumManagementEvents", "NumOTVEvents", "NumPhysicsEvents", "NumEncounterEvents",
                          "NumHyperthermiaEvents", "NumImagingEvents", "NumTreatmentEvents", "NumPlanningEvents", "NumSimEvents", "NumSideEffectEvents",
                          "NextEvent", "PrevEvent", "DaysUntilNextEvent", "DaysSincePrevEvent", "HoursUntilNextEvent", "HoursSincePrevEvent")
    
    patdata <- rbind(patdata, newRow, stringsAsFactors = FALSE, make.row.names = FALSE)
    
    patenv[[ICN]] <- patdata
    
  }
  
  mapply(temp, x$PatientICN, x$EventGeneralCategory, x$EventDateTime, x$EventYear, x$EventMonth, x$EventDay, x$EventWeek, x$EventWeekday, 
         x$TotalNumEvents, x$NumExposureEvents, x$NumHistoryEvents, x$NumManagementEvents, x$NumOTVEvents, x$NumPhysicsEvents, x$NumEncounterEvents,
         x$NumHyperthermiaEvents, x$NumImagingEvents, x$NumTreatmentEvents, x$NumPlanningEvents, x$NumSimEvents, x$NumSideEffectEvents,
         x$NextEvent, x$PrevEvent, x$DaysUntilNextEvent, x$DaysSincePrevEvent, x$HoursUntilNextEvent, x$HoursSincePrevEvent)
  
  return()
  
}









################################################################################

### prepare package dependencies

require("RODBC")
require("logger")

################################################################################
### establish logging and output log file

logfile <- "P:/ORD_Thompson_201805044D/Celia/ALGORITHM/20200421_identifyCourses.log"
log_appender(appender_file(logfile), index=2)

### prepare database connection

con <- odbcDriverConnect(connection="driver={SQL Server};
	                                  server=VHACDWRB03;
	                                  database=ORD_Thompson_201805044D;
	                                  trusted_connection=TRUE")

### query to retrieve prepared data from database

query <- "SELECT * FROM ORD_Thompson_201805044D.Dflt.CJM_20200420_radiation_events"

### create environment

patenv <- new.env(hash = TRUE)

### retrieve data and populate environment

prepQuery(con, query)
fetchQuery(con, n = 100000, FUN = storeInHash)

### create list of individual patients

pats <- ls(envir = patenv)

### process data for each patient
########################## NEED TO AGGREGATE EVENTS WITHIN A FEW HOURS #######################################################################################################

for (pat in pats) {
  
  ### retrieve this patient's data
  
  patdata <- patenv[[pat]]
  
  patdata$EventDateTime <- as.POSIXct(patdata$EventDateTime)
  patdata$NextEvent <- as.POSIXct(patdata$NextEvent)
  patdata$PrevEvent <- as.POSIXct(patdata$PrevEvent)
  
  ### separate multiple courses, if any
  
  checkForMults <- patdata[which(!patdata$EventGeneralCategory %in% c("EXPOSURE", "HX_XRT")),]
  
  ### identify longest gap between events
  
  maxgap <- max(checkForMults$DaysUntilNextEvent, na.rm = TRUE)
  if (maxgap == -Inf) { maxgap <- 0 }
  
  ### if gap exceeds threshold, split events into two courses at the point of the gap
  ### threshold = 6 weeks 
  
  if (maxgap > 42) {
    
    beforeGap <- patdata[which(patdata$EventDateTime <= patdata[which(patdata$DaysUntilNextEvent == maxgap), "EventDateTime"])]
    afterGap <- patdata[which(patdata$EventDateTime >= patdata[which(patdata$DaysSincePrevEvent == maxgap), "EventDateTime"])]
    
  }
  
  ### THINGS THAT NEED HANDLING:
  ### what if the max gap value occurs more than once?
  ### what if there are multiple gaps greater than the threshold?
  
  ### identify all gaps between events
  
  eventGaps <- unique(checkForMults$DaysUntilNextEvent)
  
  ### remove gaps below threshold
  
  eventGaps <- eventGaps[which(eventGaps > 42)]
  
  ############### will need a loop around following to only run if eventGaps is not null
  
  ### retrieve and order the dates associated with the gaps
    
  endDates <- sort(checkForMults[which(checkForMults$DaysUntilNextEvent %in% eventGaps), "EventDateTime"])
    
  ###### separate patient data into disparate courses at each gap

  ### instantiate output dataframe
  
  allCourses <- data.frame(matrix(nrow = 0, ncol = 28, dimnames = list(NULL, colnames(checkForMults))))
  allCourses$OrdinalThisCourse <- NA
  
  ### loop through all course end dates
    
  for (ed in 1:length(endDates)) {
  
    thisCourse <- checkForMults[which(checkforMults$EventDateTime <= endDates[ed]), ]   
    thisCourse$OrdinalThisCourse <- ed
    
    allCourses <- rbind(allCourses, thisCourse, stringsAsFactors = FALSE)
    
    checkForMults <- checkForMults[which(checkforMults$EventDateTime > endDates[ed]), ]
      
  }

  allCourses$TotalNumCourses <- length(endDates)
  
  rm(checkForMults)
  
  

  ### check pattern of treatment/non-treatment days
  
  ### build patient treatment calendar
  
#  patCalendar <- data.frame(matrix(nrow = 0, ncol = 9, dimnames = list(NULL, c("EventYear", "WeekOfYear", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"))))
  
#  patCalendar <- list()
  
  ### make a date column without time
  
  patdata$EventDate <- as.POSIXct(format(patdata$EventDateTime, format = "%Y-%m-%d"))
  
  ### retrieve and order all dates 
  
  eventDates <- sort(unique(patdata$EventDate))
  
  ### retrieve and order all times per date
  
  
  
  
  
  
  ### retrieve from db all diagnoses for patient starting from time of first* event (*depending on category) through last event (what about multiple courses?)
  
  
  
  
  
  
                            
#  patenv[[pat]] <- list(patdata, patCalendar)
  
}














######################################################################################## OLD STUFF THAT MAY YET BE USEFUL



### check duration of treatment
### length of treatment could be the first indication of number of courses

### if total duration is more than six months, split into two courses at point of biggest gap; then start over with each

if (patdata[1,"DurationTx"] > 183) {
  
  ### instantiate flag variable
  
  morePossible <- "yes"
  
  while (morePossible == "yes") {
    
    ### identify biggest gap between treatment events
    
    maxgap <- max(thisPat$DaysUntilNextEvent, na.rm = TRUE)
    if (maxgap == -Inf) { maxgap <- 0 }
    
    ### split course at point of biggest gap
    
    divideCourses <- patdata[which(patdata$DaysUntilNextEvent == maxgap), "EventDateTime"]
    patdata1 <- patdata[which(patdata$EventDateTime <= divideCourses),]
    patdata2 <- patdata[which(patdata$EventDateTime > divideCourses),]
    
    rm(patdata)
    
    ### fix course count columns
    
    patdata1$TotalNumCourses <- 2
    patdata2$TotalNumCourses <- 2
    patdata2$OrdinalThisCourse <- 2
    
    ### fix other columns
    
    patdata1$TotalNumTx <- nrow(patdata1)
    patdata2$TotalNumTx <- nrow(patdata2)
    
    patdata1$LastTxEvent <- divideCourses
    patdata2$FirstTxEvent <- divideCourses
    
    
    
  }
  
  
  
}


#################################################################################
#################################################################################

### Two ways to divide the patients: by number of treatments or by duration of treatment.
### do it in a loop that makes a list of lists of dataframes; that way nothing needs to be predetermined



durations <- sort(unique(cohort$DurationTx))

courses <- list()

for (d in 1:length(durations)) {
  
  thisDur <- cohort[which(cohort$DurationTx == durations[d]),]
  
  numTx <- sort(unique(thisDur$TotalNumTx))
  
  coursesThisDur <- list()
  
  for (n in 1:length(numTx)) {
    
    coursesThisDur[[n]] <- thisDur[which(thisDur$TotalNumTx == numTx[n]),]
    
    thisDur <- thisDur[which(thisDur$TotalNumTx > numTx[n]),]
    
  }
  
  courses[[d]] <- coursesThisDur
  
  rm(coursesThisDur)
  rm(thisDur)
  rm(numTx)
  
  cohort <- cohort[which(cohort$DurationTx > durations[d]),]
  
}


### instantiate output dataframe

patCourses <- as.data.frame(matrix(nrow = 0, ncol = 5))
colnames(patCourses) <- c("PatientICN", "StartDate", "EndDate", "MultipleCourses", "ProblemFlag")
patCourses$PatientICN <- as.numeric(patCourses$PatientICN)
patCourses$StartDate <- as.POSIXct(patCourses$StartDate)
patCourses$EndDate <- as.POSIXct(patCourses$EndDate)
patCourses$MultipleCourses <- as.character(patCourses$MultipleCourses)
patCourses$ProblemFlag <- as.character(patCourses$ProblemFlag)

### step through courses list, identifying and defining treatment courses for each patient

for (c in 1:length(courses)) {
  
  for (d in 1:length(courses[[c]])) {
    
    pats <- unique(courses[[c]][[d]]$PatientICN)
    
    for (pat in pats) {
      ### assess data per patient
      
      thisPat <- courses[[c]][[d]][which(courses[[c]][[d]]$PatientICN == pat),]
      
      ### identify maximum gap between treatment events for this patient
      
      if (thisPat$DurationTx[1] > 1) {
        
        maxgap <- max(thisPat$DaysUntilNextEvent, na.rm = TRUE)
        if (maxgap == -Inf) { maxgap <- 0 }
        
      } else { maxgap <- 0 }
      
      if (thisPat$DurationTx[1] < 183) {
        ### treatment lasted less than six months which indicates a single course
        
        if (maxgap < 4) {
          ### no gaps between treatment events longer than a 3-day weekend
          ### further indicates a single course, as well as no problems
          
          thisCourse <- thisPat[1, c("PatientICN", "FirstTxEvent", "LastTxEvent")]
          thisCourse$MultipleCourses <- "no"
          thisCourse$ProblemFlag <- "no"
          
        } else if (maxgap > 62) {
          ### two month gap in treatments 
          ### indicates multiple courses more than a problem
         
          firstCourse <- thisPat[which(thisPat$DaysUntilNextEvent == maxgap), 
                                 c("PatientICN", "FirstTxEvent", "EventDateTime")]
          secondCourse <- thisPat[which(thisPat$DaysSincePrevEvent == maxgap), 
                                  c("PatientICN", "EventDateTime", "LastTxEvent")]
          thisCourse <- rbind(firstCourse, secondCourse)
          rm(firstCourse)
          rm(secondCourse)
          thisCourse$MultipleCourses <- "yes"
          thisCourse$ProblemFlag <- "no"
          
        } else {
          ### unknown if  complications or multiple courses
          
          thisCourse <- thisPat[1, c("PatientICN", "FirstTxEvent", "LastTxEvent")]
          thisCourse$MultipleCourses <- "unknown"
          thisCourse$ProblemFlag <- "yes"
          
        }
        
        patCourses <- rbind(patCourses, thisCourse)
        rm(thisCourse)
        
      } else {
        ### treatment lasted more than six months which indicates more than one course
        
        firstCourse <- thisPat[which(thisPat$DaysUntilNextEvent == maxgap), 
                               c("PatientICN", "FirstTxEvent", "EventDateTime")]
        secondCourse <- thisPat[which(thisPat$DaysSincePrevEvent == maxgap), 
                                c("PatientICN", "EventDateTime", "LastTxEvent")]
        thisCourse <- rbind(firstCourse, secondCourse)
        rm(firstCourse)
        rm(secondCourse)
        thisCourse$MultipleCourses <- "yes"
        thisCourse$ProblemFlag <- "unknown" 
        
        patCourses <- rbind(patCourses, thisCourse)
        rm(thisCourse)
        
      }
      
      rm(thisPat)
      rm(maxgap)
      
    }
    
  }
  
}


### write courses to file for posterity

rownames(patCourses) <- NULL

write.table(patCourses, file = "P:/ORD_Thompson_201805044D/Celia/ALGORITHM/20200406_courses_1.csv", sep = ",", row.names = FALSE)



### check gold standard

goldstandard <- sqlQuery(con, "SELECT
PatientICN,
JLV_TotalNumCourses,
JLV_OrdinalThisCourse,
JLV_FirstEventThisCourse,
JLV_LastEventThisCourse
FROM ORD_Thompson_201805044D.Dflt.GOLD_STANDARD", as.is = TRUE)



goldICNs <- unique(goldstandard$PatientICN)


checkpats <- patCourses[which(patCourses$PatientICN %in% goldICNs),]

### don't know why, but 28 unique patients in gold standard table, and only 12 had data in patCourses

verifycourses <- merge(goldstandard, checkpats, by = "PatientICN")

verifycourses$FirstTxEvent <- as.Date(verifycourses$FirstTxEvent, format = "%Y-%m-%d")
verifycourses$LastTxEvent <- as.Date(verifycourses$LastTxEvent, format = "%Y-%m-%d")

verifycourses$multiples <- (verifycourses$JLV_TotalNumCourses > 1 && verifycourses$MultipleCourses != "no") || (verifycourses$JLV_TotalNumCourses == 1 && verifycourses$MultipleCourses != "yes")

verifycourses$StartDate <- (verifycourses$JLV_FirstEventThisCourse == verifycourses$FirstTxEvent)

verifycourses$EndDate <- (verifycourses$JLV_LastEventThisCourse == verifycourses$LastTxEvent)

write.table(verifycourses, file = "P:/ORD_Thompson_201805044D/Celia/ALGORITHM/20200406_verified_courses_1.csv", sep = ",", row.names = FALSE)

