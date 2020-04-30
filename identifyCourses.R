### CDSRG
### IDENTIFY RADIATION COURSES ALGORITHM 

### This function identifies and defines courses of radiation treatment using structured data from the VA VINCI CDW.
### It requires that a user-prepared table of radiation patients be created in a VINCI database (parameter = "cohort").
### It requires the provided file of VA VINCI CDW tables and columns (parameter = "targetFile").

### The file ALL_ICD_TABLES_COLUMNS_DATES.csv lists VINCI CDW tables known to hold ICD data.
### It is not guaranteed to be exhaustive.
### Target tables were identified in SQL Server with the following query:
###      SELECT TABLE_NAME, COLUMN_NAME 
###      FROM [database].INFORMATION_SCHEMA.COLUMNS
###      WHERE COLUMN_NAME LIKE '%ICD%'
### Manual inspection of each table is required to ascertain both its usefulness and relevant columns!
### Some target tables must be joined with their parent table in order to retrieve appropriate data.

identifyCourses <- function(serverName, dbName, cohort, targetFile) {
  
  
  ### INTERNAL FUNCTIONS
  ################################################################################
  ### makeCon()
  ### function for establishing an RODBC connection to a database
  
  makeCon <- function(serverName, dbName) {
    
    ### install RODBC package
    
    suppressPackageStartupMessages(require("RODBC"))
    
    ### build connection name and establish connection
    
    conString <- paste("driver={SQL Server};server=", serverName, ";database=", dbName, ";trusted_connection=TRUE", sep = "")
    con <- odbcDriverConnect(connection = conString)
    
    ### error handling
    ###### invalid server name
    ###### invalid db name or db user doesn't have permission for
    
    ### return connection
    
    return(con)
    
  }
  
  ################################################################################
  ### prepQuery()
  ### function for properly setting up to retrieve data from a database using RODBC
  
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
  ### function to retrieve data from a database in chunks and apply another function to the data
  
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
  ### storeInHash()
  ### function for populating R hash environment with patient radiation treatment data
  
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
  ### getICD()
  ### function to retrieve ICD information from the VINCI CDW

  getICD <- function(con, dbName, pat, beginDate, endDate, targetFile) {
    
    ### establish relevant tables to query in the specified database 
    
    targets <- prepTargets(targetFile, dbName, con)
    
    ### instantiate empty vectors to hold target table queries
    
    query_9 <- c()
    query_10 <- c()
    
    ### instantiate constants for target queries
    
    subquery <- paste("(SELECT PatientSID FROM ", dbName, ".Src.CohortCrosswalk WHERE PatientICN = '", pat, "')", sep = "")
    
    WHERE <- "WHERE "
    AND <- "AND "
    WHERE_dates <- paste("BETWEEN '", beginDate, "' AND '", endDate, "'", sep = "")
    
    ### build target table queries
    
    for (tar in 1:nrow(targets)) {
      
      SELECT_9 <- paste("SELECT DISTINCT ICD9SID AS SID, ", sep = "")
      SELECT_10 <- paste("SELECT DISTINCT ICD10SID AS SID, ", sep = "")
      
      FROM <- paste("FROM ", dbName, ".Src.", targets$TABLE.NAME[tar], sep = "")
      
      if (targets$REQUIRES.JOIN[tar] == 'Y') {
        
        JOIN <- paste(" a INNER JOIN ", dbName, ".Src.", targets$JOIN.TABLE[tar], " b", sep = "")
        JOIN <- paste(JOIN, " ON a.", targets$JOIN.FIELD[tar], " = b.", targets$JOIN.TABLE.JOIN.FIELD[tar], sep = "")
        
        FROM <- paste(FROM, JOIN, sep = " ")
        
        SELECT_9 <- paste(SELECT_9, "b.", sep = "")
        SELECT_10 <- paste(SELECT_10, "b.", sep = "")
        
        WHERE <- paste(WHERE, "a.", sep = "")
        AND <- paste(AND, "b.", sep = "")
        
      }
      
      SELECT_9 <- paste(SELECT_9, targets$DATE.FIELD[tar], "AS DateDx", sep = "")
      SELECT_10 <- paste(SELECT_10, targets$DATE.FIELD[tar], "AS DateDx", sep = "")
      
      WHERE <- paste(WHERE, "PatientSID IN ", subquery, sep = "")
      AND <- paste(AND, targets$DATE.FIELD[tar], sep = "")
      WHERE <- paste(WHERE, AND, WHERE_dates, sep = " ")
      
      query_9[tar] <- paste(SELECT_9, FROM, WHERE, sep = " ")
      query_10[tar] <- paste(SELECT_10, FROM, WHERE, sep = " ")
      
      rm(SELECT_9)
      rm(SELECT_10)
      rm(FROM)
      rm(JOIN)
      rm(WHERE)
      rm(AND)
      
    }
    
    ### divide queries against target tables into batches of 5 and use loop to process in batches
    
    numTargets <- length(query_9)
    numBatches <- trunc(numTargets/5)
    if (numBatches < numTargets/5) { numBatches <- numBatches + 1 }
    lowerLimit <- 1
    upperLimit <- 5
    
    ### instantiate empty vectors to hold batched queries 
    
    batches_9 <- c()
    batches_10 <- c()
    
    ### instantiate constants for batched queries
    
    batchSELECT_9 <- "SELECT DISTINCT ICD9Code, DateDx FROM ("
    batchSELECT_10 <- "SELECT DISTINCT ICD10Code, DateDx FROM (" 
    
    batchJOIN_9 <- ") a INNER JOIN CDWWork.Dim.ICD9 b ON a.SID = b.ICD9SID"
    batchJOIN_10 <- ") a INNER JOIN CDWWork.Dim.ICD10 b ON a.SID = b.ICD10SID"  
    
    ### build batched queries
    
    for (batch in 1:numBatches) {
      
      if (upperLimit > numTargets) { upperlimit <- numTargets }
      
      this_9 <- paste(query_9[lowerLimit:upperLimit], collapse = " UNION ")
      this_10 <- paste(query_10[lowerLimit:upperLimit], collapse = " UNION ")
      
      batches_9[batch] <- paste(batchSELECT_9, this_9, batchJOIN_9, sep = "")
      batches_10[batch] <- paste(batchSELECT_10, this_10, batchJOIN_10, sep = "")
      
      lowerLimit <- lowerLimit + 5
      upperLimit <- upperLimit + 5
      
    }
    
    ### tidy
    
    rm(query_9)
    rm(query_10)
    
    ### instantiate empty data frame to hold patient diagnostic data
    
    patDx <- data.frame(matrix(nrow = 0, ncol = 2, dimnames = list(NULL, c("ICDCode", "DateDx"))))
    
    for (batch in 1:numBatches) {
      
      data_9 <- sqlQuery(con, batches_9[batch], as.is = TRUE)
      colnames(data_9) <- c("ICDCode", "DateDx")
      patDx <- rbind(patDx, data_9, stringsAsFactors = FALSE)
      
      data_10 <- sqlQuery(con, batches_10[batch], as.is = TRUE)
      colnames(data_10) <- c("ICDCode", "DateDx")
      patDx <- rbind(patDx, data_10, stringsAsFactors = FALSE)
      
      rm(data_9)
      rm(data_10)
      
    }
    
    ### tidy
    
    rm(batches_9)
    rm(batches_10)  
    
    return(patDx)
    
  }
  
  ################################################################################
  ### prepTargets()
  ### function to identify existing tables in a particular VINCI CDW database
  
  prepTargets <- function(targetFile = NULL, dbName = NULL, con = NULL) {
    
    ### error handling
    ###### invalid con or no con
    ###### invalid file name or no file name
    ###### invalid db name or db user doesn't have permission for
    
    ### load file
    
    targets <- read.csv(targetFile, header = TRUE, stringsAsFactors = FALSE)
    
    ### change all instances of 'X' to null values (X was used as a placeholder in the file)
    
    for (x in 1:nrow(targets)) {
      targets[x, which(targets[x,] == 'X')] <- NA
    }
    
    ### determine which tables exist in the given database 
    
    for (target in 1:nrow(targets)) {
      query <- paste("SELECT NULLIF(COUNT(*), 0) AS TableExists
                 FROM ", dbName, ".INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_SCHEMA = 'Src'
                 AND TABLE_NAME = '", targets[target,1], "'", sep = "")
      targets$TABLE.EXISTS[target] <- sqlQuery(con, query)
    }
    
    ### subset targets dataframe to contain only rows where TABLE.EXISTS == 1
    
    targets <- targets[which(targets$TABLE.EXISTS == 1),]
    rownames(targets) <- NULL
    
    return(targets)
    
  }
  
  ################################################################################
  ### makeLog()
  ### function to install logger package and set up logging file
  
  makeLog <- function(dbName) {
    
    ### install logger package
    
    suppressPackageStartupMessages(require("logger"))
    
    ### acquire current date and format for using in file name
    
    today <- Sys.Date()
    today <- format(today, format = "%Y%m%d")
    
    ### instantiate file
    
    logfile <- paste("P:/", dbName, "/", today, "_identifyCourses.log", sep = "")
    log_appender(appender_file(logfile), index=2)
    
    return(TRUE)
    
  }
  
  ################################################################################
  
  ### increase working memory
  
  memory.limit(size = 100000)
  
  ### instantiate logging file
  
  makeLog(dbName)
  
  ### instantiate database connection
  
  con <- makeCon(serverName, dbName)
  
  ### query to retrieve prepared data from database
  
  query <- paste("SELECT * FROM ", cohort, sep = "")
  
  ### create environment
  
  patenv <- new.env(hash = TRUE)
  
  ### retrieve data and populate environment
  
  prepQuery(con, query)
  fetchQuery(con, n = 100000, FUN = storeInHash)
  
  ### create list of individual patients
  
  pats <- ls(envir = patenv)
  
  ### process data for each patient
  
  for (pat in pats) {
    
    ### retrieve this patient's data
    
    patdata <- patenv[[pat]]
    
    patdata$EventDateTime <- as.POSIXct(patdata$EventDateTime)
    patdata$NextEvent <- as.POSIXct(patdata$NextEvent)
    patdata$PrevEvent <- as.POSIXct(patdata$PrevEvent)
    
    ### separate obvious multiple courses, if any (obvious currently defined as 6 weeks)
    
    ### do not include exposure and history events in course definition
    
    checkForMults <- patdata[which(!patdata$EventGeneralCategory %in% c("EXPOSURE", "HX_XRT")),]
    
    ### reduce patdata data frame to only exposure and history events, if any (those events not in checkForMults)
    
    patdata <- patdata[which(patdata$EventGeneralCategory %in% c("EXPOSURE", "HX_XRT")),]
    
    ### identify all gaps between events
    
    eventGaps <- unique(checkForMults$DaysUntilNextEvent)
    
    ### remove gaps below threshold
    
    eventGaps <- eventGaps[which(eventGaps > 42)]
 
    if (nrow(eventGaps) > 0) {    
      
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
      
      ### tidy
      
      rm(checkForMults)
      rm(eventGaps)
    
    } else {
    
      checkForMults$OrdinalThisCourse <- 1
      checkForMults$TotalNumCourses <- 1
      
      allCourses <- checkForMults
      
      ### tidy
      
      rm(checkForMults)
      rm(eventGaps)
      
    }
    
    ### add new course columns to history and exposure events, if any, and rejoin dataframes
    
    if (nrow(patdata) > 0) {
      
      patdata$OrdinalThisCourse <- NULL
      patdata$TotalNumCourses <- allCourses[1, "TotalNumCourses"]
      
      allCourses <- rbind(allCourses, patdata, stringsAsFactors = FALSE)
      
    }
    
    patdata <- allCourses
    
    ### tidy
    
    rm(allCourses)
    
    ### establish date range -- for each course! -- in which to gather patient's diagnostic data
    ### date range: from six weeks before first radiation event until two weeks after last radiation event
    
    ### instantiate empty data frame to hold course start and end dates
    
    patCourses <- data.frame(matrix(nrow = 0, ncol = 5, dimnames = list(NULL, c("PatientICN", "OrdinalThisCourse", "TotalNumCourses", "CourseBegin", "CourseEnd"))))
    
    ### instantiate empty list to hold data frames of diagnostic data retrieved from the database
    
    patdx <- list()
    
    for (course in 1:patdata[1, "TotalNumCourses"]) {
      
      thisCourse <- patdata[which(patdata$OrdinalThisCourse == course), ]
      
      theseDates <- unique(thisCourse[, "EventDateTime"])
      
      beginDate <- min(theseDates)
      endDate <- max(theseDates)
      
      rm(theseDates)
      
      thisCourse <- thisCourse[1, c("PatientICN", "OrdinalThisCourse", "TotalNumCourses")]
      
      thisCourse$beginDate <- beginDate
      thisCourse$endDate <- endDate
      
      patCourses <- rbind(patCourses, thisCourse, stringsAsFactors = FALSE)
    
      ### retrieve diagnostic data for patient for this course

      patdx[[course]] <- getICD(pat, beginDate, endDate)      
      
    }

    
    ### try to determine cancer diagnosis
    ### cancer ICD 9: 140. - 240.
    ### cancer ICD 10: C00. - D50. (C76 - C80 could be mets) 
    
    
    
    ### check diagnostic data during any gaps in treatment
    
    
    
    
    
    ### write patient information to output file
    
    
    
  }

### after processing all patient data, do validation against gold standard

### retrieve gold standard data from database
### read in courses output file

}



### cohort <- "ORD_Thompson_201805044D.Dflt.CJM_20200420_radiation_events"
### targetFile <- "P:/ORD_Conlin_201708011D/Celia/Comorb/ALL_ICD_TABLES_COLUMNS_DATES.csv"










