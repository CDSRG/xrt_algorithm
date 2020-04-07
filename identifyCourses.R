### CDSRG
### IDENTIFY RADIATION COURSES ALGORITHM 

# retrieve data

### prepare package dependencies

if (!requireNamespace("RODBC", partial = TRUE, quietly = TRUE)) {
  warning("installing missing package 'RODBC'")
  install.packages("RODBC", quiet = TRUE)
}
if (!isNamespaceLoaded("RODBC")) {
  suppressPackageStartupMessages(require("RODBC"))
}

### prepare database connection

con <- odbcDriverConnect(connection="driver={SQL Server};
	                                  server=VHACDWRB03;
	                                  database=ORD_Thompson_201805044D;
	                                  trusted_connection=TRUE")

### retrieve prepared data from database

cohort <- sqlQuery(con, "SELECT * FROM ORD_Thompson_201805044D.Dflt.CJM_20200323_radiation_TX", as.is = TRUE)
cohort$DOB <- as.POSIXct(cohort$DOB)
cohort$DOD <- as.POSIXct(cohort$DOD)
cohort$LastFollowUp <- as.POSIXct(cohort$LastFollowUp)
cohort$EventDateTime <- as.POSIXct(cohort$EventDateTime)
cohort$FirstTxEvent <- as.POSIXct(cohort$FirstTxEvent)
cohort$LastTxEvent <- as.POSIXct(cohort$LastTxEvent)



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

