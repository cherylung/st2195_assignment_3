#Clear environment
rm(list=ls())

#install.packages("RSQLite")
#install.packages("dplyr")
#install.packages("dbplyr")
library(DBI)
library(dplyr)


setwd("C:/Users/chery/ST2195/st2195_assignment_3/")

#create database
if (file.exists("airline2.db"))
  file.remove("airline2.db")
connect <- dbConnect(RSQLite::SQLite(),"airline2.db") #connect to database


#combine multiple .csv files into one vector
for (i in c(2000:2005)){
  ontime <- read.csv(paste0(i,".csv"),header = TRUE) #paste0() -> combine 2000-2005 with .csv as one string
  if(i == 2000) {
    dbWriteTable(connect, "ontime",ontime)
  }
    else {
      dbWriteTable(connect, "ontime", ontime, append = TRUE)
    }
}


airports <- read.csv("airports.csv", header = TRUE)
carriers <- read.csv("carriers.csv", header = TRUE)
planes <- read.csv("plane-data.csv", header = TRUE)

dbWriteTable(connect,"airports",airports) #writing to db 
dbWriteTable(connect,"carriers",carriers)
dbWriteTable(connect,"planes", planes)

dbListFields(connect,"ontime")
dbListFields(connect,"airports")
dbListFields(connect,"planes")
dbListFields(connect,"carriers")



#a. airplane with the lowest associated average departure delay
q1 <- dbGetQuery(connect,
                 "SELECT model AS model, AVG(ontime.DepDelay) as avgDelay
                 FROM planes JOIN ontime USING(tailnum)
                 WHERE ontime.Cancelled = 0 AND Ontime.Diverted = 0 AND ontime.DepDelay > 0
                 GROUP BY model
                 ORDER BY avgDelay"
                 )
print(paste(q1[1,"model"], "has the lowest associated average departure delay"))

#b. cities has the highest number of inbound flights(excluding cancelled flights)
q2 <- dbGetQuery(connect,
                 "SELECT airports.city AS cities, COUNT(*) AS total
                 FROM airports JOIN ontime ON ontime.Dest = airports.iata
                 WHERE ontime.Cancelled = 0
                 GROUP BY cities
                 ORDER BY total DESC"
)

print(paste(q2[1,"cities"], "has the highest number of inbound flights excluding cancelled flights."))

#c. companies that have the highest number of cancelled flights
q3 <- dbGetQuery(connect,
                 "SELECT carriers.Description AS company, COUNT(*) AS total
                 FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                 WHERE ontime.Cancelled = 1 AND company IN ('Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.')
                 GROUP BY company
                 ORDER BY total DESC"
)

print(paste(q3[1,"company"], "has the highest number of cancelled flights."))

#d. companies that have the highest number of cancelled flights with respect to their number of total flights
q4 <- dbGetQuery(connect,
                 "SELECT carriers.Description AS company, COUNT(*) AS total
                 FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                 WHERE ontime.Cancelled = 1 AND company IN ('Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.')
                 GROUP BY company
                 ORDER BY total DESC"
)

print(paste(q4[1,"company"], "has the highest number of canceled flights with respect to their number of total flights"))

## ============= using dplyr =============

# put into table #
planes_db <- tbl(connect, "planes")
ontime_db <-tbl(connect, "ontime")
carriers_db <- tbl(connect, "carriers")
airports_db <- tbl(connect, "airports")

#a. 
q1 <- ontime_db %>%
  rename_all(tolower) %>% #lowercase all values
  inner_join(planes_db, by = "tailnum", suffix = c(".ontime",".planes")) %>%
  filter(Cancelled == 0 & Diverted == 0 & DepDelay > 0) %>%
  group_by(model) %>%
  summarize(avgDelay = mean(DepDelay, na.rm = TRUE)) %>% #na.rm -> remove null values
  arrange(avgDelay)

print(head(q1,1))

#b.
q2 <- ontime_db %>%
  inner_join(airports_db, by = c("Dest" = "iata")) %>%
  filter(Cancelled == 0) %>%
  group_by(city) %>%
  summarize(total=n()) %>%
  arrange(desc(total))

print(head(q2,1))

#c.
q3 <- ontime_db %>%
    inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Cancelled == 1 & Description %in% c('Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.')) %>%
  group_by(Description) %>%
  summarize(total = n()) %>%
  arrange(desc(total))

print(head(q3,1))

#d.
q4a <- ontime_db %>%
  inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Cancelled == 1 & Description %in% 'Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.') %>%
  group_by(Description) %>%
  summarize(numerator = n()) %>%
  rename(carrier = Description)

q4b <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Description %in% 'Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.') %>%
  group_by(Description) %>%
  summarize(denominator = n()) %>%
  rename(carrier = Description)

q4 <- inner_join(q4a, q4b, by = "carrier") %>%
  mutate_if(is.integer, as.double) %>%
  mutate(ratio = numerator/denominator) %>%
  select(carrier, ratio) %>%
  arrange(desc(ratio))

print(head(q4))
  
q4_simplified <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Description %in% 'Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.') %>%
  rename(carrier = Description) %>%
  group_by(carrier) %>%
  summarise(ratio = mean(Cancelled, na.rm = TRUE)) %>%
  arrange(desc(ratio))

print(head(q4_simplified,1))