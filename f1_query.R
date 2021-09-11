library(duckdb)
library(DBI)
library(purrr)
library(dplyr)
library(lubridate)

path_db <- file.path("..", "F1db.duckdb")

#Connect to F1 DB
con <- dbConnect(duckdb(), path_db)

# Convert each dbTable to df
nested_df <-
  dbListTables(con) %>% map(~ tbl(con, .x)) %>% set_names(dbListTables(con))

for (i in 1:length(nested_df)) {
  dfname = names(nested_df)[i]
  assign(dfname, nested_df[[i]] %>% collect())
}


# Longest Racing Lap ------------------------------------------------------


longest_racing_lap <- lap_times %>%
  left_join(races, "raceId") %>%
  left_join(drivers, "driverId") %>%
  select(forename, surname, nationality, name, date, longest_lap = time.x) %>%
  arrange(desc(longest_lap)) %>% 
  slice_head(n = 10)


# Shortest Racing Lap -----------------------------------------------------


shortest_racing_lap <- results %>%
  left_join(races, "raceId") %>%
  left_join(drivers, "driverId") %>%
  filter(!is.na(fastestLapTime)) %>%
  select(forename, surname, nationality, name, date, shortest_lap = fastestLapTime) %>%
  arrange(shortest_lap) %>%
  slice_min(shortest_lap, n = 10)


# Fastest Lap at Each Circuit ---------------------------------------------

circuit_fastest_lap <- results %>%
  left_join(races, "raceId") %>%
  left_join(drivers, "driverId") %>%
  left_join(circuits, "circuitId") %>%
  filter(!is.na(fastestLapTime)) %>%
  select(
    forename,
    surname,
    nationality,
    Fastest_Lap = fastestLapTime,
    Circuit = name.y,
    Race = name.x,
    year
  ) %>%
  group_by(Circuit) %>%
  slice_min(Fastest_Lap, n = 1)


# Greatest Number of Race Entries -----------------------------------------

total_entries <- results %>%
  left_join(drivers, "driverId") %>%
  group_by(driverId) %>%
  summarise(total_entries = n())

greatest_num_race_entries <- total_entries %>%
  left_join(drivers, "driverId") %>%
  select(forename, surname, nationality, total_entries) %>%
  slice_max(total_entries, n = 10)



# Youngest Driver to start a race -----------------------------------------

youngest_driver_to_start <- results %>%
  left_join(races, "raceId") %>%
  left_join(drivers, "driverId") %>%
  group_by(driverId) %>%
  slice(which.min(date - dob)) %>%
  mutate(age_at_first_race = (date - dob)) %>%
  select(forename, surname, nationality, age_at_first_race, Race = name) %>%
  arrange(age_at_first_race) %>%
  head(10)




# Older Driver to start a race --------------------------------------------

oldest_driver_to_start <- results %>%
  left_join(races, "raceId") %>%
  left_join(drivers, "driverId") %>%
  group_by(driverId) %>%
  slice(which.max(date - dob)) %>%
  mutate(age_at_first_race = (date - dob)) %>%
  select(forename, surname, nationality, age_at_first_race, Race = name) %>%
  arrange(desc(age_at_first_race)) %>%
  head(10)


# Most Wins All-Time ------------------------------------------------------

most_wins_all_time <- results %>%
  filter(position == 1) %>%
  group_by(driverId) %>%
  summarise(race_wins = sum(position)) %>%
  left_join(drivers, "driverId") %>%
  select(forename, surname, nationality, race_wins) %>%
  slice_max(race_wins, n = 10)

race_wins_entries <- results %>%
  group_by(driverId) %>%
  summarise(race_wins = sum(position == 1, na.rm = TRUE),
            entries = n_distinct(raceId)) %>%
  left_join(drivers, "driverId") %>%
  select(forename, surname, nationality, race_wins, entries) %>%
  slice_max(race_wins, n = 10)

win_percentage <- results %>%
  group_by(driverId) %>%
  summarise(race_wins = sum(position == 1, na.rm = TRUE),
            entries = n_distinct(raceId), win_percentage=race_wins/entries) %>%
  left_join(drivers, "driverId") %>%
  select(forename, surname, nationality, race_wins, entries, win_percentage) %>%
  slice_max(race_wins, n = 10)


# Most Wins At Same GP ----------------------------------------------------

results %>% 
  filter(position==1) %>% 
  left_join(races,"raceId") %>%  # colnames()
  left_join(drivers,"driverId") %>% 
  group_by(driverId,name) %>% 
  count(name="gp_wins") %>% 
  left_join(drivers,"driverId") %>% 
  ungroup() %>% 
  select(forename, surname, nationality, name, gp_wins) %>% 
  slice_max(gp_wins, n=10)



# Most Wins in Single Season ----------------------------------------------

driver_standings %>% 
  left_join(races,"raceId") %>% 
  group_by(driverId, year) %>% 
  summarise(wins=max(wins)) %>% 
  left_join(drivers,"driverId") %>% 
  select(forename, surname, nationality, year, wins) %>% 
  ungroup() %>% slice_max(wins,n=10)



# Most Podiums All-Time -------------------------------------------------



# Most Winning Constructor ------------------------------------------------



# Highest Percentage of Constructor Wins All-Time -------------------------



# Highest Percentage of Constructor Wins In a Season ----------------------


