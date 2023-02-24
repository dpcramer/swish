# PBP_Players_On_Court
To run the program, you must have the pbp_players.xlsx file in your home directory, along with a .env using the .env.example as a guide:

Make run

You may pass an event_id as an argument to process only a single game:

$ python main.py 1947160

Tests can be run via:

Make test

git history can be found at: https://github.com/dpcramer/swish

## Data Structure
I'm assuming that the destination warehouse will have raw data tables for all three files: pbp_players, pbp, and rosters. To make aggregate functions as easy possible, I am inserting an entry into the table for each event_id-play_id-player_id combination.

I have also created an additional table - Players_On_Court_Hash - that is used to store a hash of the lineup state for every event_id-play_id-team_id combination.


## PbpPlayersByEventHandler
The intention of the PbpPlayersByEventHandler class is to take in a complete dictionary of pbp_players data for a single event id and create a list of records to insert into the PBP_Players_On_Court table. 
The class assumes that the input values are pre-sorted in order by of play_id asc, then sequence_id desc (sorting is handled in the pre-processing).

Upon instantiation, the Handler sets the initial starting lineups for each team with the set_starting_lineup() method. 

To lower database overhead the PBP_Players_On_Court values are inserted in a single job at the very end. 
The PbpPlayersByEventHandler.players_on_court_insert_data list variable is where the records are stored during processing.

After receiving the sorted list of plays, the handler begins iterating through each row.
  
The first step of each iteration creates a hash of the current lineup state for each team and then queries the Players_On_Court_Hash table for the previously stored lineup hash for that play_id-team_id. 

There is a run_hash_comparisons_for_team_and_play() method that compares those two values:

        1. If there's no hash record in the table - it's a new entry and proceeds as normal
        2. If the two hashes match - the value already exists, skip record.
        3. If there is a hash in the db, but it doesn't match the new value - update the hash value in the source table, and then delete all entries for that event_id-play_id-team_id record in the PBP_Players_On_Court table and the players_on_court_insert_data list (replacement values will be created in the following step) 
  
Within the main processing loop there is an if conditional with 5 different situations:

        1. Creating PBP_Players_On_Court records for the starting lineup.
        2. Handle records with no team_id. Generally start/end of quarter, etc.
        3. Updating the team lineup to remove an outgoing player substitution (play_event_id == 10 & sequence == 2).
        4. Update team lineup to add an incoming player substitution (play_event_id == 10 & sequence == 1). Then add records to the players_on_court_insert_data list variable.
        5. Handle other plays by first adding the player to the lineup if they don't exist.

Step 5 was created to handle the known data errors around substitution. Unfortunately, while we can find players to add to the lineups this way, it is not possible with the current data sources to find when a player should be removed. To provide the best approximation possible with the data at hand, I have added an 'is_anomaly' field on these records to identify which records have been affected. In these anomalous situations, the count of players != 5.

## Pre & Post Processing
Initial loading of the Excel files is handled via Pandas, as well as the initial sorting and filtering. After that, I intentionally dump the dataset back into a regular dictionary prior to the main lineup sequencing because looping within Pandas is very much an anti-pattern.

# Known Areas For Improvement

## Data Issues
Given that the data quality concerns are a known issue and part of the problem, I think using non-substitution records to find missing players and flagging any anomalous data is reasonable. 

If facing this problem in a real world situation my first response would be to try to fix the data source. Failing that, the next step would be to look for complementary data that could fill in the gaps.

##Updates vs. Destroy & Rebuild
The hashing component feels like overkill, but it's the solution that seemed most in line with the requirement 'When the script is run a second time, it updates the new table to catch any new changes.'
It's a curated dataset compiled by professionals; as such, the greatest value comes from the data being complete and accurate. If changes do happen to the data, it's generally because a human made a mistake originally and has since corrected it. 

I would want to confirm that idea with my data scientists, but if they agreed with that assessment, I would push for a fully batched job that would treat each event run as a new set of data. To preserve history, data from previous runs would be marked 'Inactive' and a fresh new batch of records would be created.

I believe that change would drastically reduce the code complexity while significantly improving the maintainability and performance of the job. That being said, with the hash checks on a local db server it only takes about 5 seconds to run, so performance isn't a huge concern (code complexity is, though).

## Data Classes
If this were to be productionalized and running against an api, I would, at a minimum, create data classes for all of our outside data dependancies. It feels like there are some established data standards out there for the play-by-play data sources, and pulling them in would be a low effort means to reduce ambiguity.

## Testing
Similarly, once Data Sources and Classes are set it will make it much easier to create Factories for our different inputs to fill out the testing suite and make it more robust.

With more time I would like to set up mocks for all the database calls, too.

## Database Connections
I am not very not pleased with having multiple MySQL clients and connection strings that stay open for varying amounts of time. But again, this goes back to the design decision of updates to catch changes vs. bulk imports. If we are okay using a bulk import process, those would go away immediately. If not, I'm sure that there are still meaningful improvements that can be made.

## Exception Handling & logging
Speaking of databases, once we get to the point of no longer running on a local server, try/catch handling will be essential.
