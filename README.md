# PBP_Players_On_Court

## Data Structure
I'm assuming that the destination warehouse will have raw data tables for all three files: pbp_players, pbp, and rosters. With that in mind, and to make aggregate functions as easy possible, I am inserting an entry into the table for each event_id-play_id-player_id combination.

For this initial iteration, I included the bare minimum data needed, but it would be trivial to add any other data from the pbp_players file such as team. However, this data can be just as easily accessed via simple joins within the data warehouse.

I have also created an additional table - Players_On_Court_Hash - that is used to store a hash of the lineup state for every event_id-play_id combination.


## PbpPlayersByEventHandler
The intention of the PbpPlayersByEventHandler class is to take in a complete dictionary of pbp_players data for a single event id and create a list of records to insert into the PBP_Players_On_Court table. 
The class assumes that the input values are pre-sorted in order by of play_id, then sequence_id (sorting is handled in the pre-processing).

Upon instantiation, the Handler sets the initial starting lineup with the set_starting_lineup() method. 
It also creates two separate MySQL clients - one to be used for the bulk insert of records into the PBP_Players_On_Court ('sql_client'). The other is used for making smaller one-off calls as part of the hashing process ('hashing_sql_client').

To lower complexity with the multiple connection strings to the database, the PBP_Players_On_Court values are inserted in a single job at the very end. 
The PbpPlayersByEventHandler.insert_data list variable is where the records are stored during processing.

Next, the handler begins iterating through each row of the sorted list of play_ids & sequences.
  
The first step of each iteration creates a hash of the current lineup state and then queries the Players_On_Court_Hash table for the previously stored lineup hash for that play_id. 

There is a run_hash_comparisons() method that compares those two values:

        1. If there's no hash record in the table - it's a new entry and proceeds as normal
        2. If the two hatches match - the value already exists, skip record.
        3. If there is a hash in the db, but it doesn't match the new value - update the hash value in the source table, and then delete all entries for that event_id-play_id record in the PBP_Players_On_Court. (replacement values will be created in the following step) 
  
Within the main processing loop there is an if conditional with 4 different situations. 
        1. Creating PBP_Players_On_Court records for the starting lineup.
        2. Updating the lineup to include an incoming player substitution (play_event_id == 10 & sequence == 1).
        3. Update linup to remove outgoing player substition (play_event_id == 10 & sequence == 2). Then add records to insert_data list variable.
        4. Handle other plays with current PbpPlayersByEventHandler.lineup.

To limit redundant database calls, there is a last_play_id iterator to ensure that only one run_hash_comparisons() call is made per play. Some plays have multiple sequences which each have their own record.


## Pre & Post Processing
Initial loading of the Excel files is handled via Pandas, as well as the initial sorting and filtering. After that, I intentionally dump the dataset back into a regular dictionary prior to the main lineup sequencing because looping within Pandas is very much an anti-pattern.

# Known Areas For Improvement

##Updates
The hashing component feels like overkill, but it's the solution that seemed most in line with the requirement 'When the script is run a second time, it updates the new table to catch any new changes.'
It's a curated dataset compiled by professionals; as such, the greatest value comes from the data being complete and accurate. If changes do happen to the data, it's generally because a human made a mistake originally and has since corrected it. 

I would want to confirm that idea with my data scientists, but if they agreed with that assessment, I would push for a fully batched job that would treat each event run as a new set of data. To preserve history, data from previous runs would be marked 'Inactive'.

I believe that change would drastically reduce the complexity while significantly improving the maintainability and performance of the job.

## Testing
At the moment, due to the blocker with substitions, I have not added any tests. The lack of tests makes me very antsy and self-conscious. But at the same time, I can't bring myself to start adding some when there's a blocker that may require to radical re-writes to core functionality.

If I were to start now, it would be getting unit tests around my class methods.

## Data Classes
If this were to be productionalized and running against an api, I would, at a minimum, create data classes for all of our outside data dependancies. It feels like there are some established data standards out there for the play-by-play data sources, and pulling them in would be a low effort means to reduce potential ambiguity.

## Database Connections
I am not very not pleased with having multiple MySQL clients and connection strings that stay open for varying amounts of time. But again, this goes back to the design decision of updates to catch changes vs. bulk imports. If we are okay using a bulk import process, those would go away immediately. If not, I'm sure that there are significant improvements that can be made.
