# PBP_Players_On_Court
The core features of this program are built around the  class in orchestration with data exclusively from the pbp_players file.


## Data Structure
I'm assuming that the destination warehouse will have raw data tables for all three files: pbp_players, pbp, and rosters. With that in mind, and to make aggregate functions as easy possible, I am inserting an entry into the table for each event_id-play_id-player_id combination.

For this initial iteration, I included the bare minimum data needed, but it would be trivial to add any other data from the pbp_players file such as team. Though, this data can be just as easily accessed with simple joins within the data warehouse.

I have also created an additional table - Players_On_Court_Hash - that is used to store a hash of the lineup state for every event_id-play_id combination.


## PbpPlayersByEventHandler
The intention of the PbpPlayersByEventHandler class is to take in a complete dictionary of pbp_players data for a single event id. 
The class assumes that the values are sorted in order by of play_id, then sequence_id (sorting is handled in the pre-processing).

Upon instantiation, Handler sets the initial starting lineup with the set_starting_lineup() method. 
It also creates two separate MySQL clients - one to be used for the bulk insert of records into the PBP_Players_On_Court ('sql_client'). The other is used for making smaller one-off calls as part of the hashing process ('hashing_sql_client').

To lower complexity with the multiple connection strings to the database, the PBP_Players_On_Court values are inserted in a single job at the very end. 
The PbpPlayersByEventHandler.insert_data list variable is where the records are stored during processing.

Next, the handler begins iterating through each row of the sorted list of play_ids & sequences.
  
The first step of each iteration creates a hash for the current lineup state from the class variable and then queries the Players_On_Court_Hash table for the lineup hash for that play_id. 

There is a run_hash_comparisons() method that compares those two values:

        1. If there's no hash record in the table - it's a new entry and proceeds as normal
        2. If the two hatches match - the value already exists, skip record.
        3. If there is a hash in the db, but it doesn't match the new value - update the hash record, and then delete all entries for that event_id-play_id record in the PBP_Players_On_Court so updated records can be written. 
  
Within the main processing loop there is an if conditional with 4 different situations. 
        1. Handling the starting lineup scenario, and add records to insert_data list variable.
        2. Update lineup to include incoming substitution (play_event_id == 10 & sequence == 1).
        3. Update linup to remove outgoing player substition (play_event_id == 10 & sequence == 2). Then add records to insert_data list variable.
        4. Handle other plays as normal.

To limit redundant database calls, there is a last_play_id iterator to ensure that only one run_hash_comparisons() call is made per play.

## Pre & Post Processing
Initial loading of the Excel files is handled via Pandas, as well as the initial sorting and filtering. But I intentionally dump the dataset back into a regular dictionary prior to the main lineup sequencing because looping within Pandas is very much an anti-pattern.


