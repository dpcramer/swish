import pandas as pd
import mysql.connector
import os
import sys
from dotenv import load_dotenv


class PbpPlayersByEventHandler:
    def __init__(self, pbp_players_by_event):
        self.pbp_players_by_event = pbp_players_by_event
        self.last_play_id = None
        self.lineup = []
        self.insert_data = []
        self.set_starting_lineup()
        self.sql_client = MysqlClient()
        self.hashing_sql_client = MysqlClient()

    def set_starting_lineup(self):
        for play in self.pbp_players_by_event:
            if play['play_id'] == 1:
                self.lineup.append(play['player_id'])
            else:
                break

    def create_player_rows_per_play(self, play: dict):
        for player in self.lineup:
            insert_record = (play['event_id'], play['play_id'], player)
            self.insert_data.append(insert_record)
        self.last_play_id = play['play_id']

    def get_lineup_hash(self):
        sorted_lineup = sorted(self.lineup)
        lineup_hash = hash(tuple(sorted_lineup))
        return lineup_hash

    def query_play_hash_from_database(self, play: dict):
        event_id = play['event_id']
        play_id = play['play_id']
        # query hash for event and play_id
        query = f"SELECT lineup_hash from players_on_court_hash WHERE event_id = {event_id} AND play_id = {play_id}"
        event_handler.hashing_sql_client.cursor.execute(query)
        stored_hash = event_handler.hashing_sql_client.cursor.fetchall()
        return stored_hash

    def write_hash_to_database(self, play: dict, lineup_hash: int):
        hash_record = (play['event_id'], play['play_id'], lineup_hash)
        add_hash = ("INSERT INTO players_on_court_hash"
                    "(event_id, play_id, lineup_hash) "
                    "VALUES (%s, %s, %s)")
        event_handler.hashing_sql_client.cursor.execute(add_hash, hash_record)
        event_handler.hashing_sql_client.cnx.commit()

    def update_and_delete_changed_data_from_database(self, play: dict, lineup_hash: int):
        event_id = play['event_id']
        play_id = play['play_id']
        print('updating data store')
        update_hash = f"update players_on_court_hash set lineup_hash = {lineup_hash} where event_id = {event_id} and play_id = {play_id}"
        event_handler.hashing_sql_client.cursor.execute(update_hash)
        event_handler.hashing_sql_client.cnx.commit()
        delete_players_on_court = f"delete from pbp_players_on_court where event_id = {event_id} and play_id = {play_id}"
        event_handler.hashing_sql_client.cursor.execute(delete_players_on_court)
        event_handler.hashing_sql_client.cnx.commit()

    def run_hash_comparisons(self, play: dict, current_lineup_hash: hash, stored_hash_records):
        # skip if the stored lineup hash matches the new lineup hash
        if len(stored_hash_records) > 0 and stored_hash_records[0][0] == current_lineup_hash:
            # print('no change - skipping play')
            return
        # if no hash exists, write the current hash
        if len(stored_hash_records) == 0:
            # print('no hash exists - adding')
            event_handler.write_hash_to_database(play, current_lineup_hash)
            return
        # If not equal, delete all records for play and update hash
        if len(stored_hash_records) > 0 and stored_hash_records[0][0] != current_lineup_hash:
            # 'play updated'
            event_handler.update_and_delete_changed_data_from_database(play, current_lineup_hash)

class MysqlClient:
    def __init__(self):
        load_dotenv()
        database_user = os.environ.get('DB_USERNAME')
        database = os.environ.get('DB_NAME')
        database_password = os.environ.get('DB_PASSWORD')
        self.cnx = mysql.connector.connect(user=database_user, database=database, password=database_password)
        self.cursor = self.cnx.cursor()

    def write_records_and_close_connection(self):
        self.cnx.commit()
        self.cursor.close()
        self.cnx.close()


if __name__ == "__main__":
    file_location = 'pbp_players.xlsx'
    raw_data = pd.read_excel(file_location)
    df = raw_data[['event_id', 'player_id', 'team_id', 'play_id', 'play_event_id', 'sequence', 'play_sequence']].copy()
    del raw_data
    df = df.astype({'player_id': 'Int64', 'team_id': 'Int64', 'sequence': 'Int64'})
    df = df.sort_values(by=['event_id', 'play_id', 'play_sequence'], ascending=True)
    if len(sys.argv) > 1:
        event_ids = [int(sys.argv[1])]
    else:
        event_ids = df.event_id.unique()

    for event_id in event_ids:
        print(event_id)
        print(df)
        event_data = df[df['event_id'] == event_id]
        print(event_data)
        event_data_dict = event_data.to_dict('records')
        event_handler = PbpPlayersByEventHandler(event_data_dict)

        for play in event_handler.pbp_players_by_event:
            # create insert records for starting lineup (play 1)
            stored_hash_records = event_handler.query_play_hash_from_database(play)
            current_lineup_hash = event_handler.get_lineup_hash()

            # Handle lineup for initial play
            if play['play_id'] == 1 and play['play_sequence'] == 1 and event_handler.last_play_id is None:
                event_handler.run_hash_comparisons(play, current_lineup_hash, stored_hash_records)
                # then write new records for play
                event_handler.create_player_rows_per_play(play)

            # add incoming substituted player to lineup
            elif play['play_event_id'] == 10 and play['play_sequence'] == 1:
                player_id = play['player_id']
                play_id = play['play_id']
                print(f'adding {player_id} event {play_id} ')
                # print(f'before lineup - {event_handler.lineup}')
                event_handler.lineup.append(play['player_id'])
                # print(f'after lineup {event_handler.lineup}')

            # remove outgoing substituted player and create insert records for play
            elif play['play_event_id'] == 10 and play['play_sequence'] == 2: #and play['play_id'] > event_handler.last_play_id:
                player_id = play['player_id']
                event_id = play['event_id']
                play_id = play['play_id']
                print(f'removing {player_id}, play {play_id}')
                # print(f'before lineup - {event_handler.lineup}')
                event_handler.lineup.remove(play['player_id'])
                print(f'after lineup {event_handler.lineup}')
                current_lineup_hash = event_handler.get_lineup_hash()
                event_handler.run_hash_comparisons(play, current_lineup_hash, stored_hash_records)
                event_handler.create_player_rows_per_play(play)

            # handle other records
            elif play['play_id'] > event_handler.last_play_id:
                event_handler.run_hash_comparisons(play, current_lineup_hash, stored_hash_records)
                event_handler.create_player_rows_per_play(play)

            else:
                pass

        for play in event_handler.insert_data:
            add_game = ("INSERT INTO pbp_players_on_court "
                        "(event_id, play_id, player_id) "
                        "VALUES (%s, %s, %s)")
            event_handler.sql_client.cursor.execute(add_game, play)
        event_handler.sql_client.write_records_and_close_connection()

