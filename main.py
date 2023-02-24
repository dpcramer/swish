import pandas as pd
import mysql.connector
import os
import sys
from dotenv import load_dotenv


class PbpPlayersByEventHandler:
    def __init__(self, pbp_players_by_event):
        self.pbp_players_by_event = pbp_players_by_event
        self.home_team = None
        self.away_team = None
        self.teams = []
        self.home_lineup = []
        self.away_lineup = []
        self.set_team_ids()
        self.set_starting_lineups()
        self.players_on_court_insert_data = []

    def set_team_ids(self):
        self.home_team = self.pbp_players_by_event[0]['home_team_id']
        self.away_team = self.pbp_players_by_event[0]['away_team_id']
        self.teams = [self.home_team, self.away_team]

    def set_starting_lineups(self):
        for play in self.pbp_players_by_event:
            if play['play_id'] == 1 and play['team_id'] == self.home_team:
                self.home_lineup.append(play['player_id'])
            elif play['play_id'] == 1 and play['team_id'] == self.away_team:
                self.away_lineup.append(play['player_id'])

    def create_player_rows_per_play(self, play: dict, team: str):
        if team == 'home':
            is_anomaly = 1
            if len(self.home_lineup) == 5:
                is_anomaly = 0
            for player in self.home_lineup:
                insert_record = (play['event_id'], play['play_id'], self.home_team, player, is_anomaly)
                self.players_on_court_insert_data.append(insert_record)
        if team == 'away':
            is_anomaly = 1
            if len(self.away_lineup) == 5:
                is_anomaly = 0
            for player in self.away_lineup:
                insert_record = (play['event_id'], play['play_id'], self.away_team, player, is_anomaly)
                self.players_on_court_insert_data.append(insert_record)

    def create_lineup_hashes(self):
        home_sorted_lineup = sorted(self.home_lineup)
        home_lineup_hash = hash(tuple(home_sorted_lineup))
        away_sorted_lineup = sorted(self.away_lineup)
        away_lineup_hash = hash(tuple(away_sorted_lineup))
        return home_lineup_hash, away_lineup_hash

    def query_play_hash_from_database(self, play: dict):
        event_id = play['event_id']
        play_id = play['play_id']
        # query hash for event and play_id
        home_query = f"SELECT lineup_hash from players_on_court_hash WHERE event_id = {event_id} AND play_id = {play_id} and team_id = {self.home_team}"
        event_handler.hashing_sql_client.cursor.execute(home_query)
        home_hash = event_handler.hashing_sql_client.cursor.fetchall()
        away_query = f"SELECT lineup_hash from players_on_court_hash WHERE event_id = {event_id} AND play_id = {play_id} and team_id = {self.away_team}"
        event_handler.hashing_sql_client.cursor.execute(away_query)
        away_hash = event_handler.hashing_sql_client.cursor.fetchall()
        return home_hash, away_hash

    def write_hash_to_database(self, play: dict, team_id: int, lineup_hash: int):
        hash_record = (play['event_id'], play['play_id'], team_id, lineup_hash)
        add_hash = ("INSERT INTO players_on_court_hash"
                    "(event_id, play_id, team_id, lineup_hash) "
                    "VALUES (%s, %s, %s, %s)")
        event_handler.hashing_sql_client.cursor.execute(add_hash, hash_record)
        event_handler.hashing_sql_client.cnx.commit()

    def update_and_delete_changed_data_from_database(self, play: dict, team_id: int, lineup_hash: int):
        event_id = play['event_id']
        play_id = play['play_id']
        update_hash = f"update players_on_court_hash set lineup_hash = {lineup_hash} where event_id = {event_id} and play_id = {play_id} and team_id = {team_id}"
        event_handler.hashing_sql_client.cursor.execute(update_hash)
        event_handler.hashing_sql_client.cnx.commit()
        delete_players_on_court = f"delete from pbp_players_on_court where event_id = {event_id} and play_id = {play_id} and team_id = {team_id}"
        event_handler.hashing_sql_client.cursor.execute(delete_players_on_court)
        event_handler.hashing_sql_client.cnx.commit()

    def run_hash_comparisons_for_team_and_play(self, play: dict, current_lineup_hash: hash, stored_hash_record: hash, team_id: int):
        # skip if the stored lineup hash matches the new lineup hash
        if len(stored_hash_record) > 0 and stored_hash_record == current_lineup_hash:
            return 'skip'
        # if no hash exists, write the current hash
        if len(stored_hash_record) == 0:
            event_handler.write_hash_to_database(play, team_id, current_lineup_hash)
            return 'update'
        # If not equal, delete all records for play and update hash
        if len(stored_hash_record) > 0 and stored_hash_record[0][0] != current_lineup_hash:
            event_handler.update_and_delete_changed_data_from_database(play, team_id, current_lineup_hash)
            for record in event_handler.players_on_court_insert_data:
                if record[0] == play['event_id'] and record[1] == play['play_id'] and record[2] == team_id:
                    event_handler.players_on_court_insert_data.remove(record)
            return 'update'

    def compare_and_update_home_and_away_hashes(self, play):
        stored_home_hash, stored_away_hash = self.query_play_hash_from_database(play)
        home_lineup_hash, away_lineup_hash = self.create_lineup_hashes()
        home_comparison_result = self.run_hash_comparisons_for_team_and_play(play, home_lineup_hash,
                                                                             stored_home_hash,
                                                                             self.home_team)
        away_comparison_result = self.run_hash_comparisons_for_team_and_play(play, away_lineup_hash,
                                                                             stored_away_hash,
                                                                             self.away_team)
        # then write new records for play
        if home_comparison_result == 'update':
            self.create_player_rows_per_play(play, 'home')
        if away_comparison_result == 'update':
            self.create_player_rows_per_play(play, 'away')


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

    def write_records(self):
        self.cnx.commit()
        self.cursor.close()

    def close_connection(self):
        self.cnx.close()


if __name__ == "__main__":
    file_location = 'pbp_players.xlsx'
    raw_data = pd.read_excel(file_location)
    df = raw_data[['event_id', 'player_id', 'home_team_id', 'away_team_id', 'team_id',
                   'play_id', 'play_event_id', 'play_sequence']].copy()
    del raw_data
    df = df.astype({'event_id': 'Int64', 'player_id': 'Int64', 'home_team_id': 'Int64', 'away_team_id': 'Int64',
                    'team_id': 'Int64', 'play_id': 'Int64', 'play_event_id': 'Int64', 'play_sequence': 'Int64'})
    df = df.sort_values(by=['event_id', 'play_id', 'play_sequence'], ascending=[True, True, False])
    df['team_id'] = df['team_id'].fillna(0)
    if len(sys.argv) > 1:
        event_ids = [int(sys.argv[1])]
    else:
        event_ids = df.event_id.unique()

    for event_id in event_ids:
        event_data = df[df['event_id'] == event_id]
        event_data_dict = event_data.to_dict('records')
        event_handler = PbpPlayersByEventHandler(event_data_dict)

        for play in event_handler.pbp_players_by_event:
            event_handler.hashing_sql_client = MysqlClient()

            # Handle lineup for initial play
            if play['play_id'] == 1 and play['play_sequence'] == 1:
                event_handler.compare_and_update_home_and_away_hashes(play)

            # Handle records that didn't have team_id
            if play['team_id'] == 0:
                event_handler.compare_and_update_home_and_away_hashes(play)

            # remove outgoing substituted player
            if play['play_event_id'] == 10 and play['play_sequence'] == 2:
                if play['team_id'] == event_handler.home_team:
                    if play['player_id'] in event_handler.home_lineup:
                        event_handler.home_lineup.remove(play['player_id'])
                elif play['team_id'] == event_handler.away_team:
                    if play['player_id'] in event_handler.away_lineup:
                        event_handler.away_lineup.remove(play['player_id'])

            # add incoming substituted player and create records
            if play['play_event_id'] == 10 and play['play_sequence'] == 1:
                if play['team_id'] == event_handler.home_team and play['player_id'] not in event_handler.home_lineup:
                    event_handler.home_lineup.append(play['player_id'])
                elif play['team_id'] == event_handler.away_team and play['player_id'] not in event_handler.away_lineup:
                    event_handler.away_lineup.append(play['player_id'])
                event_handler.compare_and_update_home_and_away_hashes(play)

            # Add player to lineup if doesn't exist
            if play['play_id'] > 1 and play['team_id'] > 0 and play['play_event_id'] != 10:
                if play['team_id'] == event_handler.home_team and play['player_id'] not in event_handler.home_lineup:
                    event_handler.home_lineup.append(play['player_id'])
                if play['team_id'] == event_handler.away_team and play['player_id'] not in event_handler.away_lineup:
                    event_handler.away_lineup.append(play['player_id'])
                event_handler.compare_and_update_home_and_away_hashes(play)

        event_handler.sql_client = MysqlClient()
        for play in event_handler.players_on_court_insert_data:
            add_game = ("INSERT INTO pbp_players_on_court "
                        "(event_id, play_id, team_id, player_id, is_anomaly) "
                        "VALUES (%s, %s, %s, %s, %s)")
            event_handler.sql_client.cursor.execute(add_game, play)
        event_handler.sql_client.write_records_and_close_connection()
