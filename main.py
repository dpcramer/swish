import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv


class PbpPlayersByEventHandler:
    def __init__(self, pbp_players_by_event):
        self.pbp_players_by_event = pbp_players_by_event
        self.last_play_id = None
        self.lineup = []
        self.insert_data = []

    def set_starting_lineup(self):
        for play in self.pbp_players_by_event:
            if play['play_id'] == 1:
                self.lineup.append(play['player_id'])
            else:
                break

    def create_player_rows_per_play(self, play):
        for player in self.lineup:
            insert_record = (play['event_id'], play['play_id'], player)
            self.insert_data.append(insert_record)
        self.last_play_id = play['play_id']


class MysqlClient:
    def __init__(self):
        load_dotenv()
        database_user = os.environ.get('DB_USERNAME')
        database = os.environ.get('DB_NAME')
        database_password = os.environ.get('DB_PASSWORD')
        self.cnx = mysql.connector.connect(user=database_user, database=database, password=database_password)
        self.cursor = self.cnx.cursor()

    def write_records(self):
        self.cnx.commit()
        self.cursor.close()
        self.cnx.close()


if __name__ == "__main__":
    file_location = 'pbp_players.xlsx'
    raw_data = pd.read_excel(file_location)
    df = raw_data[['event_id', 'player_id', 'team_id', 'play_id', 'play_event_id', 'sequence', 'play_sequence']].copy()
    del raw_data
    df = df.astype({'player_id': 'Int64', 'team_id': 'Int64', 'sequence': 'Int64'})
    df = df.sort_values(by=['play_id', 'play_sequence'], ascending=True)
    event_ids = df.event_id.unique()

    for event_id in event_ids:
        event_data = df[df['event_id'] == event_id]
        event_data_dict = event_data.to_dict('records')
        event_handler = PbpPlayersByEventHandler(event_data_dict)

        # get_starting_lineup
        event_handler.set_starting_lineup()

        for play in event_handler.pbp_players_by_event:
            # create insert records for starting lineup (play 1)
            if play['play_id'] == 1 and play['play_sequence'] == 1 and event_handler.last_play_id is None:
                event_handler.create_player_rows_per_play(play)

            # add incoming substituted player to lineup
            if play['play_event_id'] == 10 and play['play_sequence'] == 1 and play['play_id'] > event_handler.last_play_id:
                event_handler.lineup.append(play['player_id'])

            # remove outgoing substituted player and create insert records for play
            if play['play_event_id'] == 10 and play['play_sequence'] == 1 and play['play_id'] > event_handler.last_play_id:
                event_handler.lineup.remove(play['player_id'])
                event_handler.create_player_rows_per_play(play)

            # handle other records
            if play['play_id'] > event_handler.last_play_id:
                event_handler.create_player_rows_per_play(play)

        sql_client = MysqlClient()
        for play in event_handler.insert_data:
            add_game = ("INSERT INTO pbp_players_on_court "
                        "(event_id, play_id, player_id) "
                        "VALUES (%s, %s, %s)")
            sql_client.cursor.execute(add_game, play)
        sql_client.write_records()

