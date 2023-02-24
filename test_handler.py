import unittest
import json
from main import PbpPlayersByEventHandler


class TestHandler(unittest.TestCase):
    def setUp(self):
        self.file = open('test_data.json')
        self.test_data = json.load(self.file)
        self.handler = PbpPlayersByEventHandler(self.test_data)

    def tearDown(self) -> None:
        self.file.close()

    def test_set_team_ids(self):
        expected_teams = [2, 9]
        self.assertEqual(self.handler.teams, expected_teams)

    def test_set_starting_lineups(self):
        expected_home = [280587, 937647, 883436, 551768, 469089]
        expected_away = [468895, 457611, 338365, 329525, 214168]
        self.assertEqual(self.handler.home_lineup, expected_home)
        self.assertEqual(self.handler.away_lineup, expected_away)

    def test_create_home_player_rows_per_play(self):
        expected_records = [(1947160, 1, 2, 280587, 0),
                            (1947160, 1, 2, 937647, 0),
                            (1947160, 1, 2, 883436, 0),
                            (1947160, 1, 2, 551768, 0),
                            (1947160, 1, 2, 469089, 0)]
        play = self.test_data[0]
        self.handler.create_player_rows_per_play(play, 'home')
        self.assertEqual(self.handler.players_on_court_insert_data, expected_records)

    def test_create_away_player_rows_per_play(self):
        expected_records = [(1947160, 1, 9, 468895, 0),
                            (1947160, 1, 9, 457611, 0),
                            (1947160, 1, 9, 338365, 0),
                            (1947160, 1, 9, 329525, 0),
                            (1947160, 1, 9, 214168, 0)]
        play = self.test_data[0]
        self.handler.create_player_rows_per_play(play, 'away')
        self.assertEqual(self.handler.players_on_court_insert_data, expected_records)