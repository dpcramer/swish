import unittest
import json
from main import PbpPlayersByEventHandler


class TestHandler(unittest.TestCase):
    def setUp(self):
        self.file = open('test_data.json')
        test_data = json.load(self.file)
        self.handler = PbpPlayersByEventHandler(test_data)

    def tearDown(self) -> None:
        self.file.close()

    def test_set_team_ids(self):
        expected_teams = [2, 9]
        self.assertEqual(self.handler.teams, expected_teams)