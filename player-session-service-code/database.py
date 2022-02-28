from datetime import datetime, timedelta
from cassandra.cluster import Cluster
import pandas as pd

"""
==========
database
==========
This module contains functions to interact with Cassandra.
==========
* CassandraDB - Cassandra Database Instance.
* get_player_events()  - This function will fetch completed events for a player from Cassandra.
* save_batch()- This function insert a batch of events into Cassandra.
"""


class CassandraDB:
    """
    Cassandra Database Instance.
    ===========
    Keyspace name : sessionkeyspace
    Tables: start_events and end_events
    """

    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('sessionkeyspace')
        self.start_events_insert_stmt = self.session.prepare('INSERT INTO start_events(country,player_id,session_id,ts)'
                                                             ' values(?,?,?,?)')
        self.end_events_insert_stmt = self.session.prepare('INSERT INTO end_events(player_id,session_id,ts)'
                                                           ' values(?,?,?)')

        self.events_select_stmt = self.session.prepare(
            'SELECT JSON * FROM end_events WHERE player_id=? and ts > ? LIMIT 20')
        

    def get_player_events(self, player):
        """
        Returns: Completed events for the given player.
        ===========
        Fetches completed events from Cassandra Database for the given player,
        events older than 1 year will be discarded.
        """
        current_time = datetime.now() - timedelta(days=365)
        events = pd.DataFrame(self.session.execute(self.events_select_stmt, (player, current_time)).current_rows)

        return events

    def save_batch(self, batch):
        """
        Returns:
        ===========
        Saves a batch of events to the Cassandra Database,
        start events get inserted into start_events
        end events get inserted into end_events
        """
        try:
            for event in batch:
                time_stamp = datetime.strptime(event['ts'], '%Y-%m-%dT%H:%M:%S')

                if event['event'] == 'start':
                    self.session.execute(self.start_events_insert_stmt, (event['country'], event['player_id'],
                                                                         event['session_id'], time_stamp))
                else:
                    self.session.execute(self.end_events_insert_stmt,
                                         (event['player_id'], event['session_id'], time_stamp))
        except:
            return False
        return True
