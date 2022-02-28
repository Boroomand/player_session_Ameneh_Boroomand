from flask import Flask, request, jsonify
from database import CassandraDB

app = Flask(__name__)

db = CassandraDB()


@app.route('/session/<player_id>', methods=['GET'])
def fetch_player_session(player_id):
    """
    Returns: Completed events for the given player
    ===========
    calls database function get_player_events() to fetch completed events from the Database for the given player,
    Events older than 1 year will be discarded.
    """
    events = db.get_player_events(player_id)
    return events.to_json(orient='values')


@app.route('/batch', methods=['POST'])
def add_events_batch():
    """
    Returns: Status of the insert (sucessful or unsucessful)
     ===========
    calls database function save_batch() to save a batch into the database,
    If action performed without error returns Successful and if not returns Unsuccessful.
    """
    batch = request.get_json()
    insert_status = db.save_batch(batch)
    if insert_status:
        response = 'Successful'
    else:
        response = 'Unsuccessful'
    return jsonify(response)


if __name__ == '__main__':
    app.run()
