import multiprocessing
from flask import Flask, request, jsonify
from uuid import uuid4
import threading

host_name = "0.0.0.0"
port = 6003

app = Flask(__name__)             # create an app instance

APP_VERSION = "1.0.2"
 
_events_queue: multiprocessing.Queue = None

@app.route("/alerts", methods=['GET'])
def get_alerts():
    auth = request.headers['auth']
    if auth != 'very-secure-token':
        return "unauthorized", 401

    events = []

    while True:
        try:
            event = _events_queue.get_nowait()

            for item in event["alerts"]:
                events.append(item)
        except Exception as _:
            break

    return jsonify(events)

def start_rest(events_queue):
    global _events_queue 
    _events_queue = events_queue
    threading.Thread(target=lambda: app.run(host=host_name, port=port, debug=True, use_reloader=False)).start()

if __name__ == "__main__":        # on running python app.py
    start_rest()