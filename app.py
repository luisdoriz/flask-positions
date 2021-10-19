import os
import json

from src.controllers.areas import AreaProcessor
from src.controllers.positions import CoordsProcesor
from src.modules.compute import Compute
from flask import Flask, request

app = Flask(__name__)


def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    if request.method == "OPTIONS":
        response.headers[
            "Access-Control-Allow-Methods"
        ] = "DELETE,  \
          GET,POST, PUT"
        headers = request.headers.get("Access-Control-Request-Headers")
        if headers:
            response.headers["Access-Control-Allow-Headers"] = headers
    return response


app.after_request(add_cors_headers)


@app.route("/")
def home():
    output = {"status": "success", "version": 0.1}
    return output


@app.route("/clean_beacon_data", methods=["POST"])
def trigger_process():
    try:
        thread_a = Compute(request.__copy__(), class_name=CoordsProcesor)
        thread_a.start()
        output = {"status": "success", "message": "process triggered"}
    except:
        output = {"status": "error", "message": "process not triggered"}
    return output


@app.route("/areas", methods=["POST"])
def trigger_areas_process():
    try:
        thread_a = Compute(request.__copy__(), class_name=AreaProcessor)
        thread_a.start()
        output = {"status": "success", "message": "process triggered"}
    except:
        output = {"status": "error", "message": "process not triggered"}
    return output


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=3001)
