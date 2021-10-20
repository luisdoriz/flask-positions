from flask import Flask, request, Response
from dotenv import load_dotenv
from os import environ
import json

from src.controllers.areas import AreaProcessor
from src.controllers.positions import CoordsProcesor
from src.modules.compute import Compute

load_dotenv()

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
    output = {"status": "success", "version": 0.1, "environment": environ.get("ENV")}
    return Response(
        response=json.dumps(output), status=200, mimetype="application/json"
    )


@app.route("/clean_beacon_data", methods=["POST"])
def trigger_process():
    try:
        thread_a = Compute(request.__copy__(), class_name=CoordsProcesor)
        thread_a.start()
        output = {"status": "success", "message": "process triggered"}
        status = 200
    except:
        status = 500
        output = {"status": "error", "message": "process not triggered"}
    return Response(
        response=json.dumps(output), status=status, mimetype="application/json"
    )


@app.route("/areas", methods=["POST"])
def trigger_areas_process():
    try:
        thread_a = Compute(request.__copy__(), class_name=AreaProcessor)
        thread_a.start()
        output = {"status": "success", "message": "process triggered"}
        status = 200
    except:
        output = {"status": "error", "message": "process not triggered"}
        status = 500
    return Response(
        response=json.dumps(output), status=status, mimetype="application/json"
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=3001)
