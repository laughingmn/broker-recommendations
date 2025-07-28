import json
import os
import logging
from typing import Dict, Any, Tuple
from flask import Flask, request, jsonify, Response
from src.handlers import (
    lambda_handler,
    health_check_handler,
    cleanup_handler,
    stats_handler,
    top_companies_handler,
    top_brokers_handler,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)


def create_lambda_event(path: str, method: str) -> Dict[str, Any]:
    return {
        "httpMethod": method,
        "path": path,
        "headers": dict(request.headers),
        "queryStringParameters": dict(request.args) if request.args else None,
        "body": request.get_data(as_text=True) if request.data else None,
    }


def create_flask_response(lambda_response: Dict[str, Any]) -> Tuple[Response, int]:
    try:
        body_data = json.loads(lambda_response["body"]) if lambda_response.get("body") else {}
        status_code = lambda_response.get("statusCode", 200)
        return jsonify(body_data), status_code
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid response format"}), 500


@app.route("/health", methods=["GET", "OPTIONS"])
def health() -> Tuple[Response, int]:
    event = create_lambda_event("/health", request.method)
    response = health_check_handler(event, None)
    return create_flask_response(response)


@app.route("/recommendations", methods=["GET", "OPTIONS"])
def recommendations() -> Tuple[Response, int]:
    event = create_lambda_event("/recommendations", request.method)
    response = lambda_handler(event, None)
    return create_flask_response(response)


@app.route("/cleanup", methods=["POST", "OPTIONS"])
def cleanup() -> Tuple[Response, int]:
    event = create_lambda_event("/cleanup", request.method)
    response = cleanup_handler(event, None)
    return create_flask_response(response)


@app.route("/stats", methods=["GET", "OPTIONS"])
def stats() -> Tuple[Response, int]:
    event = create_lambda_event("/stats", request.method)
    response = stats_handler(event, None)
    return create_flask_response(response)


@app.route("/top-companies", methods=["GET", "OPTIONS"])
def top_companies() -> Tuple[Response, int]:
    event = create_lambda_event("/top-companies", request.method)
    response = top_companies_handler(event, None)
    return create_flask_response(response)


@app.route("/top-brokers", methods=["GET", "OPTIONS"])
def top_brokers() -> Tuple[Response, int]:
    event = create_lambda_event("/top-brokers", request.method)
    response = top_brokers_handler(event, None)
    return create_flask_response(response)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
