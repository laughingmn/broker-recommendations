"""
Lambda handlers for broker recommendations application.
"""

import json
import os
import logging
from typing import Dict, Any, List
from datetime import datetime
from dataclasses import asdict
from collections import defaultdict

from src.models import HealthResponse, ErrorResponse, MessageResponse, TopCompany, TopBroker, BrokerRecommendation
from src.crawler import MoneyControlCrawler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_KEY = os.environ.get("API_KEY", "dev-api-key-123")


def validate_api_key(event: Dict[str, Any]) -> bool:
    headers = event.get("headers", {}) or {}
    for key, value in headers.items():
        if key.lower() == "x-api-key":
            return value == API_KEY
    return False


def create_response(status_code: int, body: Any) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Api-Key",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    }

    if isinstance(body, str):
        body_json = body
    elif isinstance(body, dict):
        body_json = json.dumps(body, default=str, indent=2)
    else:
        body_json = json.dumps(asdict(body), default=str, indent=2)

    return {"statusCode": status_code, "headers": headers, "body": body_json}


def filter_recommendations(recommendations: List[BrokerRecommendation]) -> List[BrokerRecommendation]:
    """Filter out invalid or low-quality recommendations"""
    filtered = []
    for rec in recommendations:
        if (
            rec.company_name
            and rec.recommendation
            and len(rec.company_name) > 2
            and rec.recommendation in ["BUY", "SELL", "HOLD"]
        ):
            filtered.append(rec)
    return filtered


def get_top_companies(recommendations: List[BrokerRecommendation], limit: int = 10) -> List[tuple]:
    """Get top companies by average target price"""
    company_targets = defaultdict(list)

    for rec in recommendations:
        if rec.target_price > 0:
            company_targets[rec.company_name].append(rec.target_price)

    company_averages = []
    for company, targets in company_targets.items():
        avg_target = sum(targets) / len(targets)
        company_averages.append((company, avg_target))

    return sorted(company_averages, key=lambda x: x[1], reverse=True)[:limit]


def get_top_brokers(recommendations: List[BrokerRecommendation], limit: int = 10) -> List[tuple]:
    """Get top brokers by best target price"""
    broker_targets = defaultdict(list)

    for rec in recommendations:
        if rec.target_price > 0:
            broker_targets[rec.broker_name].append(rec.target_price)

    broker_best = []
    for broker, targets in broker_targets.items():
        best_target = max(targets)
        broker_best.append((broker, best_target))

    return sorted(broker_best, key=lambda x: x[1], reverse=True)[:limit]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        logger.info("=== Starting recommendations request ===")

        if event.get("httpMethod") == "OPTIONS":
            return create_response(200, "")

        if not validate_api_key(event):
            logger.warning("Invalid API key")
            return create_response(401, ErrorResponse(error="Invalid API key"))

        logger.info("API key validated")
        logger.info("Starting crawler...")

        crawler = MoneyControlCrawler()
        recommendations = crawler.get_recommendations()
        logger.info(f"Raw recommendations: {len(recommendations)}")

        filtered_recommendations = filter_recommendations(recommendations)
        logger.info(f"After filtering: {len(filtered_recommendations)}")

        # Format response data
        recommendations_data = []
        for rec in filtered_recommendations:
            recommendations_data.append(
                {
                    "broker_name": rec.broker_name,
                    "company_name": rec.company_name,
                    "recommendation": rec.recommendation,
                    "target_price": rec.target_price,
                    "current_price": rec.current_price,
                    "reporting_date": rec.reporting_date.isoformat() if rec.reporting_date else None,
                }
            )

        response_data = {
            "recommendations": recommendations_data,
            "timestamp": datetime.now().isoformat(),
            "total_recommendations": len(filtered_recommendations),
        }

        logger.info(f"=== Returning {len(filtered_recommendations)} recommendations ===")
        return create_response(200, response_data)

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return create_response(500, ErrorResponse(error=str(e)))


def top_companies_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        logger.info("Processing top companies request")

        if event.get("httpMethod") == "OPTIONS":
            return create_response(200, "")

        if not validate_api_key(event):
            return create_response(401, ErrorResponse(error="Invalid API key"))

        crawler = MoneyControlCrawler()
        recommendations = crawler.get_recommendations()
        filtered_recommendations = filter_recommendations(recommendations)

        logger.info(f"Analyzing {len(filtered_recommendations)} recommendations for top companies")

        top_companies = get_top_companies(filtered_recommendations)

        top_companies_data = [
            TopCompany(rank=i + 1, company_name=company, avg_target_price=round(target, 2))
            for i, (company, target) in enumerate(top_companies)
        ]

        response_data = {
            "top_companies": top_companies_data,
            "timestamp": datetime.now().isoformat(),
            "based_on_recommendations": len(filtered_recommendations),
        }

        return create_response(200, response_data)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, ErrorResponse(error=str(e)))


def top_brokers_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        logger.info("Processing top brokers request")

        if event.get("httpMethod") == "OPTIONS":
            return create_response(200, "")

        if not validate_api_key(event):
            return create_response(401, ErrorResponse(error="Invalid API key"))

        crawler = MoneyControlCrawler()
        recommendations = crawler.get_recommendations()
        filtered_recommendations = filter_recommendations(recommendations)

        logger.info(f"Analyzing {len(filtered_recommendations)} recommendations for top brokers")

        top_brokers = get_top_brokers(filtered_recommendations)

        top_brokers_data = [
            TopBroker(rank=i + 1, broker_name=broker, best_target_price=round(target, 2))
            for i, (broker, target) in enumerate(top_brokers)
        ]

        response_data = {
            "top_brokers": top_brokers_data,
            "timestamp": datetime.now().isoformat(),
            "based_on_recommendations": len(filtered_recommendations),
        }

        return create_response(200, response_data)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, ErrorResponse(error=str(e)))


def health_check_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    health_data = HealthResponse(status="healthy", timestamp=datetime.now().isoformat(), service="broker-recommendations")
    return create_response(200, health_data)


def cleanup_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        if not validate_api_key(event):
            return create_response(401, ErrorResponse(error="Invalid API key"))

        message_data = MessageResponse(message="Docker container handles cleanup automatically")
        return create_response(200, message_data)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, ErrorResponse(error=str(e)))


def stats_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        if not validate_api_key(event):
            return create_response(401, ErrorResponse(error="Invalid API key"))

        crawler = MoneyControlCrawler()
        recommendations = crawler.get_recommendations()
        filtered_recommendations = filter_recommendations(recommendations)

        buy_count = sum(1 for rec in filtered_recommendations if rec.recommendation == "BUY")
        sell_count = sum(1 for rec in filtered_recommendations if rec.recommendation == "SELL")

        avg_target = 0
        if filtered_recommendations:
            targets = [rec.target_price for rec in filtered_recommendations if rec.target_price > 0]
            avg_target = sum(targets) / len(targets) if targets else 0

        stats_data = {
            "total_recommendations": len(filtered_recommendations),
            "buy_recommendations": buy_count,
            "sell_recommendations": sell_count,
            "average_target_price": round(avg_target, 2),
            "timestamp": datetime.now().isoformat(),
        }

        return create_response(200, stats_data)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, ErrorResponse(error=str(e)))
