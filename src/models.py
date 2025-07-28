"""
Data models for broker recommendations application.
"""

from typing import List, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class BrokerRecommendation:
    """Main data model for broker recommendations."""

    broker_name: str
    company_name: str
    recommendation: str
    target_price: float
    current_price: float
    reporting_date: datetime


@dataclass
class TopCompany:
    """Top company ranking data."""

    rank: int
    company_name: str
    avg_target_price: float


@dataclass
class TopBroker:
    """Top broker ranking data."""

    rank: int
    broker_name: str
    best_target_price: float


@dataclass
class RecommendationResponse:
    """Structured recommendation response data."""

    broker_name: str
    company_name: str
    recommendation: str
    target_price: float
    current_price: float
    reporting_date: Optional[str]


@dataclass
class RecommendationsData:
    """Complete recommendations response payload."""

    timestamp: str
    total_recommendations: int
    top_companies: List[TopCompany]
    top_brokers: List[TopBroker]
    recommendations: List[RecommendationResponse]


@dataclass
class HealthResponse:
    """Health check response data."""

    status: str
    timestamp: str
    service: str


@dataclass
class ErrorResponse:
    """Error response data."""

    error: str


@dataclass
class MessageResponse:
    """Generic message response data."""

    message: str
    timestamp: Optional[str] = None


@dataclass
class ApiHeaders:
    """Standard API response headers."""

    content_type: str = "application/json"
    access_control_allow_origin: str = "*"
    access_control_allow_headers: str = "Content-Type,X-Api-Key"
    access_control_allow_methods: str = "GET,POST,OPTIONS"
