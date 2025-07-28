from unittest.mock import patch
from src.crawler import MoneyControlCrawler


class TestMoneyControlCrawler:
    def test_crawler_initialization(self):
        crawler = MoneyControlCrawler()
        assert crawler.base_url == "https://www.moneycontrol.com/markets/stock-ideas/"
        assert "Mozilla" in crawler.session.headers["User-Agent"]

    def test_extract_recommendation(self):
        crawler = MoneyControlCrawler()
        assert crawler._extract_recommendation("BUY") == "BUY"
        assert crawler._extract_recommendation("SELL") == "SELL"
        assert crawler._extract_recommendation("HOLD") == "HOLD"
        assert crawler._extract_recommendation("ACCUMULATE") == "BUY"

    def test_clean_company_name(self):
        crawler = MoneyControlCrawler()
        assert crawler._clean_company_name("Buy Reliance Industries") == "Reliance Industries"
        assert crawler._clean_company_name("TCS: Motilal Oswal") == "TCS"

    @patch("src.crawler.requests.Session.get")
    def test_get_recommendations_api_failure(self, mock_get):
        mock_get.side_effect = Exception("Network error")
        crawler = MoneyControlCrawler()
        result = crawler.get_recommendations()
        assert result == []
