from typing import List
from datetime import datetime
import requests
import re
import logging
import json

from src.models import BrokerRecommendation

logger = logging.getLogger(__name__)


class MoneyControlCrawler:
    def __init__(self):
        self.base_url = "https://www.moneycontrol.com/markets/stock-ideas/"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
            }
        )

    def get_recommendations(self) -> List[BrokerRecommendation]:
        """
        Main method to get stock recommendations from MoneyControl
        Assignment URL: https://www.moneycontrol.com/markets/stock-ideas/
        """
        try:
            logger.info("Starting MoneyControl recommendations crawl")

            # Try Selenium first (best for dynamic content)
            recommendations = self._get_recommendations_with_selenium()
            if recommendations:
                logger.info(f"Found {len(recommendations)} recommendations via Selenium")
                return recommendations

            # Fallback to direct HTTP requests
            recommendations = self._get_recommendations_with_requests()
            if recommendations:
                logger.info(f"Found {len(recommendations)} recommendations via HTTP")
                return recommendations

            # No fallback data - return empty list if scraping fails
            logger.warning("Scraping failed - no recommendations found")
            return []

        except Exception as e:
            logger.error(f"Error fetching recommendations: {e}")
            return []

    def _get_recommendations_with_requests(self) -> List[BrokerRecommendation]:
        """HTTP requests with anti-bot protection bypassing"""
        import time
        import random

        # Try different request strategies
        strategies = [
            {
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Cache-Control": "max-age=0",
                    "Referer": "https://www.google.com/",
                }
            },
            {
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-us",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Referer": "https://www.moneycontrol.com/",
                }
            },
        ]

        for attempt, strategy in enumerate(strategies, 1):
            try:
                logger.debug(f"HTTP attempt {attempt} with strategy {attempt}")

                # Random delay between attempts
                if attempt > 1:
                    time.sleep(random.uniform(2, 4))

                # Create new session for each attempt
                session = requests.Session()
                session.headers.update(strategy["headers"])

                # Try different URLs
                urls_to_try = [
                    self.base_url,
                    f"{self.base_url}research/",
                    "https://www.moneycontrol.com/markets/stock-ideas/research/",
                ]

                for url in urls_to_try:
                    try:
                        logger.debug(f"Trying URL: {url}")
                        response = session.get(url, timeout=10)

                        logger.debug(f"Status: {response.status_code}, Content-Length: {len(response.text)}")

                        if response.status_code == 200:
                            recommendations = self._parse_html_content(response.text)
                            if recommendations:
                                logger.info(f"Successfully got {len(recommendations)} recommendations via HTTP")
                                return recommendations
                        elif response.status_code == 403:
                            logger.debug("403 Forbidden - trying next strategy")
                            break  # Try next strategy

                    except requests.exceptions.Timeout:
                        logger.debug(f"Timeout for {url}")
                        continue
                    except Exception as e:
                        logger.debug(f"Error with {url}: {e}")
                        continue

            except Exception as e:
                logger.debug(f"Strategy {attempt} failed: {e}")
                continue

        logger.debug("All HTTP strategies failed")
        return []

    def _get_recommendations_with_selenium(self) -> List[BrokerRecommendation]:
        """Selenium crawler with anti-bot protection bypassing"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            import os
            import time
            import random

            options = Options()

            # Anti-detection options
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-images")
            options.add_argument("--disable-javascript")
            options.add_argument("--window-size=1920,1080")

            # Docker environment options
            if os.environ.get("ENVIRONMENT") == "docker":
                logger.debug("Running in Docker environment - adding additional options")
                options.add_argument("--virtual-display-size=1920x1080x24")
                options.add_argument("--disable-background-timer-throttling")
                options.add_argument("--disable-renderer-backgrounding")
                options.add_argument("--disable-backgrounding-occluded-windows")
                options.add_argument("--disable-client-side-phishing-detection")
                options.add_argument("--disable-default-apps")
                options.add_argument("--disable-hang-monitor")
                options.add_argument("--disable-prompt-on-repost")
                options.add_argument("--disable-sync")
                options.add_argument("--disable-web-resources")
                options.add_argument("--enable-automation")
                options.add_argument("--log-level=3")
                options.add_argument("--silent")

            # Rotate user agents
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ]
            options.add_argument(f"--user-agent={random.choice(user_agents)}")

            if os.environ.get("CHROME_BIN"):
                options.binary_location = os.environ.get("CHROME_BIN")

            service = None
            if os.environ.get("CHROMEDRIVER_PATH"):
                service = Service(os.environ.get("CHROMEDRIVER_PATH"))

            driver = None
            try:
                logger.debug("Starting Selenium WebDriver")

                if service:
                    driver = webdriver.Chrome(service=service, options=options)
                else:
                    from webdriver_manager.chrome import ChromeDriverManager

                    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

                # Hide webdriver properties
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
                driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")

                # Try the main assignment URL
                urls_to_try = [
                    "https://www.moneycontrol.com/markets/stock-ideas/",
                    "https://www.moneycontrol.com/markets/stock-ideas/research/",
                    "https://www.moneycontrol.com/news/business/stocks/",
                    "https://www.moneycontrol.com/india/stockmarket/stocks/research/",
                ]

                recommendations = []
                for url in urls_to_try:
                    logger.debug(f"Trying URL: {url}")
                    try:
                        driver.get(url)

                        # Random delay to mimic human behavior
                        time.sleep(random.uniform(3, 6))

                        # Wait for page to load
                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

                        # Additional wait for dynamic content
                        time.sleep(random.uniform(3, 6))

                        # Try to trigger any lazy-loaded content by scrolling
                        try:
                            # Scroll to load more content
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/4);")
                            time.sleep(2)
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                            time.sleep(2)
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(3)
                            driver.execute_script("window.scrollTo(0, 0);")
                            time.sleep(2)
                        except:
                            pass

                        # Wait for any AJAX requests to complete
                        try:
                            WebDriverWait(driver, 10).until(
                                lambda driver: (
                                    driver.execute_script("return jQuery.active == 0")
                                    if driver.execute_script("return typeof jQuery !== 'undefined'")
                                    else True
                                )
                            )
                        except:
                            pass

                        # Additional wait for price data to load
                        time.sleep(random.uniform(2, 4))

                        # Check if we got blocked
                        page_text = driver.page_source.lower()
                        if "access denied" in page_text or "forbidden" in page_text:
                            logger.debug("Got access denied, trying refresh")
                            time.sleep(random.uniform(2, 4))
                            driver.refresh()
                            time.sleep(random.uniform(3, 5))

                        # Parse the content
                        page_recommendations = self._parse_html_content(driver.page_source)

                        if page_recommendations:
                            logger.info(f"Successfully scraped {len(page_recommendations)} recommendations from {url}")
                            recommendations.extend(page_recommendations)
                            # If we found good data, we can break or continue to get more
                            if len(recommendations) >= 10:  # Got enough data
                                break
                        else:
                            logger.debug(f"No recommendations found from {url}")

                    except Exception as e:
                        logger.debug(f"Error with URL {url}: {e}")
                        continue

                if recommendations:
                    return recommendations
                else:
                    logger.warning("No recommendations found")

                return []

            except Exception as e:
                logger.debug(f"Selenium error: {e}")
                return []

            finally:
                if driver:
                    driver.quit()

        except Exception as e:
            logger.debug(f"Selenium setup failed: {e}")
            return []

    def _parse_html_content(self, html_content: str) -> List[BrokerRecommendation]:
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html_content, "html.parser")
            recommendations = []

            logger.debug(f"Parsing HTML content of length: {len(html_content)}")

            # Look for text with recommendation keywords
            recommendation_texts = soup.find_all(
                string=lambda text: text
                and any(word in text.upper() for word in ["BUY", "SELL", "HOLD", "TARGET", "RECOMMENDATION"])
            )
            logger.debug(f"Found {len(recommendation_texts)} text elements with recommendation keywords")

            # Look for table rows or list items with stock data
            table_rows = soup.find_all(
                ["tr", "li", "div"], class_=re.compile(r"stock|recommendation|research|idea|row|item", re.I)
            )
            logger.debug(f"Found {len(table_rows)} potential stock data containers")

            # Look for links containing company information
            stock_links = soup.find_all("a", href=re.compile(r"/stocks?/|/stockpricequote/|/company/"))
            logger.debug(f"Found {len(stock_links)} stock-related links")

            # Parse any JSON data embedded in the page
            scripts = soup.find_all("script", string=re.compile(r"stocks?|recommendation|research"))
            logger.debug(f"Found {len(scripts)} scripts with stock-related data")

            # Look for embedded JSON with price data
            json_scripts = soup.find_all("script", string=re.compile(r"\{.*price.*\}|\[.*price.*\]", re.I))
            for script in json_scripts:
                try:
                    script_content = script.string
                    if script_content and ("price" in script_content.lower() or "target" in script_content.lower()):
                        # Extract JSON objects from script
                        json_matches = re.findall(r'\{[^{}]*(?:"price"|"target")[^{}]*\}', script_content, re.I)
                        for json_str in json_matches:
                            try:
                                data = json.loads(json_str)
                                recommendations.extend(self._extract_from_json_data(data))
                            except:
                                continue
                except Exception as e:
                    logger.debug(f"Error parsing JSON script: {e}")
                    continue

            # Process found elements
            processed_recommendations = set()

            # Parse table rows/containers
            for container in table_rows:
                try:
                    container_text = container.get_text()
                    if any(word in container_text.upper() for word in ["BUY", "SELL", "HOLD"]):
                        rec = self._extract_recommendation_from_container(container)
                        if rec and rec.company_name:
                            key = (rec.company_name.lower(), rec.broker_name.lower())
                            if key not in processed_recommendations:
                                recommendations.append(rec)
                                processed_recommendations.add(key)
                                logger.debug(f"Extracted: {rec.company_name} - {rec.recommendation}")
                except Exception as e:
                    logger.debug(f"Error parsing container: {e}")
                    continue

            # Parse stock links for additional data
            for link in stock_links:
                try:
                    link_text = link.get_text(strip=True)
                    if len(link_text) > 2 and self._is_valid_company_name(link_text):
                        # Look for recommendation context around this link
                        parent = link.parent
                        for _ in range(3):  # Go up 3 levels to find context
                            if parent and parent.parent:
                                parent = parent.parent

                        if parent:
                            context_text = parent.get_text()
                            if any(word in context_text.upper() for word in ["BUY", "SELL", "HOLD"]):
                                rec = self._extract_recommendation_from_context(link_text, context_text)
                                if rec:
                                    key = (rec.company_name.lower(), rec.broker_name.lower())
                                    if key not in processed_recommendations:
                                        recommendations.append(rec)
                                        processed_recommendations.add(key)
                                        logger.debug(f"From link: {rec.company_name} - {rec.recommendation}")
                except Exception as e:
                    logger.debug(f"Error parsing link: {e}")
                    continue

            # If still no recommendations, try to extract from any text containing keywords
            if not recommendations:
                logger.debug("No structured data found, trying text extraction")
                for text_element in recommendation_texts[:20]:  # Limit to prevent spam
                    try:
                        rec = self._extract_from_text(str(text_element))
                        if rec:
                            key = (rec.company_name.lower(), rec.broker_name.lower())
                            if key not in processed_recommendations:
                                recommendations.append(rec)
                                processed_recommendations.add(key)
                    except Exception as e:
                        logger.debug(f"Error parsing text: {e}")
                        continue

            unique_recs = self._remove_duplicates(recommendations)
            logger.info(f"Final result: {len(unique_recs)} unique recommendations found")

            return unique_recs

        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return []

    def _extract_recommendation_from_container(self, container) -> BrokerRecommendation:
        """Extract recommendation from a container element"""
        try:
            text = container.get_text()

            # Extract company name
            company_name = self._extract_company_name(container)
            if not company_name or not self._is_valid_company_name(company_name):
                return None

            # Extract recommendation
            recommendation = self._extract_recommendation(text)
            if not recommendation:
                return None

            # Extract broker
            broker_name = self._extract_broker_name(text)
            if not broker_name:
                broker_name = "MoneyControl Research"

            # DEBUG: Log the container content for price extraction debugging
            logger.debug(f"Processing {company_name} from {broker_name}")
            logger.debug(f"Container text (first 200 chars): {text[:200]}...")

            # Price extraction from HTML elements
            current_price, target_price = self._extract_prices_from_html(container)
            logger.debug(f"HTML extraction: Current={current_price}, Target={target_price}")

            # Strategy: Look for MoneyControl specific price patterns in surrounding elements
            if current_price == 0.0 or target_price == 0.0:
                # Search in parent and sibling elements for price data
                search_elements = [container]
                if container.parent:
                    search_elements.append(container.parent)
                    search_elements.extend(container.parent.find_all(["div", "span", "td"], recursive=False))

                for elem in search_elements:
                    elem_text = elem.get_text()
                    extracted_current, extracted_target = self._extract_prices(elem_text)
                    if current_price == 0.0 and extracted_current > 0:
                        current_price = extracted_current
                        logger.debug(f"Found current price in surrounding element: Rs {current_price}")
                    if target_price == 0.0 and extracted_target > 0:
                        target_price = extracted_target
                        logger.debug(f"Found target price in surrounding element: Rs {target_price}")
                    if current_price > 0 and target_price > 0:
                        break

            # Fallback to text-based extraction if no prices found
            if current_price == 0.0 and target_price == 0.0:
                logger.debug("No prices in HTML, trying text extraction")
                current_price, target_price = self._extract_prices(text)
                if current_price > 0 or target_price > 0:
                    logger.debug(f"Text extraction: Current={current_price}, Target={target_price}")

            # If still no prices, try to fetch current price from MoneyControl API
            if current_price == 0.0 and company_name:
                logger.debug(f"No price found in HTML, trying API for {company_name}")
                current_price = self._fetch_current_price(company_name)
                if current_price > 0:
                    logger.info(f"API price fetch successful for {company_name}: Rs {current_price}")
                else:
                    logger.debug(f"API price fetch failed for {company_name}")

            # Debug logging for price extraction
            if current_price > 0 or target_price > 0:
                logger.debug(f"Price extraction for {company_name}: Current=Rs{current_price}, Target=Rs{target_price}")
            else:
                logger.warning(f"No prices found for {company_name}")

            return BrokerRecommendation(
                broker_name=broker_name,
                company_name=company_name,
                recommendation=recommendation,
                target_price=target_price,
                current_price=current_price,
                reporting_date=datetime.now(),
            )

        except Exception as e:
            logger.debug(f"Error extracting from container: {e}")
            return None

    def _extract_prices_from_html(self, container) -> tuple:
        """Extract prices from HTML elements and attributes"""
        current_price = 0.0
        target_price = 0.0

        try:
            # Look for elements with data attributes
            price_elements = container.find_all(attrs={"data-price": True})
            for elem in price_elements:
                price_val = elem.get("data-price")
                if price_val and price_val.replace(".", "").isdigit():
                    price = float(price_val)
                    if 10 <= price <= 50000:
                        if target_price == 0.0:
                            target_price = price
                        elif current_price == 0.0:
                            current_price = price

            # Check for price in title attributes
            price_titles = container.find_all(attrs={"title": re.compile(r"Rs\s*\d+", re.I)})
            for elem in price_titles:
                title_text = elem.get("title", "")
                price_match = re.search(r"Rs\s*(\d+(?:,\d{3})*(?:\.\d+)?)", title_text, re.I)
                if price_match:
                    price = float(price_match.group(1).replace(",", ""))
                    if 10 <= price <= 50000:
                        if "target" in title_text.lower():
                            target_price = price
                        elif "current" in title_text.lower() or "cmp" in title_text.lower():
                            current_price = price

            # Look for elements with class names containing 'price'
            price_classes = container.find_all(class_=re.compile(r"price|target|current|cmp", re.I))
            for elem in price_classes:
                text = elem.get_text(strip=True)
                # Extract numbers from price elements
                numbers = re.findall(r"(\d+(?:,\d{3})*(?:\.\d+)?)", text)
                for num_str in numbers:
                    try:
                        price = float(num_str.replace(",", ""))
                        if 10 <= price <= 50000:
                            elem_classes = " ".join(elem.get("class", [])).lower()
                            if "target" in elem_classes or "target" in text.lower():
                                if target_price == 0.0:
                                    target_price = price
                            elif "current" in elem_classes or "current" in text.lower() or "cmp" in elem_classes:
                                if current_price == 0.0:
                                    current_price = price
                            else:
                                if target_price == 0.0:
                                    target_price = price
                                elif current_price == 0.0:
                                    current_price = price
                    except ValueError:
                        continue

            # Check for price table cells
            price_cells = container.find_all(["td", "th"], string=re.compile(r"\d+\.?\d*"))
            for cell in price_cells:
                text = cell.get_text(strip=True)
                if re.match(r"^\d+(?:,\d{3})*(?:\.\d+)?$", text):
                    try:
                        price = float(text.replace(",", ""))
                        if 10 <= price <= 50000:
                            # Check neighboring cells for context
                            siblings = cell.find_next_siblings() + cell.find_previous_siblings()
                            context_text = " ".join([s.get_text().lower() for s in siblings[:3]])

                            if "target" in context_text or "tp" in context_text:
                                if target_price == 0.0:
                                    target_price = price
                            elif "current" in context_text or "cmp" in context_text or "price" in context_text:
                                if current_price == 0.0:
                                    current_price = price
                            else:
                                # Default assignment based on order
                                if target_price == 0.0:
                                    target_price = price
                                elif current_price == 0.0:
                                    current_price = price
                    except ValueError:
                        continue

            # Look for span or div elements containing numerical data
            numeric_elements = container.find_all(["span", "div", "strong", "b"], string=re.compile(r"\d+"))
            for elem in numeric_elements:
                text = elem.get_text(strip=True)
                # Check if this looks like a price
                if re.match(r"^\d+(?:,\d{3})*(?:\.\d+)?$", text):
                    try:
                        price = float(text.replace(",", ""))
                        if 10 <= price <= 50000:
                            # Use context clues to determine if it's current or target
                            elem_text = str(elem).lower()
                            parent_text = str(elem.parent).lower() if elem.parent else ""
                            context = elem_text + " " + parent_text

                            if "target" in context or "tp" in context:
                                if target_price == 0.0:
                                    target_price = price
                            elif "current" in context or "cmp" in context or "price" in context:
                                if current_price == 0.0:
                                    current_price = price
                            else:
                                # Default assignment
                                if target_price == 0.0:
                                    target_price = price
                                elif current_price == 0.0:
                                    current_price = price
                    except ValueError:
                        continue

            # Look for hidden input fields with price data
            hidden_inputs = container.find_all("input", type="hidden")
            for input_elem in hidden_inputs:
                name = input_elem.get("name", "").lower()
                value = input_elem.get("value", "")
                if ("price" in name or "target" in name) and value.replace(".", "").replace(",", "").isdigit():
                    try:
                        price = float(value.replace(",", ""))
                        if 10 <= price <= 50000:
                            if "target" in name:
                                if target_price == 0.0:
                                    target_price = price
                            elif "current" in name or "cmp" in name:
                                if current_price == 0.0:
                                    current_price = price
                    except ValueError:
                        continue

        except Exception as e:
            logger.debug(f"Error extracting prices from HTML: {e}")

        return current_price, target_price

    def _extract_recommendation_from_context(self, company_name: str, context_text: str) -> BrokerRecommendation:
        """Extract recommendation from text context around a company name"""
        try:
            if not self._is_valid_company_name(company_name):
                return None

            recommendation = self._extract_recommendation(context_text)
            if not recommendation:
                return None

            broker_name = self._extract_broker_name(context_text)
            if not broker_name:
                broker_name = "MoneyControl Research"

            current_price, target_price = self._extract_prices(context_text)

            return BrokerRecommendation(
                broker_name=broker_name,
                company_name=self._clean_company_name(company_name),
                recommendation=recommendation,
                target_price=target_price,
                current_price=current_price,
                reporting_date=datetime.now(),
            )

        except Exception as e:
            logger.debug(f"Error extracting from context: {e}")
            return None

    def _extract_from_text(self, text: str) -> BrokerRecommendation:
        """Extract recommendation from plain text"""
        try:
            # Look for patterns like "Company Name: BUY" or "BUY Company Name"
            patterns = [
                r"([A-Za-z\s&]+?):\s*(BUY|SELL|HOLD)",
                r"(BUY|SELL|HOLD)\s+([A-Za-z\s&]+)",
                r"([A-Za-z\s&]+?)\s+-\s*(BUY|SELL|HOLD)",
            ]

            for pattern in patterns:
                matches = re.findall(pattern, text, re.I)
                for match in matches:
                    if len(match) == 2:
                        if match[1].upper() in ["BUY", "SELL", "HOLD"]:
                            company_name = self._clean_company_name(match[0])
                            recommendation = match[1].upper()
                        else:
                            company_name = self._clean_company_name(match[1])
                            recommendation = match[0].upper()

                        if self._is_valid_company_name(company_name):
                            return BrokerRecommendation(
                                broker_name="MoneyControl Research",
                                company_name=company_name,
                                recommendation=recommendation,
                                target_price=0.0,
                                current_price=0.0,
                                reporting_date=datetime.now(),
                            )

            return None

        except Exception as e:
            logger.debug(f"Error extracting from text: {e}")
            return None

    def _extract_company_name(self, context_element) -> str:
        if not context_element:
            return ""

        # Look for links with stockpricequote (most reliable)
        links = context_element.find_all("a")
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if "/stockpricequote/" in href and len(text) > 2:
                cleaned = self._clean_company_name(text)
                if self._is_valid_company_name(cleaned):
                    return cleaned

        # Look for any stock-related links
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if ("/stocks/" in href or "/company/" in href) and len(text) > 2:
                cleaned = self._clean_company_name(text)
                if self._is_valid_company_name(cleaned):
                    return cleaned

        # Look for headings and bold text
        bold_elements = context_element.find_all(["b", "strong", "h1", "h2", "h3", "h4", "h5", "h6"])
        for element in bold_elements:
            text = element.get_text(strip=True)
            cleaned_text = self._clean_company_name(text)
            if self._is_valid_company_name(cleaned_text):
                return cleaned_text

        # Look for known company patterns
        text_content = context_element.get_text()
        # Common Indian company patterns
        company_patterns = [
            r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Ltd|Limited|Industries|Bank|Corp|Corporation|Financial|Services|Motors|Steel|Power|Energy|Pharma|Technologies))\b",
            r"\b(Reliance|TCS|Infosys|HDFC|ICICI|SBI|Wipro|HCL|Bharti|Maruti|Asian Paints|ITC|Hindustan Unilever|Bajaj|Mahindra|Tata)\b",
        ]

        for pattern in company_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                cleaned = self._clean_company_name(match)
                if self._is_valid_company_name(cleaned):
                    return cleaned

        return ""

    def _is_valid_company_name(self, name: str) -> bool:
        """Check if the extracted text is likely a valid company name"""
        if not name or len(name) < 3 or len(name) > 50:
            return False

        # Filter out common UI/navigation elements
        invalid_names = [
            "all stats",
            "view all",
            "read more",
            "click here",
            "see more",
            "home",
            "about",
            "contact",
            "login",
            "register",
            "search",
            "menu",
            "portfolio",
            "watchlist",
            "news",
            "research",
            "analysis",
            "market",
            "stock",
            "share",
            "price",
            "chart",
            "data",
            "report",
        ]

        if name.lower() in invalid_names:
            return False

        # Must contain at least one letter
        if not re.search(r"[a-zA-Z]", name):
            return False

        # Should not be mostly numbers
        if len(re.findall(r"\d", name)) > len(name) / 2:
            return False

        # Should not contain common non-company words
        non_company_words = ["the", "and", "or", "for", "with", "from", "to", "by", "at", "on", "in"]
        words = name.lower().split()
        if len(words) > 0 and words[0] in non_company_words:
            return False

        return True

    def _clean_company_name(self, company_name: str) -> str:
        if not company_name:
            return ""

        # Remove recommendation prefixes
        company_name = re.sub(r"^(?:Buy|Sell|Hold)\s+", "", company_name, flags=re.I)

        # Remove target price suffixes
        company_name = re.sub(r";\s*target\s+of\s+Rs\s*\d+.*$", "", company_name, flags=re.I)
        company_name = re.sub(r":\s*[A-Za-z\s&\.]+$", "", company_name, flags=re.I)

        return re.sub(r"\s+", " ", company_name).strip()

    def _extract_broker_name(self, text: str) -> str:
        # Look for "Research by" pattern
        match = re.search(r"Research by\s+([A-Za-z\s&\.]+?)(?:\s|$|;|:)", text)
        if match:
            return match.group(1).strip()

        # Look for broker name after colon
        match = re.search(r":\s*([A-Za-z\s&\.]+?)(?:\s*$)", text)
        if match:
            potential_broker = match.group(1).strip()
            if self._is_likely_broker_name(potential_broker):
                return potential_broker

        # Known Indian brokers
        known_brokers = [
            "Motilal Oswal",
            "Prabhudas Lilladher",
            "Anand Rathi",
            "HDFC Securities",
            "ICICI Direct",
            "Sharekhan",
            "Kotak Securities",
            "Axis Securities",
            "Edelweiss",
            "YES Securities",
            "IIFL Securities",
            "Emkay Global",
            "Angel Broking",
            "Zerodha",
            "5paisa",
            "Upstox",
            "Religare Securities",
            "India Infoline",
            "SMC Global",
            "Geojit",
            "JM Financial",
            "LKP Securities",
            "Nirmal Bang",
            "Centrum Broking",
            "SBI Securities",
            "BOI AXA Investment",
            "IDBI Capital",
            "Ventura Securities",
            "Choice Broking",
            "Master Capital",
            "MoneyControl Research",
            "Equitymaster",
            "Dalal Street Investment",
        ]

        for broker in known_brokers:
            if broker.lower() in text.lower():
                return broker

        # Look for patterns like "XYZ Securities", "ABC Capital", etc.
        broker_patterns = [
            r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Securities|Capital|Broking|Investment|Financial|Research)\b",
            r"\b([A-Z][a-z]+)\s+(?:Securities|Capital|Broking|Investment|Financial|Research)\b",
            r"\b([A-Z]{2,5})\s+(?:Securities|Capital|Broking|Investment|Financial|Research)\b",
        ]

        for pattern in broker_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if len(match) > 2 and not match.lower() in ["the", "and", "ltd", "limited"]:
                    return f"{match} Securities"

        return ""

    def _is_likely_broker_name(self, name: str) -> bool:
        if len(name) < 3 or len(name) > 30:
            return False

        broker_patterns = [r"\b(Securities|Capital|Wealth|Financial|Research|Advisors)\b", r"\b(Ltd|Limited|Pvt|Private)\b"]

        for pattern in broker_patterns:
            if re.search(pattern, name, re.I):
                return True

        return False

    def _parse_nextjs_data(self, page_props: dict) -> List[BrokerRecommendation]:
        recommendations = []

        # Check multiple possible data sources
        data_keys = ["stocks", "research", "recommendations", "items", "data", "results"]
        for key in data_keys:
            if key in page_props:
                value = page_props[key]
                if isinstance(value, list) and len(value) > 0:
                    recommendations.extend(self._parse_api_response({"data": value}))
                    break

        return recommendations

    def _parse_table_row(self, cells) -> BrokerRecommendation:
        try:
            cell_texts = [cell.get_text().strip() for cell in cells]

            company_name = ""
            broker_name = ""
            recommendation = ""
            current_price = 0.0
            target_price = 0.0

            # Find recommendation cell
            recommendation_cell_idx = -1
            for i, text in enumerate(cell_texts):
                if re.search(r"\b(BUY|SELL|HOLD|ACCUMULATE|REDUCE)\b", text, re.I):
                    recommendation = self._extract_recommendation(text)
                    recommendation_cell_idx = i
                    break

            if recommendation_cell_idx >= 0:
                # Look for company name
                for i in range(recommendation_cell_idx):
                    text = cell_texts[i]
                    if len(text) > 2 and not re.search(r"\d{4}", text):
                        cleaned_name = self._clean_company_name(text)
                        if len(cleaned_name) > 2:
                            company_name = cleaned_name
                            break

                # Look for broker name
                broker_text = " ".join(cell_texts[recommendation_cell_idx:])
                extracted_broker = self._extract_broker_name(broker_text)
                if extracted_broker:
                    broker_name = extracted_broker

            # Extract prices
            all_text = " ".join(cell_texts)
            current_price, target_price = self._extract_prices(all_text)

            if company_name and broker_name and recommendation:
                return BrokerRecommendation(
                    broker_name=broker_name,
                    company_name=company_name,
                    recommendation=recommendation,
                    target_price=target_price,
                    current_price=current_price,
                    reporting_date=datetime.now(),
                )

        except Exception:
            pass

        return None

    def _extract_prices(self, text: str) -> tuple:
        current_price = 0.0
        target_price = 0.0

        logger.debug(f"Price extraction from text: {text[:100]}...")

        # Target price patterns
        target_rs_match = re.search(r"target\s+of\s+Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
        if target_rs_match:
            target_str = target_rs_match.group(1).replace(",", "")
            target_price = float(target_str)
            logger.debug(f"Found target via 'target of Rs' pattern: Rs {target_price}")

        if target_price == 0.0:
            target_match = re.search(r"Target(?:\s*Price)?[:\s]*Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
            if target_match:
                target_str = target_match.group(1).replace(",", "")
                target_price = float(target_str)
                logger.debug(f"Found target via 'Target:' pattern: Rs {target_price}")

        if target_price == 0.0:
            tp_match = re.search(r"(?:TP|PT)[:\s]*Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
            if tp_match:
                target_str = tp_match.group(1).replace(",", "")
                target_price = float(target_str)
                logger.debug(f"Found target via 'TP:' pattern: Rs {target_price}")

        if target_price == 0.0:
            pt_match = re.search(r"price\s+target\s+of\s+Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
            if pt_match:
                target_str = pt_match.group(1).replace(",", "")
                target_price = float(target_str)
                logger.debug(f"Found target via 'price target' pattern: Rs {target_price}")

        # Current price patterns
        reco_match = re.search(r"(?:Reco|Current|CMP)\s*Price[:\s]*Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
        if reco_match:
            current_str = reco_match.group(1).replace(",", "")
            current_price = float(current_str)
            logger.debug(f"Found current via 'Current Price' pattern: Rs {current_price}")

        if current_price == 0.0:
            current_match = re.search(r"(?:at|@)\s*Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
            if current_match:
                current_str = current_match.group(1).replace(",", "")
                current_price = float(current_str)
                logger.debug(f"Found current via 'at Rs' pattern: Rs {current_price}")

        if current_price == 0.0:
            cmp_match = re.search(r"CMP[:\s]*Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
            if cmp_match:
                current_str = cmp_match.group(1).replace(",", "")
                current_price = float(current_str)
                logger.debug(f"Found current via 'CMP:' pattern: Rs {current_price}")

        # Price ranges
        if current_price == 0.0 or target_price == 0.0:
            range_match = re.search(r"Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*-\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
            if range_match:
                price1 = float(range_match.group(1).replace(",", ""))
                price2 = float(range_match.group(2).replace(",", ""))
                if current_price == 0.0:
                    current_price = min(price1, price2)
                    logger.debug(f"Found current via range pattern: Rs {current_price}")
                if target_price == 0.0:
                    target_price = max(price1, price2)
                    logger.debug(f"Found target via range pattern: Rs {target_price}")

        # Extract numbers with Rs currency symbol
        if current_price == 0.0 or target_price == 0.0:
            # Look for numbers with Rs currency symbol
            price_patterns = re.findall(r"Rs\.?\s*(\d+(?:,\d{3})*(?:\.\d+)?)", text, re.I)
            if price_patterns:
                prices = []
                for p in price_patterns:
                    try:
                        price_val = float(p.replace(",", ""))
                        # Filter realistic stock prices (Rs 10 to Rs 50,000)
                        if 10 <= price_val <= 50000:
                            prices.append(price_val)
                    except ValueError:
                        continue

                if len(prices) >= 2:
                    prices.sort()
                    if current_price == 0.0:
                        current_price = prices[0]
                        logger.debug(f"Found current via multiple Rs pattern: Rs {current_price}")
                    if target_price == 0.0:
                        target_price = prices[-1]
                        logger.debug(f"Found target via multiple Rs pattern: Rs {target_price}")
                elif len(prices) == 1:
                    if target_price == 0.0:
                        target_price = prices[0]
                        logger.debug(f"Found target via single Rs pattern: Rs {target_price}")

        # Context-specific patterns
        if current_price == 0.0 or target_price == 0.0:
            # Look for patterns like "Buy at 1500, target 1800"
            buy_target_match = re.search(
                r"(?:buy|purchase)\s+(?:at\s+)?(\d+(?:,\d{3})*(?:\.\d+)?)\s*,?\s*(?:target|tp)\s+(\d+(?:,\d{3})*(?:\.\d+)?)",
                text,
                re.I,
            )
            if buy_target_match:
                if current_price == 0.0:
                    current_price = float(buy_target_match.group(1).replace(",", ""))
                    logger.debug(f"Found current via 'buy at X target Y' pattern: Rs {current_price}")
                if target_price == 0.0:
                    target_price = float(buy_target_match.group(2).replace(",", ""))
                    logger.debug(f"Found target via 'buy at X target Y' pattern: Rs {target_price}")

        # MoneyControl specific patterns
        if current_price == 0.0 or target_price == 0.0:
            # Pattern for "Stock Name: BUY (1234/1500)"
            bracket_pattern = re.search(r"\((\d+(?:,\d{3})*(?:\.\d+)?)[/\-](\d+(?:,\d{3})*(?:\.\d+)?)\)", text)
            if bracket_pattern:
                price1 = float(bracket_pattern.group(1).replace(",", ""))
                price2 = float(bracket_pattern.group(2).replace(",", ""))
                if 10 <= price1 <= 50000 and 10 <= price2 <= 50000:
                    if current_price == 0.0:
                        current_price = min(price1, price2)
                        logger.debug(f"Found current via bracket pattern: Rs {current_price}")
                    if target_price == 0.0:
                        target_price = max(price1, price2)
                        logger.debug(f"Found target via bracket pattern: Rs {target_price}")

        # MoneyControl table patterns
        if current_price == 0.0 or target_price == 0.0:
            number_pairs = re.findall(r"(\d{3,5})\s+(\d{3,5})", text)
            for pair in number_pairs:
                try:
                    price1, price2 = float(pair[0]), float(pair[1])
                    if 50 <= price1 <= 50000 and 50 <= price2 <= 50000:
                        if abs(price2 - price1) / price1 < 0.5:  # Reasonable difference
                            if current_price == 0.0:
                                current_price = min(price1, price2)
                                logger.debug(f"Found current via number pair: Rs {current_price}")
                            if target_price == 0.0:
                                target_price = max(price1, price2)
                                logger.debug(f"Found target via number pair: Rs {target_price}")
                            break
                except:
                    continue

        # Standalone realistic numbers
        if current_price == 0.0 or target_price == 0.0:
            standalone_numbers = re.findall(r"\b(\d{3,5})\b", text)
            realistic_prices = []
            for num_str in standalone_numbers:
                try:
                    price = float(num_str)
                    if 100 <= price <= 20000:  # Realistic Indian stock price range
                        realistic_prices.append(price)
                except:
                    continue

            if len(realistic_prices) >= 2:
                realistic_prices.sort()
                if current_price == 0.0:
                    current_price = realistic_prices[0]
                    logger.debug(f"Found current via standalone numbers: Rs {current_price}")
                if target_price == 0.0:
                    target_price = realistic_prices[-1]
                    logger.debug(f"Found target via standalone numbers: Rs {target_price}")

        if current_price == 0.0 and target_price == 0.0:
            logger.debug("No price patterns matched in text")

        return current_price, target_price

    def _extract_recommendation(self, call_text: str) -> str:
        call_upper = str(call_text).upper()
        if "BUY" in call_upper or "BUYS" in call_upper:
            return "BUY"
        elif "SELL" in call_upper or "SELLS" in call_upper:
            return "SELL"
        elif "HOLD" in call_upper or "HOLDS" in call_upper:
            return "HOLD"
        return "BUY"

    def _parse_api_response(self, data: dict) -> List[BrokerRecommendation]:
        recommendations = []

        items = []
        if isinstance(data, dict):
            if "data" in data:
                items = data["data"]
            elif isinstance(data.get("items"), list):
                items = data["items"]
        elif isinstance(data, list):
            items = data

        for item in items:
            if not isinstance(item, dict):
                continue

            company_name = (item.get("company_name") or item.get("companyName") or "").strip()
            broker_name = (item.get("broker_name") or item.get("brokerName") or "").strip()
            recommendation = (item.get("recommendation") or item.get("call") or "").strip()
            target_price = float(item.get("target_price", 0) or 0)
            current_price = float(item.get("current_price", 0) or 0)

            if company_name and broker_name:
                rec = BrokerRecommendation(
                    broker_name=broker_name,
                    company_name=company_name,
                    recommendation=self._extract_recommendation(recommendation),
                    target_price=target_price,
                    current_price=current_price,
                    reporting_date=datetime.now(),
                )
                recommendations.append(rec)

        return recommendations

    def _remove_duplicates(self, recommendations: List[BrokerRecommendation]) -> List[BrokerRecommendation]:
        unique_recommendations = []
        seen_combinations = set()

        for rec in recommendations:
            key = (rec.company_name.lower(), rec.broker_name.lower())
            if key not in seen_combinations:
                unique_recommendations.append(rec)
                seen_combinations.add(key)

        return unique_recommendations

    def _fetch_current_price(self, company_name: str) -> float:
        """Price fetching from MoneyControl with multiple strategies"""
        try:
            # Direct MoneyControl search API
            price = self._fetch_price_from_search_api(company_name)
            if price > 0:
                return price

            # MoneyControl stock quote API
            price = self._fetch_price_from_quote_api(company_name)
            if price > 0:
                return price

            # Web scraping approach
            price = self._fetch_price_from_web_scraping(company_name)
            if price > 0:
                return price

        except Exception as e:
            logger.debug(f"Error fetching current price for {company_name}: {e}")

        return 0.0

    def _fetch_price_from_search_api(self, company_name: str) -> float:
        """Price fetching using MoneyControl search API"""
        try:
            # Try multiple search variations
            search_variations = [
                company_name,
                company_name.replace(" ", ""),
                company_name.replace("Limited", "Ltd"),
                company_name.replace("Ltd", ""),
                company_name.split()[0] if " " in company_name else company_name,
                company_name.replace("Bank", "").strip(),
                company_name.replace("Finance", "").strip(),
            ]

            for search_term in search_variations:
                if not search_term:
                    continue

                search_name = search_term.replace(" ", "%20").replace("&", "%26")
                search_url = (
                    f"https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php?query={search_name}&type=1&format=json"
                )

                try:
                    response = self.session.get(search_url, timeout=8)
                    if response.status_code == 200:
                        data = response.json()
                        if data and len(data) > 0:
                            for item in data[:5]:  # Check more results
                                if "pdt_dis_nm" in item and "sc_id" in item:
                                    # Check if this is a good match
                                    item_name = item["pdt_dis_nm"].lower()
                                    search_lower = search_term.lower()

                                    # Simple similarity check
                                    if (
                                        search_lower in item_name
                                        or item_name in search_lower
                                        or any(word in item_name for word in search_lower.split() if len(word) > 2)
                                    ):

                                        stock_id = item["sc_id"]

                                        # Try multiple price APIs
                                        price_urls = [
                                            f"https://www.moneycontrol.com/mccode/common/getlivejson.php?sc_id={stock_id}",
                                            f"https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol={stock_id}&resolution=1D&from=1640995200&to=1672531200&countback=1",
                                        ]

                                        for price_url in price_urls:
                                            try:
                                                stock_response = self.session.get(price_url, timeout=5)
                                                if stock_response.status_code == 200:
                                                    stock_data = stock_response.json()

                                                    # Try different price field names
                                                    price_fields = ["lastPrice", "ltp", "price", "close", "c"]
                                                    for field in price_fields:
                                                        if field in stock_data:
                                                            price = float(stock_data[field])
                                                            if 10 <= price <= 50000:
                                                                logger.debug(
                                                                    f"Found price via search API for {company_name}: Rs {price}"
                                                                )
                                                                return price
                                            except:
                                                continue
                except Exception as e:
                    logger.debug(f"Search API error for {search_term}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Search API failed for {company_name}: {e}")
        return 0.0

    def _fetch_price_from_quote_api(self, company_name: str) -> float:
        """Fetch price using alternative MoneyControl APIs"""
        try:
            # Try different API endpoints
            search_terms = [
                company_name,
                company_name.replace(" ", ""),
                company_name.split()[0] if " " in company_name else company_name,
            ]

            for term in search_terms:
                # Try quote API
                quote_url = f"https://priceapi.moneycontrol.com/pricefeed/notapplicable/inr/{term.lower()}"
                try:
                    response = self.session.get(quote_url, timeout=5)
                    if response.status_code == 200:
                        data = response.json()
                        if "pricecurrent" in data:
                            price = float(data["pricecurrent"])
                            if 10 <= price <= 50000:
                                logger.debug(f"Fetched price via quote API for {company_name}: Rs {price}")
                                return price
                except:
                    continue

        except Exception as e:
            logger.debug(f"Quote API failed for {company_name}: {e}")
        return 0.0

    def _fetch_price_from_web_scraping(self, company_name: str) -> float:
        """Fetch price by scraping company page directly"""
        try:
            # Try to find the company's dedicated page
            search_name = company_name.replace(" ", "-").lower()
            possible_urls = [
                f"https://www.moneycontrol.com/india/stockpricequote/{search_name}",
                f"https://www.moneycontrol.com/stocks/marketstats/{search_name}",
                f"https://www.moneycontrol.com/stocksmarketsindia/{search_name}",
            ]

            for url in possible_urls:
                try:
                    response = self.session.get(url, timeout=5)
                    if response.status_code == 200:
                        from bs4 import BeautifulSoup

                        soup = BeautifulSoup(response.text, "html.parser")

                        # Look for price elements
                        price_selectors = [
                            ".pcnspa",
                            ".span_price_wrap",
                            ".price_current",
                            '[id*="Nse_Prc_tick"]',
                            '[class*="price"]',
                        ]

                        for selector in price_selectors:
                            price_elem = soup.select_one(selector)
                            if price_elem:
                                price_text = price_elem.get_text(strip=True)
                                price_match = re.search(r"(\d+(?:,\d{3})*(?:\.\d+)?)", price_text)
                                if price_match:
                                    price = float(price_match.group(1).replace(",", ""))
                                    if 10 <= price <= 50000:
                                        logger.debug(f"Fetched price via web scraping for {company_name}: Rs {price}")
                                        return price
                except:
                    continue

        except Exception as e:
            logger.debug(f"Web scraping failed for {company_name}: {e}")
        return 0.0

    def _extract_from_json_data(self, data: dict) -> List[BrokerRecommendation]:
        """Extract recommendations from JSON data found in scripts"""
        recommendations = []
        try:
            # Handle different JSON structures
            if isinstance(data, dict):
                # Look for price information
                current_price = 0.0
                target_price = 0.0
                company_name = ""

                # Extract company name
                name_keys = ["name", "company", "symbol", "companyName", "stockName"]
                for key in name_keys:
                    if key in data and data[key]:
                        company_name = str(data[key]).strip()
                        break

                # Extract prices
                price_keys = ["price", "currentPrice", "lastPrice", "ltp"]
                for key in price_keys:
                    if key in data and data[key]:
                        try:
                            current_price = float(data[key])
                            break
                        except:
                            continue

                target_keys = ["targetPrice", "target", "priceTarget", "tp"]
                for key in target_keys:
                    if key in data and data[key]:
                        try:
                            target_price = float(data[key])
                            break
                        except:
                            continue

                # Extract recommendation
                recommendation = "BUY"
                rec_keys = ["recommendation", "call", "action"]
                for key in rec_keys:
                    if key in data and data[key]:
                        recommendation = self._extract_recommendation(str(data[key]))
                        break

                if company_name and self._is_valid_company_name(company_name):
                    rec = BrokerRecommendation(
                        broker_name="MoneyControl Research",
                        company_name=company_name,
                        recommendation=recommendation,
                        target_price=target_price,
                        current_price=current_price,
                        reporting_date=datetime.now(),
                    )
                    recommendations.append(rec)

        except Exception as e:
            logger.debug(f"Error extracting from JSON data: {e}")

        return recommendations
