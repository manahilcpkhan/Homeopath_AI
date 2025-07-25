#!/usr/bin/env python3
"""
Enhanced Homeopathic Repertory Scraper with IP Rotation
Improved version that captures bold symptoms more accurately and includes IP rotation
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
from urllib.parse import urljoin
import random
from itertools import cycle
import logging
from typing import Dict, List, Optional, Tuple
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lxml.html import fromstring

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('homeopathic_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProxyRotator:
    """Handles dynamic proxy rotation by fetching free proxies"""
    
    def __init__(self, proxy_list: List[str] = None):
        # You can provide your own proxy list or use dynamic fetching
        self.custom_proxies = proxy_list or []
        self.proxies = set()
        self.proxy_cycle = None
        self.last_proxy_fetch = 0
        self.proxy_refresh_interval = 300  # Refresh proxies every 5 minutes
        
        # List of user agents to rotate
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
        ]
        
        # IP check URL
        self.ip_check_url = 'https://api64.ipify.org'
        
        # Initialize proxies
        self.refresh_proxies()
        
    def fetch_free_proxies(self) -> set:
        """Fetch free proxies from free-proxy-list.net"""
        try:
            logger.info("Fetching fresh proxy list...")
            response = requests.get('https://free-proxy-list.net/', timeout=10)
            parser = fromstring(response.text)
            proxies = set()
            
            for i in parser.xpath('//tbody/tr'):
                if i.xpath('.//td[7][contains(text(),"yes")]'):  # HTTPS support
                    try:
                        ip = i.xpath('.//td[1]/text()')[0]
                        port = i.xpath('.//td[2]/text()')[0]
                        proxy = f"{ip}:{port}"
                        proxies.add(proxy)
                    except (IndexError, AttributeError):
                        continue
                        
            logger.info(f"Fetched {len(proxies)} proxies")
            return proxies
            
        except Exception as e:
            logger.error(f"Failed to fetch proxies: {e}")
            return set()
    
    def test_proxy(self, proxy: str) -> bool:
        """Test if a proxy is working"""
        try:
            response = requests.get(
                self.ip_check_url, 
                proxies={"http": proxy, "https": proxy}, 
                timeout=10
            )
            if response.ok:
                logger.info(f'Proxy {proxy} is working')
                return True
        except Exception as e:
            logger.debug(f'Proxy {proxy} failed: {e}')
        return False
    
    def refresh_proxies(self):
        """Refresh the proxy list"""
        current_time = time.time()
        
        # Check if we need to refresh
        if current_time - self.last_proxy_fetch < self.proxy_refresh_interval and self.proxies:
            return
            
        # Use custom proxies if provided
        if self.custom_proxies:
            self.proxies = set(self.custom_proxies)
            logger.info(f"Using {len(self.proxies)} custom proxies")
        else:
            # Fetch new proxies
            self.proxies = self.fetch_free_proxies()
            
        if self.proxies:
            self.proxy_cycle = cycle(self.proxies)
            self.last_proxy_fetch = current_time
            logger.info(f"Proxy pool refreshed with {len(self.proxies)} proxies")
        else:
            logger.warning("No proxies available")
    
    def get_working_proxy(self) -> Optional[str]:
        """Get a working proxy from the pool"""
        if not self.proxies:
            self.refresh_proxies()
            
        if not self.proxy_cycle:
            return None
            
        # Try up to 5 proxies to find a working one
        for _ in range(min(5, len(self.proxies))):
            proxy = next(self.proxy_cycle)
            if self.test_proxy(proxy):
                return proxy
                
        return None
        
    def get_session(self) -> requests.Session:
        """Create a new session with proxy and user agent rotation"""
        session = requests.Session()
        
        # Set up retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Rotate user agent
        session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
        
        # Set proxy if available
        if self.proxies:
            working_proxy = self.get_working_proxy()
            if working_proxy:
                session.proxies = {
                    'http': working_proxy,
                    'https': working_proxy
                }
                logger.info(f"Using proxy: {working_proxy}")
            else:
                logger.warning("No working proxy found, using direct connection")
        
        return session

class EnhancedHomeopathicScraper:
    """Enhanced scraper with better symptom detection and IP rotation"""
    
    def __init__(self, base_url="http://homeoint.org/books/kentrep/", use_proxies=False, proxy_list=None):
        self.base_url = base_url
        self.proxy_rotator = ProxyRotator(proxy_list) if use_proxies else None
        self.session = self.proxy_rotator.get_session() if use_proxies else requests.Session()
        
        # Set default headers if not using proxy rotation
        if not use_proxies:
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
        
        # Request counter for session rotation
        self.request_count = 0
        self.max_requests_per_session = 5  # Lower threshold for more frequent rotation
        self.failed_requests = 0
        self.max_failed_requests = 3
        
    def rotate_session(self):
        """Rotate session after certain number of requests or failures"""
        if self.proxy_rotator:
            # Refresh proxies if we've had too many failures
            if self.failed_requests >= self.max_failed_requests:
                self.proxy_rotator.refresh_proxies()
                self.failed_requests = 0
                logger.info("Refreshed proxy pool due to failures")
            
            self.session = self.proxy_rotator.get_session()
            self.request_count = 0
            logger.info("Session rotated with new proxy")
    
    def generate_links(self, start: int, end: int, step: int = 5) -> List[str]:
        """Generate symptom links with specific numbering pattern"""
        links = []
        for num in range(start, end + 1, step):
            page_num = str(num).zfill(3)  # Ensure three-digit format
            anchor_num = num + 1  # Adjust for anchor pattern
            link = f"{self.base_url}kent0{page_num}.htm#P{anchor_num}"
            links.append(link)
        return links
    
    def is_remedy_name(self, text: str) -> bool:
        """Enhanced logic to identify remedy names"""
        text = text.strip()
        
        # Check if it ends with a period (common for remedies)
        if text.endswith('.'):
            return True
        
        # Check if it matches common remedy patterns
        remedy_patterns = [
            r'^[A-Z][a-z]+-[a-z]+$',  # Pattern like Nat-m, Kali-c
            r'^[A-Z][a-z]+$',         # Pattern like Acon, Bell
            r'^[A-Z][a-z]+-[a-z]+-[a-z]+$',  # Pattern like Ant-s-aur
            r'^[A-Z][a-z]+\s+[a-z]+$',  # Pattern like Calc carb
        ]
        
        for pattern in remedy_patterns:
            if re.match(pattern, text):
                return True
        
        # Check if it's a short capitalized word (likely remedy abbreviation)
        if len(text) <= 8 and text[0].isupper() and '-' in text:
            return True
        
        return False
    
    def extract_all_bold_text(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        """Extract all bold text with their context"""
        bold_elements = []
        
        # Find all bold elements: <b>, <strong>, and elements with bold styling
        bold_tags = soup.find_all(['b', 'strong'])
        
        # Also find elements with bold styling
        styled_elements = soup.find_all(attrs={'style': re.compile(r'font-weight:\s*bold', re.I)})
        bold_tags.extend(styled_elements)
        
        # Find elements with bold classes (if any)
        bold_class_elements = soup.find_all(attrs={'class': re.compile(r'bold', re.I)})
        bold_tags.extend(bold_class_elements)
        
        for tag in bold_tags:
            text = tag.get_text(strip=True)
            if text:  # Only include non-empty text
                # Get parent context for better classification
                parent_text = tag.parent.get_text(strip=True) if tag.parent else ""
                bold_elements.append((text, parent_text))
        
        return bold_elements
    
    def extract_symptoms_medicines(self, url: str) -> Dict[str, List[str]]:
        """Extract symptoms and medicines from a page with enhanced detection"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Rotate session if needed
                self.request_count += 1
                if self.request_count > self.max_requests_per_session:
                    self.rotate_session()
                
                # Add random delay to avoid rate limiting
                time.sleep(random.uniform(2.0, 5.0))
                
                logger.info(f"Scraping: {url} (attempt {retry_count + 1})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Reset failed requests counter on success
                self.failed_requests = 0
                
                soup = BeautifulSoup(response.text, "html.parser")
                symptoms_data = {}
                
                # Extract all bold text
                bold_elements = self.extract_all_bold_text(soup)
                
                current_symptom = None
                medicines = []
                
                logger.info(f"Found {len(bold_elements)} bold elements")
                
                for text, context in bold_elements:
                    # Clean up text
                    text = re.sub(r'\s+', ' ', text).strip()
                    
                    if not text:
                        continue
                    
                    # Check if this is a remedy
                    if self.is_remedy_name(text):
                        # Remove trailing period if present
                        remedy = text.rstrip('.').strip()
                        if remedy:
                            medicines.append(remedy)
                            logger.debug(f"Found remedy: {remedy}")
                    else:
                        # This is likely a symptom
                        # Save previous symptom if we have one
                        if current_symptom and medicines:
                            symptoms_data[current_symptom] = medicines.copy()
                            logger.info(f"Saved symptom: {current_symptom} -> {len(medicines)} medicines")
                        
                        # Start new symptom
                        current_symptom = text
                        medicines = []
                        logger.debug(f"Found symptom: {current_symptom}")
                
                # Don't forget the last symptom
                if current_symptom and medicines:
                    symptoms_data[current_symptom] = medicines.copy()
                    logger.info(f"Saved final symptom: {current_symptom} -> {len(medicines)} medicines")
                
                return symptoms_data
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                self.failed_requests += 1
                logger.error(f"Request failed for {url} (attempt {retry_count}): {e}")
                
                if retry_count < max_retries:
                    logger.info(f"Retrying in {retry_count * 2} seconds...")
                    time.sleep(retry_count * 2)  # Exponential backoff
                    self.rotate_session()  # Try with new session/proxy
                else:
                    logger.error(f"Max retries reached for {url}")
                    return {}
                    
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                return {}
        
        return {}
    
    def scrape_mind_module(self, output_file: str = "mind_module_enhanced.json") -> Dict:
        """Scrape the Mind module with enhanced detection"""
        mind_data = {"Mind": {}}
        
        # Generate links for Mind module
        symptom_links = self.generate_links(105, 230, 5)
        
        logger.info(f"Starting to scrape {len(symptom_links)} pages")
        
        for i, link in enumerate(symptom_links, 1):
            logger.info(f"Progress: {i}/{len(symptom_links)} pages")
            
            try:
                extracted_data = self.extract_symptoms_medicines(link)
                if extracted_data:
                    mind_data["Mind"].update(extracted_data)
                    logger.info(f"Added {len(extracted_data)} symptoms from page {i}")
                else:
                    logger.warning(f"No data extracted from page {i}")
                    
            except Exception as e:
                logger.error(f"Failed to process page {i} ({link}): {e}")
                continue
        
        # Save to JSON file
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(mind_data, f, indent=4, ensure_ascii=False)
            
            logger.info(f"Scraping completed. Data saved to {output_file}")
            logger.info(f"Total symptoms collected: {len(mind_data['Mind'])}")
            
            # Print summary
            total_remedies = sum(len(remedies) for remedies in mind_data["Mind"].values())
            logger.info(f"Total remedies collected: {total_remedies}")
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
        
        return mind_data
    
    def scrape_custom_range(self, start: int, end: int, step: int = 5, 
                           output_file: str = "custom_repertory.json") -> Dict:
        """Scrape a custom range of pages"""
        data = {"Custom Range": {}}
        
        symptom_links = self.generate_links(start, end, step)
        
        logger.info(f"Starting to scrape {len(symptom_links)} pages (range: {start}-{end})")
        
        for i, link in enumerate(symptom_links, 1):
            logger.info(f"Progress: {i}/{len(symptom_links)} pages")
            
            try:
                extracted_data = self.extract_symptoms_medicines(link)
                if extracted_data:
                    data["Custom Range"].update(extracted_data)
                    
            except Exception as e:
                logger.error(f"Failed to process page {i}: {e}")
                continue
        
        # Save to JSON file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Custom range scraping completed. Data saved to {output_file}")
        return data

def main():
    """Main function to run the scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Homeopathic Repertory Scraper")
    parser.add_argument("--use-proxies", action="store_true", help="Enable proxy rotation")
    parser.add_argument("--proxy-file", help="File containing proxy list (one per line)")
    parser.add_argument("--output", default="mind_module_enhanced.json", help="Output file name")
    parser.add_argument("--start", type=int, default=105, help="Start page number")
    parser.add_argument("--end", type=int, default=230, help="End page number")
    parser.add_argument("--step", type=int, default=5, help="Step size for page numbers")
    
    args = parser.parse_args()
    
    # Load proxy list if provided
    proxy_list = None
    if args.proxy_file:
        try:
            with open(args.proxy_file, 'r') as f:
                proxy_list = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(proxy_list)} proxies")
        except Exception as e:
            logger.error(f"Failed to load proxy file: {e}")
    
    # Create scraper instance
    scraper = EnhancedHomeopathicScraper(
        use_proxies=args.use_proxies,
        proxy_list=proxy_list
    )
    
    # Run scraper
    if args.start == 105 and args.end == 230:
        # Default Mind module scraping
        scraper.scrape_mind_module(args.output)
    else:
        # Custom range scraping
        scraper.scrape_custom_range(args.start, args.end, args.step, args.output)

if __name__ == "__main__":
    main()