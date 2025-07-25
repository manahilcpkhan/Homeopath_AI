import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random
from itertools import cycle
from lxml.html import fromstring
from urllib.parse import urljoin
import os
from typing import Dict, Set, List, Tuple


class KentRepertoryScraper:
    def __init__(self):
        self.base_url = "http://homeoint.org/books/kentrep3/"
        self.base_url_3 = "http://homeoint.org/books/kentrep3/"
        self.proxy_pool = None
        self.current_proxy = None
        self.session = requests.Session()
        self.all_data = {}
        
    def get_proxy_pool(self):
        """Fetch and setup proxy rotation pool"""
        try:
            print("🔄 Fetching proxy list...")
            response = requests.get('https://free-proxy-list.net/', timeout=10)
            parser = fromstring(response.text)
            proxies = set()
            
            for i in parser.xpath('//tbody/tr'):
                if i.xpath('.//td[7][contains(text(),"yes")]'):
                    try:
                        ip = i.xpath('.//td[1]/text()')[0]
                        port = i.xpath('.//td[2]/text()')[0]
                        proxy = f"{ip}:{port}"
                        proxies.add(proxy)
                    except IndexError:
                        continue
                        
            if proxies:
                self.proxy_pool = cycle(proxies)
                print(f"✅ Found {len(proxies)} proxies")
                return True
            else:
                print("⚠️ No proxies found, continuing without rotation")
                return False
                
        except Exception as e:
            print(f"❌ Failed to fetch proxies: {e}")
            return False
    
    def rotate_proxy(self):
        """Test and rotate to next working proxy"""
        if not self.proxy_pool:
            return False
            
        test_url = 'https://api64.ipify.org'
        max_attempts = 10
        
        for attempt in range(max_attempts):
            proxy = next(self.proxy_pool)
            try:
                proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
                response = requests.get(test_url, proxies=proxies, timeout=10)
                
                if response.ok:
                    self.current_proxy = proxies
                    print(f"✅ Rotated to proxy: {proxy}")
                    return True
                    
            except Exception as e:
                print(f"❌ Proxy {proxy} failed: {str(e)[:50]}...")
                continue
                
        print("⚠️ All proxies failed, continuing without proxy")
        self.current_proxy = None
        return False
    
    def make_request(self, url: str, max_retries: int = 3):
        """Make HTTP request with proxy rotation and retry logic"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for attempt in range(max_retries):
            try:
                # Add random delay
                time.sleep(random.uniform(1.0, 3.0))
                
                if self.current_proxy:
                    response = requests.get(url, headers=headers, proxies=self.current_proxy, timeout=30)
                else:
                    response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 429:  # Rate limited
                    print(f"⚠️ Rate limited, rotating proxy...")
                    self.rotate_proxy()
                    time.sleep(random.uniform(5.0, 10.0))
                    continue
                    
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                print(f"❌ Request failed (attempt {attempt + 1}): {str(e)[:100]}...")
                if attempt < max_retries - 1:
                    if self.proxy_pool:
                        self.rotate_proxy()
                    time.sleep(random.uniform(2.0, 5.0))
                else:
                    raise
        
        return None
    
    def generate_page_urls(self):
        """Generate all page URLs to scrape based on the correct pattern"""
        urls = []
        
        # Pattern: file numbers increment by 5, page numbers = file_number + 1
        # kent0000.htm#P1, kent0005.htm#P6, kent0010.htm#P11, etc.
        
        file_number = 1075
        page_number = 1076
        
        # Generate URLs until we reach the expected end
        # Based on the original code expecting ~1423 pages
        while page_number <= 1423:
            # Determine which base URL to use
            if file_number <= 1415:  # Most files are in kentrep folder
                url = f"{self.base_url}kent{file_number:04d}.htm#P{page_number}"
            else:  # Later files might be in kentrep3 folder
                url = f"{self.base_url_3}kent{file_number:04d}.htm#P{page_number}"
            
            urls.append(url)
            
            # Increment by 5 for both file and page numbers
            file_number += 5
            page_number += 5
        
        return urls
    
    def get_page_lines_and_body_parts(self, url: str) -> Tuple[List[str], Set[str]]:
        """Extract lines and body parts from a page"""
        try:
            response = self.make_request(url)
            if not response:
                return [], set()
                
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find body parts from nav links
            nav_links = soup.find_all("a", href=True)
            body_parts = set()
            for link in nav_links:
                if link.text and link.text.isupper() and len(link.text) > 2:
                    body_parts.add(link.text.strip())
            
            # Extract page text
            text = soup.get_text(separator='\n')
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            return lines, body_parts
            
        except Exception as e:
            print(f"❌ Error processing {url}: {e}")
            return [], set()
    
    def parse_lines(self, lines: List[str], body_parts: Set[str]) -> Dict:
        """Parse lines to extract medical data"""
        data = {}
        current_body_part = None
        current_symptom = None
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Detect body part using next-line logic
            if line in body_parts and (i + 1 < len(lines) and re.match(r'^p\.\s*\d+$', lines[i + 1].lower())):
                current_body_part = line
                current_symptom = None
                i += 2  # skip page number
                continue
            
            # Detect new symptom line
            match = re.match(r'^([A-Z][A-Z\s\-]+)(.*)$', line)
            if match:
                current_symptom = match.group(1).strip()
                remainder = match.group(2).strip()
            elif current_symptom:
                remainder = line
            else:
                i += 1
                continue
            
            # Parse sub-symptom and medicines
            if ':' in remainder:
                sub_part, meds_part = remainder.split(':', 1)
                sub_symptom = sub_part.strip(' ,.') or "general"
            else:
                sub_symptom = "general"
                meds_part = remainder.strip()
            
            # Extract medicines ending in .
            meds_raw = re.findall(r'\b[A-Za-z][A-Za-z-]{0,20}\.', meds_part)
            medicines = [m.rstrip('.').strip().capitalize() for m in meds_raw]
            
            # Store in data structure
            if current_body_part and current_symptom and medicines:
                data.setdefault(current_body_part, {})
                data[current_body_part].setdefault(current_symptom, {})
                data[current_body_part][current_symptom].setdefault(sub_symptom, [])
                data[current_body_part][current_symptom][sub_symptom].extend(medicines)
            
            i += 1
        
        # Clean duplicates
        for bp in data:
            for sym in data[bp]:
                for sub in data[bp][sym]:
                    data[bp][sym][sub] = sorted(set(data[bp][sym][sub]))
        
        return data
    
    def merge_data(self, new_data: Dict):
        """Merge new page data with existing data"""
        for body_part, symptoms in new_data.items():
            if body_part not in self.all_data:
                self.all_data[body_part] = {}
            
            for symptom, sub_symptoms in symptoms.items():
                if symptom not in self.all_data[body_part]:
                    self.all_data[body_part][symptom] = {}
                
                for sub_symptom, medicines in sub_symptoms.items():
                    if sub_symptom not in self.all_data[body_part][symptom]:
                        self.all_data[body_part][symptom][sub_symptom] = []
                    
                    # Merge and deduplicate medicines
                    existing = set(self.all_data[body_part][symptom][sub_symptom])
                    new_meds = set(medicines)
                    combined = sorted(existing | new_meds)
                    self.all_data[body_part][symptom][sub_symptom] = combined
    
    def save_progress(self, filename: str = "kent_progress.json"):
        """Save current progress to file"""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.all_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Progress saved to {filename}")
        except Exception as e:
            print(f"❌ Failed to save progress: {e}")
    
    def scrape_all_pages(self, start_page: int = 1, save_interval: int = 50):
        """Main method to scrape all pages"""
        print("🚀 Starting Kent Repertory full scrape...")
        
        # Setup proxy rotation
        if self.get_proxy_pool():
            self.rotate_proxy()
        
        # Generate all URLs
        urls = self.generate_page_urls()
        total_pages = len(urls)
        
        print(f"📄 Total pages to scrape: {total_pages}")
        print(f"🔄 Starting from page: {start_page}")
        
        # Start scraping from specified page
        for i, url in enumerate(urls[start_page-1:], start_page):
            try:
                print(f"\n📖 Scraping page {i}/{total_pages}: {url}")
                
                lines, body_parts = self.get_page_lines_and_body_parts(url)
                if lines:
                    page_data = self.parse_lines(lines, body_parts)
                    self.merge_data(page_data)
                    
                    if len(page_data) > 0:
                        print(f"✅ Page {i}: Found {len(page_data)} body parts")
                    else:
                        print(f"⚠️ Page {i}: No data extracted")
                else:
                    print(f"❌ Page {i}: Failed to get content")
                
                # Save progress periodically
                if i % save_interval == 0:
                    self.save_progress(f"kent_progress_page_{i}.json")
                
                # Rotate proxy periodically
                if i % 20 == 0 and self.proxy_pool:
                    print("🔄 Rotating proxy...")
                    self.rotate_proxy()
                
            except KeyboardInterrupt:
                print("\n⚠️ Scraping interrupted by user")
                self.save_progress("kent_interrupted.json")
                break
            except Exception as e:
                print(f"❌ Error on page {i}: {e}")
                continue
        
        # Final save
        final_filename = "kent_repertory_complete.json"
        self.save_progress(final_filename)
        
        # Print summary
        total_body_parts = len(self.all_data)
        total_symptoms = sum(len(symptoms) for symptoms in self.all_data.values())
        
        print(f"\n🎉 Scraping completed!")
        print(f"📊 Summary:")
        print(f"   - Total body parts: {total_body_parts}")
        print(f"   - Total symptoms: {total_symptoms}")
        print(f"   - Output saved to: {final_filename}")


def main():
    scraper = KentRepertoryScraper()
    
    # You can resume from a specific page if needed
    start_page = 1  # Change this to resume from a different page
    
    try:
        scraper.scrape_all_pages(start_page=start_page, save_interval=25)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        scraper.save_progress("kent_error_backup.json")


if __name__ == "__main__":
    main()