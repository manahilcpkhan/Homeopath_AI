#!/usr/bin/env python3
"""
Improved Homeopathic Repertory Scraper
Fixed version that properly distinguishes symptoms from medicines and filters out page numbers
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
from urllib.parse import urljoin
import random
import logging
from typing import Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

class ImprovedHomeopathicScraper:
    """Improved scraper with better symptom-medicine distinction"""
    
    def __init__(self, base_url="http://homeoint.org/books/kentrep/"):
        self.base_url = base_url
        self.session = requests.Session()
        
        # Set up retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Common medicine patterns (these usually end with periods)
        self.medicine_patterns = [
            r'^[A-Z][a-z]+-[a-z]+\.?$',          # Nat-m., Kali-c.
            r'^[A-Z][a-z]+\.?$',                 # Acon., Bell.
            r'^[A-Z][a-z]+-[a-z]+-[a-z]+\.?$',  # Ant-s-aur.
            r'^[A-Z][a-z]+\s+[a-z]+\.?$',       # Calc carb.
            r'^[A-Z][a-z]{2,8}\.?$',            # Common remedy abbreviations
        ]
        
        # Patterns to exclude (page numbers, navigation, etc.)
        self.exclude_patterns = [
            r'^p\.\s*\d+',                       # p.109, p. 110
            r'^P\.\s*\d+',                       # P.109, P. 110  
            r'^page\s*\d+',                      # page 109
            r'^\d+$',                            # Just numbers
            r'^[A-Z]+$',                         # All caps (likely headers)
            r'^[a-z]+\s*\d+',                    # lowercase + number
            r'^\s*$',                            # Empty/whitespace
        ]
    
    def is_page_reference(self, text: str) -> bool:
        """Check if text is a page reference or navigation element"""
        text = text.strip()
        
        for pattern in self.exclude_patterns:
            if re.match(pattern, text, re.IGNORECASE):
                return True
        return False
    
    def is_medicine_name(self, text: str) -> bool:
        """Enhanced logic to identify medicine names"""
        text = text.strip()
        
        # Skip if it's a page reference
        if self.is_page_reference(text):
            return False
        
        # Medicine names usually end with a period
        if text.endswith('.'):
            # Remove period and check patterns
            clean_text = text.rstrip('.')
            for pattern in self.medicine_patterns:
                if re.match(pattern, clean_text + '.'):
                    return True
        
        # Check patterns without requiring period
        for pattern in self.medicine_patterns:
            if re.match(pattern, text):
                return True
        
        # Additional checks for common medicine characteristics
        if len(text) <= 12:  # Medicines are usually short
            # Check for common medicine patterns
            if re.match(r'^[A-Z][a-z]+(-[a-z]+)*\.?$', text):  # Capitalized with optional hyphens
                return True
            if re.match(r'^[A-Z][a-z]{1,3}\.?$', text):  # Short capitalized (Acon., Bell.)
                return True
        
        return False
    
    def is_symptom(self, text: str) -> bool:
        """Check if text is likely a symptom description"""
        text = text.strip()
        
        # Skip if it's a page reference
        if self.is_page_reference(text):
            return False
        
        # Skip if it looks like a medicine
        if self.is_medicine_name(text):
            return False
        
        # Symptoms are usually longer descriptive phrases
        if len(text) > 15:  # Longer text is likely symptom
            return True
        
        # Common symptom keywords
        symptom_keywords = [
            'pain', 'ache', 'burn', 'itch', 'swell', 'throb', 'shoot', 'sting',
            'cramp', 'numb', 'weak', 'dizzy', 'nausea', 'vomit', 'fever', 'chill',
            'anxiety', 'fear', 'worry', 'sad', 'angry', 'irritab', 'restless',
            'cough', 'breath', 'congest', 'discharge', 'dry', 'moist', 'hot', 'cold'
        ]
        
        text_lower = text.lower()
        if any(keyword in text_lower for keyword in symptom_keywords):
            return True
        
        # If it starts with lowercase and contains spaces, likely a symptom
        if text[0].islower() and ' ' in text:
            return True
        
        # If it's descriptive (contains adjectives/adverbs)
        if any(word in text_lower for word in ['very', 'much', 'little', 'great', 'constant', 'sudden']):
            return True
        
        return False
    
    def extract_bold_text_structured(self, soup: BeautifulSoup) -> List[str]:
        """Extract bold text in document order, filtering out unwanted elements"""
        bold_elements = []
        
        # Find all bold elements: <b>, <strong>
        bold_tags = soup.find_all(['b', 'strong'])
        
        # Also find elements with bold styling
        styled_elements = soup.find_all(attrs={'style': re.compile(r'font-weight:\s*bold', re.I)})
        bold_tags.extend(styled_elements)
        
        for tag in bold_tags:
            text = tag.get_text(strip=True)
            if text and not self.is_page_reference(text):
                bold_elements.append(text)
        
        return bold_elements
    
    def parse_symptoms_medicines(self, bold_texts: List[str]) -> Dict[str, List[str]]:
        """Parse bold text to separate symptoms from medicines"""
        symptoms_data = {}
        current_symptom = None
        current_medicines = []
        
        logger.info(f"Processing {len(bold_texts)} bold elements")
        
        for i, text in enumerate(bold_texts):
            text = text.strip()
            
            if not text or self.is_page_reference(text):
                continue
            
            # Check if this is a medicine
            if self.is_medicine_name(text):
                # Clean medicine name (remove trailing period)
                medicine = text.rstrip('.').strip()
                if medicine:
                    current_medicines.append(medicine)
                    logger.debug(f"Found medicine: {medicine}")
            else:
                # This might be a symptom
                # Save previous symptom if we have medicines for it
                if current_symptom and current_medicines:
                    symptoms_data[current_symptom] = current_medicines.copy()
                    logger.info(f"Saved symptom: '{current_symptom}' -> {len(current_medicines)} medicines")
                
                # Start new symptom only if it looks like a symptom
                if self.is_symptom(text) or (len(text) > 8 and not self.is_medicine_name(text)):
                    current_symptom = text
                    current_medicines = []
                    logger.debug(f"New symptom: '{current_symptom}'")
                else:
                    logger.debug(f"Skipped unclear text: '{text}'")
        
        # Don't forget the last symptom
        if current_symptom and current_medicines:
            symptoms_data[current_symptom] = current_medicines.copy()
            logger.info(f"Saved final symptom: '{current_symptom}' -> {len(current_medicines)} medicines")
        
        return symptoms_data
    
    def extract_symptoms_medicines(self, url: str) -> Dict[str, List[str]]:
        """Extract symptoms and medicines from a page"""
        max_retries = 3
        
        for retry_count in range(max_retries):
            try:
                # Add delay to avoid rate limiting
                time.sleep(random.uniform(1.0, 3.0))
                
                logger.info(f"Scraping: {url} (attempt {retry_count + 1})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, "html.parser")
                
                # Extract bold text in order
                bold_texts = self.extract_bold_text_structured(soup)
                
                # Parse into symptoms and medicines
                symptoms_data = self.parse_symptoms_medicines(bold_texts)
                
                logger.info(f"Extracted {len(symptoms_data)} symptoms from {url}")
                return symptoms_data
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {url} (attempt {retry_count + 1}): {e}")
                if retry_count < max_retries - 1:
                    time.sleep((retry_count + 1) * 2)  # Exponential backoff
                else:
                    logger.error(f"Max retries reached for {url}")
                    return {}
                    
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                return {}
        
        return {}
    
    def generate_links(self, start: int, end: int, step: int = 5) -> List[str]:
        """Generate symptom links with specific numbering pattern"""
        links = []
        for num in range(start, end + 1, step):
            page_num = str(num).zfill(3)
            anchor_num = num + 1
            link = f"{self.base_url}kent0{page_num}.htm#P{anchor_num}"
            links.append(link)
        return links
    
    def scrape_repertory_section(self, start: int, end: int, section_name: str = "Mind", 
                                step: int = 5, output_file: str = None) -> Dict:
        """Scrape a section of the repertory"""
        if output_file is None:
            output_file = f"{section_name.lower()}_repertory_improved.json"
        
        data = {section_name: {}}
        
        # Generate links
        symptom_links = self.generate_links(start, end, step)
        
        logger.info(f"Starting to scrape {len(symptom_links)} pages for {section_name} section")
        
        total_symptoms = 0
        total_medicines = 0
        
        for i, link in enumerate(symptom_links, 1):
            logger.info(f"Progress: {i}/{len(symptom_links)} pages")
            
            try:
                extracted_data = self.extract_symptoms_medicines(link)
                if extracted_data:
                    data[section_name].update(extracted_data)
                    page_symptoms = len(extracted_data)
                    page_medicines = sum(len(meds) for meds in extracted_data.values())
                    total_symptoms += page_symptoms
                    total_medicines += page_medicines
                    logger.info(f"Page {i}: {page_symptoms} symptoms, {page_medicines} medicines")
                else:
                    logger.warning(f"No data extracted from page {i}")
                    
            except Exception as e:
                logger.error(f"Failed to process page {i} ({link}): {e}")
                continue
        
        # Save to JSON file
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Scraping completed. Data saved to {output_file}")
            logger.info(f"Total symptoms: {total_symptoms}")
            logger.info(f"Total medicines: {total_medicines}")
            
            # Print sample data for verification
            if data[section_name]:
                logger.info("Sample symptoms extracted:")
                for i, (symptom, medicines) in enumerate(list(data[section_name].items())[:5]):
                    logger.info(f"  {i+1}. '{symptom}' -> {len(medicines)} medicines: {medicines[:3]}...")
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
        
        return data
    
    def validate_extracted_data(self, data: Dict) -> Dict:
        """Validate the extracted data quality"""
        validation = {
            'total_sections': len(data),
            'issues': [],
            'statistics': {}
        }
        
        for section_name, symptoms_dict in data.items():
            section_stats = {
                'total_symptoms': len(symptoms_dict),
                'total_medicines': 0,
                'avg_medicines_per_symptom': 0,
                'symptoms_without_medicines': 0,
                'page_references_found': 0
            }
            
            for symptom, medicines in symptoms_dict.items():
                # Check for page references in symptoms
                if self.is_page_reference(symptom):
                    section_stats['page_references_found'] += 1
                    validation['issues'].append(f"Page reference found in symptoms: '{symptom}'")
                
                # Count medicines
                section_stats['total_medicines'] += len(medicines)
                
                # Check for symptoms without medicines
                if not medicines:
                    section_stats['symptoms_without_medicines'] += 1
                
                # Check for page references in medicines
                for medicine in medicines:
                    if self.is_page_reference(medicine):
                        validation['issues'].append(f"Page reference found in medicines: '{medicine}'")
            
            if section_stats['total_symptoms'] > 0:
                section_stats['avg_medicines_per_symptom'] = section_stats['total_medicines'] / section_stats['total_symptoms']
            
            validation['statistics'][section_name] = section_stats
        
        return validation

def main():
    """Main function to run the improved scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Improved Homeopathic Repertory Scraper")
    parser.add_argument("--section", default="Mind", help="Section name")
    parser.add_argument("--output", help="Output file name")
    parser.add_argument("--start", type=int, default=105, help="Start page number")
    parser.add_argument("--end", type=int, default=230, help="End page number")
    parser.add_argument("--step", type=int, default=5, help="Step size for page numbers")
    parser.add_argument("--validate", action="store_true", help="Validate extracted data")
    
    args = parser.parse_args()
    
    # Create scraper instance
    scraper = ImprovedHomeopathicScraper()
    
    # Run scraper
    data = scraper.scrape_repertory_section(
        start=args.start,
        end=args.end,
        section_name=args.section,
        step=args.step,
        output_file=args.output
    )
    
    # Validate if requested
    if args.validate and data:
        validation = scraper.validate_extracted_data(data)
        
        print("\n" + "="*50)
        print("VALIDATION REPORT")
        print("="*50)
        
        for section, stats in validation['statistics'].items():
            print(f"\n{section} Section:")
            print(f"  Symptoms: {stats['total_symptoms']}")
            print(f"  Medicines: {stats['total_medicines']}")
            print(f"  Avg medicines per symptom: {stats['avg_medicines_per_symptom']:.1f}")
            print(f"  Symptoms without medicines: {stats['symptoms_without_medicines']}")
            print(f"  Page references found: {stats['page_references_found']}")
        
        if validation['issues']:
            print(f"\nIssues found ({len(validation['issues'])}):")
            for issue in validation['issues'][:10]:  # Show first 10 issues
                print(f"  - {issue}")
            if len(validation['issues']) > 10:
                print(f"  ... and {len(validation['issues']) - 10} more issues")
        else:
            print("\nNo issues found! ✓")

if __name__ == "__main__":
    main()