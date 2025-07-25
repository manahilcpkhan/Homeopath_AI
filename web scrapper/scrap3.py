#!/usr/bin/env python3
"""
Improved Homeopathic Repertory Scraper
Fixed version that properly extracts symptoms and medicines from the actual HTML structure
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
    """Improved scraper that handles the actual HTML structure of Kent's Repertory"""
    
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
        
        # Patterns to exclude (page numbers, navigation, etc.)
        self.exclude_patterns = [
            r'^p\.\s*\d+',                       # p.109, p. 110
            r'^P\.\s*\d+',                       # P.109, P. 110  
            r'^page\s*\d+',                      # page 109
            r'^\d+$',                            # Just numbers
            r'^[a-z]+\s*\d+',                    # lowercase + number
            r'^\s*$',                            # Empty/whitespace
            r'^-+$',                             # Dashes
            r'^next\s*$',                        # Navigation
            r'^prev\s*$',                        # Navigation
            r'^home\s*$',                        # Navigation
            r'^Copyright.*$',                    # Copyright notice
            r'^MEDI-T.*$',                       # MEDI-T notice
        ]
    
    def is_page_reference(self, text: str) -> bool:
        """Check if text is a page reference or navigation element"""
        text = text.strip()
        
        for pattern in self.exclude_patterns:
            if re.match(pattern, text, re.IGNORECASE):
                return True
        return False
    
    def is_main_symptom(self, text: str) -> bool:
        """Check if text is a main symptom (ALL CAPS ending with :)"""
        text = text.strip()
        
        # Skip if it's a page reference
        if self.is_page_reference(text):
            return False
        
        # Main symptoms: ALL CAPS ending with ":"
        if text.endswith(':'):
            # Remove the colon and check if it's mostly uppercase
            symptom_text = text[:-1].strip()
            
            # Should have at least one letter
            if not re.search(r'[A-Za-z]', symptom_text):
                return False
            
            # Check if it's mostly uppercase
            if symptom_text:
                # Count uppercase vs lowercase letters
                upper_count = sum(1 for c in symptom_text if c.isupper() and c.isalpha())
                lower_count = sum(1 for c in symptom_text if c.islower() and c.isalpha())
                
                # If mostly uppercase (at least 70% or starts with uppercase word)
                total_letters = upper_count + lower_count
                if total_letters > 0:
                    upper_ratio = upper_count / total_letters
                    if upper_ratio >= 0.7 or re.match(r'^[A-Z]+', symptom_text):
                        return True
        
        return False
    
    def is_sub_symptom(self, text: str) -> bool:
            """Check if text is a sub-symptom (lowercase ending with :)"""
            text = text.strip()
            
            # Skip if it's a page reference
            if self.is_page_reference(text):
                return False
            
            # Sub-symptoms: start with lowercase, end with ":"
            if text.endswith(':'):
                symptom_text = text[:-1].strip()
                
                # Should have letters
                if not re.search(r'[A-Za-z]', symptom_text):
                    return False
                
                # Should start with lowercase
                if symptom_text and symptom_text[0].islower():
                    return True
            
            return False
        
    def is_medicine_name(self, text: str) -> bool:
        """Checks for medicine-like abbreviations"""
        text = text.strip().rstrip('.')
        patterns = [
            r'^[A-Z][a-z]+(-[a-z]+)*$',        # Nat-m, Kali-c, Ant-s-aur
            r'^[A-Z][a-z]+\s+[a-z]+$',         # Calc carb
            r'^[A-Z]{2,}[a-z-]*$',             # FERR, NUX-V
        ]
        return any(re.match(p, text) for p in patterns)



    
    def extract_text_content(self, soup: BeautifulSoup) -> List[str]:
        """Extract all text content line by line, preserving structure"""
        # Get the main text content
        text_content = soup.get_text()
        
        # Split into lines and clean
        lines = []
        for line in text_content.split('\n'):
            line = line.strip()
            if line and not self.is_page_reference(line):
                lines.append(line)
        
        return lines
    
    def parse_symptoms_from_text(self, lines: List[str]) -> Dict[str, Dict]:
        """Parse symptoms and medicines from text lines"""
        repertory_data = {}
        current_main_symptom = None
        current_sub_symptom = None
        
        logger.info(f"Processing {len(lines)} lines of text")
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if not line:
                i += 1
                continue
            
            # Check if this is a main symptom (ALL CAPS ending with :)
            if self.is_main_symptom(line):
                current_main_symptom = line.rstrip(':').strip()
                current_sub_symptom = None
                if current_main_symptom not in repertory_data:
                    repertory_data[current_main_symptom] = {}
                logger.debug(f"Found main symptom: '{current_main_symptom}'")
                
            # Check if this is a sub-symptom (lowercase ending with :)
            elif self.is_sub_symptom(line):
                if current_main_symptom:
                    current_sub_symptom = line.rstrip(':').strip()
                    if current_sub_symptom not in repertory_data[current_main_symptom]:
                        repertory_data[current_main_symptom][current_sub_symptom] = []
                    logger.debug(f"Found sub-symptom: '{current_sub_symptom}' under '{current_main_symptom}'")
                
            # Check if this line contains medicines
            elif current_main_symptom:
                # Look for medicine patterns in the line
                medicines = self.extract_medicines_from_line(line)
                if medicines:
                    if current_sub_symptom:
                        # Add to sub-symptom
                        existing_medicines = set(repertory_data[current_main_symptom][current_sub_symptom])
                        new_medicines = set(medicines)
                        repertory_data[current_main_symptom][current_sub_symptom] = list(existing_medicines | new_medicines)
                        logger.debug(f"Added {len(medicines)} medicines to '{current_sub_symptom}': {medicines[:3]}...")
                    else:
                        # Add to main symptom (general case)
                        if 'general' not in repertory_data[current_main_symptom]:
                            repertory_data[current_main_symptom]['general'] = []
                        existing_medicines = set(repertory_data[current_main_symptom]['general'])
                        new_medicines = set(medicines)
                        repertory_data[current_main_symptom]['general'] = list(existing_medicines | new_medicines)
                        logger.debug(f"Added {len(medicines)} medicines to '{current_main_symptom}' (general): {medicines[:3]}...")
            
            i += 1
        
        return repertory_data
    
    def extract_medicines_from_line(self, line: str) -> List[str]:
        """Extract medicine names from a line of text"""
        medicines = []
        
        # Split by common separators
        parts = re.split(r'[,;]\s*', line)
        
        for part in parts:
            part = part.strip()
            
            # Check if this looks like a medicine
            words = part.split()
            for word in words:
                word = word.strip('.,;:()[]')
                if self.is_medicine_name(word + '.'):
                    # Clean the medicine name
                    clean_medicine = word.lstrip('*').strip()
                    if clean_medicine and clean_medicine not in medicines:
                        medicines.append(clean_medicine)
        
        return medicines
    
    def extract_symptoms_medicines(self, url: str) -> Dict[str, Dict[str, List[str]]]:
        """Accurately extracts symptoms and medicines, avoiding misclassifying medicine names as symptoms"""
        try:
            logger.info(f"Scraping: {url}")
            time.sleep(random.uniform(1.0, 2.5))
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            data = {}

            for p in soup.find_all('p'):
                # Full paragraph string
                full_text = ''.join(p.stripped_strings)

                # Skip if no colon (means no medicine list)
                if ':' not in full_text:
                    continue

                # Split into left/right of colon
                before_colon, after_colon = full_text.split(':', 1)

                # Try to get main symptom from <b> tag if it's not a medicine
                bold_tag = p.find('b')
                if bold_tag:
                    bold_text = bold_tag.get_text(strip=True).strip(':')
                    if not self.is_medicine_name(bold_text):
                        main_symptom = bold_text
                    else:
                        # fallback: use first few words before colon as symptom
                        main_symptom = before_colon.split(',')[0].strip()
                else:
                    main_symptom = before_colon.split(',')[0].strip()

                # Sub-symptom is what's after main symptom in before_colon
                sub_symptom = before_colon.replace(main_symptom, '').strip(', ')
                if not sub_symptom:
                    sub_symptom = "general"

                # Extract medicines from text after colon
                raw_meds = re.findall(r'\b[A-Z][a-zA-Z-]{1,20}\.', after_colon)
                medicines = [m.strip('.').strip() for m in raw_meds if self.is_medicine_name(m)]

                # Store in dictionary
                if medicines:
                    data.setdefault(main_symptom, {}).setdefault(sub_symptom, [])
                    data[main_symptom][sub_symptom].extend(list(set(medicines)))

            # Deduplicate
            for main in data:
                for sub in data[main]:
                    data[main][sub] = list(set(data[main][sub]))

            logger.info(f"Extracted {len(data)} symptoms from {url}")
            return data

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            return {}


    def extract_all_bold_text(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        """Extract all bold text with their parent context"""
        bold_tags = soup.find_all(['b', 'strong'])
        bold_styled = soup.find_all(attrs={'style': re.compile(r'font-weight:\s*bold', re.I)})
        bold_classes = soup.find_all(attrs={'class': re.compile(r'bold', re.I)})

        elements = bold_tags + bold_styled + bold_classes
        seen = set()
        result = []

        for tag in elements:
            text = tag.get_text(strip=True)
            if text and text not in seen:
                seen.add(text)
                parent = tag.parent.get_text(strip=True) if tag.parent else ""
                result.append((text, parent))

        return result

    def merge_repertory_data(self, existing_data: Dict, new_data: Dict) -> Dict:
        """Merge new repertory data with existing data"""
        for main_symptom, sub_symptoms in new_data.items():
            if main_symptom not in existing_data:
                existing_data[main_symptom] = {}
            
            for sub_symptom, medicines in sub_symptoms.items():
                if sub_symptom not in existing_data[main_symptom]:
                    existing_data[main_symptom][sub_symptom] = []
                
                # Merge medicines (avoid duplicates)
                existing_medicines = set(existing_data[main_symptom][sub_symptom])
                new_medicines = set(medicines)
                existing_data[main_symptom][sub_symptom] = list(existing_medicines | new_medicines)
        
        return existing_data
    
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
            output_file = f"{section_name.lower()}_repertory_fixed.json"
        
        # Initialize data structure
        data = {section_name: {}}
        
        # Generate links
        symptom_links = self.generate_links(start, end, step)
        
        logger.info(f"Starting to scrape {len(symptom_links)} pages for {section_name} section")
        
        total_main_symptoms = 0
        total_entries = 0
        total_medicines = 0
        
        for i, link in enumerate(symptom_links, 1):
            logger.info(f"Progress: {i}/{len(symptom_links)} pages")
            
            try:
                extracted_data = self.extract_symptoms_medicines(link)
                if extracted_data:
                    # Merge with existing data
                    data[section_name] = self.merge_repertory_data(data[section_name], extracted_data)
                    
                    # Count statistics
                    page_main_symptoms = len(extracted_data)
                    page_entries = sum(len(sub_dict) for sub_dict in extracted_data.values())
                    page_medicines = sum(
                        len(medicines) for sub_dict in extracted_data.values() 
                        for medicines in sub_dict.values()
                    )
                    
                    total_main_symptoms += page_main_symptoms
                    total_entries += page_entries
                    total_medicines += page_medicines
                    
                    logger.info(f"Page {i}: {page_main_symptoms} main symptoms, {page_entries} entries, {page_medicines} medicines")
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
            logger.info(f"Total main symptoms: {len(data[section_name])}")
            logger.info(f"Total entries: {sum(len(sub_dict) for sub_dict in data[section_name].values())}")
            
            # Print sample data for verification
            if data[section_name]:
                logger.info("Sample symptoms extracted:")
                for i, (main_symptom, sub_symptoms) in enumerate(list(data[section_name].items())[:5]):
                    logger.info(f"  {i+1}. '{main_symptom}':")
                    for j, (sub_symptom, medicines) in enumerate(list(sub_symptoms.items())[:3]):
                        logger.info(f"    - '{sub_symptom}': {medicines}")
            
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
        
        for section_name, main_symptoms in data.items():
            section_stats = {
                'total_main_symptoms': len(main_symptoms),
                'total_entries': 0,
                'total_medicines': 0,
                'symptoms_without_medicines': 0,
                'page_references_found': 0
            }
            
            for main_symptom, sub_symptoms in main_symptoms.items():
                # Check for page references in main symptoms
                if self.is_page_reference(main_symptom):
                    section_stats['page_references_found'] += 1
                    validation['issues'].append(f"Page reference found in main symptom: '{main_symptom}'")
                
                section_stats['total_entries'] += len(sub_symptoms)
                
                for sub_symptom, medicines in sub_symptoms.items():
                    # Check for page references in sub-symptoms
                    if self.is_page_reference(sub_symptom):
                        section_stats['page_references_found'] += 1
                        validation['issues'].append(f"Page reference found in sub-symptom: '{sub_symptom}'")
                    
                    # Count medicines
                    section_stats['total_medicines'] += len(medicines)
                    
                    # Check for entries without medicines
                    if not medicines:
                        section_stats['symptoms_without_medicines'] += 1
                    
                    # Check for page references in medicines
                    for medicine in medicines:
                        if self.is_page_reference(medicine):
                            validation['issues'].append(f"Page reference found in medicine: '{medicine}'")
            
            validation['statistics'][section_name] = section_stats
        
        return validation

def main():
    """Main function to run the fixed scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fixed Homeopathic Repertory Scraper")
    parser.add_argument("--section", default="Mind", help="Section name")
    parser.add_argument("--output", help="Output file name")
    parser.add_argument("--start", type=int, default=105, help="Start page number")
    parser.add_argument("--end", type=int, default=110, help="End page number (use small range for testing)")
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
            print(f"  Main symptoms: {stats['total_main_symptoms']}")
            print(f"  Total entries: {stats['total_entries']}")
            print(f"  Total medicines: {stats['total_medicines']}")
            print(f"  Entries without medicines: {stats['symptoms_without_medicines']}")
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