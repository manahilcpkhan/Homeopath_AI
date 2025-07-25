#for pure text extract

import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random


def get_page_text(url: str) -> list:
    """Fetch and split visible text from HTML page into lines"""
    headers = {
        'User-Agent': 'Mozilla/5.0',
    }
    time.sleep(random.uniform(1.0, 2.0))  # Be polite to the server
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text(separator='\n')
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return lines


def parse_repertory_lines(lines):
    """Parse Kent lines with improved sub-symptom extraction."""
    data = {}
    current_main = None

    for line in lines:
        line = line.strip()

        if not line or "See " in line:
            continue

        # Match the first ALL CAPS word/phrase (main symptom)
        match = re.match(r'^([A-Z][A-Z\s\-]+)(.*)$', line)
        if match:
            current_main = match.group(1).strip()
            remainder = match.group(2).strip()
        elif current_main:
            remainder = line
        else:
            continue  # skip invalid lines

        # Now handle sub-symptom and medicine part
        if ':' in remainder:
            sub_part, meds_part = remainder.split(':', 1)
            sub_symptom = sub_part.strip(' ,.') or "general"
        else:
            sub_symptom = "general"
            meds_part = remainder.strip()

        # Extract medicine abbreviations ending in dot
        meds_raw = re.findall(r'\b[A-Za-z][A-Za-z-]{0,20}\.', meds_part)
        medicines = [m.rstrip('.').strip().capitalize() for m in meds_raw]

        if medicines:
            data.setdefault(current_main, {}).setdefault(sub_symptom, [])
            data[current_main][sub_symptom].extend(medicines)

    # Deduplicate and sort
    for main in data:
        for sub in data[main]:
            data[main][sub] = sorted(list(set(data[main][sub])))

    return data



def main():
    url = "http://homeoint.org/books/kentrep/kent0105.htm#P109"  # Example page
    output_file = "kent_parsed.json"

    print(f"Scraping: {url}")
    lines = get_page_text(url)
    data = parse_repertory_lines(lines)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Parsed {len(data)} main symptoms.")
    print(f"📄 Output saved to: {output_file}")


if __name__ == "__main__":
    main()
