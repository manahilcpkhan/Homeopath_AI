import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random


def get_page_lines_and_body_parts(url: str):
    headers = {'User-Agent': 'Mozilla/5.0'}
    time.sleep(random.uniform(1.0, 2.0))
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Find body parts from nav links
    nav_links = soup.find_all("a", href=True)
    body_parts = set()
    for link in nav_links:
        if link.text.isupper() and len(link.text) > 2:
            body_parts.add(link.text.strip())

    # Extract page text
    text = soup.get_text(separator='\n')
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return lines, body_parts


def parse_lines(lines, body_parts):
    data = {}
    current_body_part = None
    current_symptom = None

    i = 0
    while i < len(lines):
        line = lines[i]

        # Detect body part using next-line logic (e.g. HEAD followed by p. 109)
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

        # Store in JSON
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


def main():
    url = "http://homeoint.org/books/kentrep/kent0105.htm"
    output_file = "kent_output.json"

    print(f"🔍 Scraping: {url}")
    lines, body_parts = get_page_lines_and_body_parts(url)
    data = parse_lines(lines, body_parts)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✅ Done. Saved to {output_file}")


if __name__ == "__main__":
    main()
