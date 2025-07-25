#failed attempt

import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random


def get_page_text(url: str):
    """Fetch page content and extract body parts and visible lines"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    time.sleep(random.uniform(1.0, 2.0))
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract all visible text lines
    text = soup.get_text(separator="\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    # Identify body parts from navigation links (e.g., HEAD, VERTIGO)
    body_parts = set()
    for a in soup.find_all("a", href=True):
        if a.text.isupper() and len(a.text.strip()) > 2:
            body_parts.add(a.text.strip())

    return lines, body_parts


def parse_lines(lines, body_parts):
    """Parse lines into structured format: bodypart -> symptom -> sub-symptom -> [meds]"""
    data = {}
    current_body_part = None
    current_symptom = None

    i = 0
    while i < len(lines):
        line = lines[i]

        # Detect body part (must be followed by p. xxx line)
        if line in body_parts and i + 1 < len(lines) and re.match(r'^p\.\s*\d+', lines[i + 1].lower()):
            current_body_part = line
            current_symptom = None
            i += 2
            continue

        # Match capitalized symptom phrase
        match = re.match(r'^([A-Z][A-Z\s\-]+)(.*)$', line)
        if match:
            current_symptom = match.group(1).strip()
            remainder = match.group(2).strip()
        elif current_symptom:
            remainder = line
        else:
            i += 1
            continue

        if ':' in remainder:
            sub_part, meds_part = remainder.split(':', 1)
            sub_part = sub_part.strip()
            meds_part = meds_part.strip()

            if sub_part:
                tokens = sub_part.split(' ', 1)
                if len(tokens) == 2:
                    symptom_keyword = tokens[0].strip()
                    sub_symptom = tokens[1].strip()
                else:
                    symptom_keyword = sub_part
                    sub_symptom = "general"
            else:
                symptom_keyword = current_symptom
                sub_symptom = "general"
        else:
            symptom_keyword = current_symptom
            sub_symptom = "general"
            meds_part = remainder.strip()

        meds_raw = re.findall(r'\b[A-Za-z][A-Za-z-]{0,20}\.', meds_part)
        medicines = [m.rstrip('.').strip().capitalize() for m in meds_raw]

        if current_body_part and symptom_keyword and medicines:
            data.setdefault(current_body_part, {})
            data[current_body_part].setdefault(symptom_keyword, {})
            data[current_body_part][symptom_keyword].setdefault(sub_symptom, [])
            data[current_body_part][symptom_keyword][sub_symptom].extend(medicines)

        i += 1

    # Remove duplicates
    for bp in data:
        for symptom in data[bp]:
            for sub in data[bp][symptom]:
                data[bp][symptom][sub] = sorted(set(data[bp][symptom][sub]))

    return data


def main():
    url = "http://homeoint.org/books/kentrep/kent0105.htm"  # Replace with other URLs to scrape different pages
    output_file = "kent_restructured.json"

    print(f"📥 Scraping from: {url}")
    lines, body_parts = get_page_text(url)
    data = parse_lines(lines, body_parts)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✅ Done. Saved to '{output_file}'")
    print(f"🧠 Extracted body parts: {len(data)}")
    print(f"📌 Sample:\n")
    for bp in list(data)[:1]:
        print(f"{bp}:")
        for sym, subs in list(data[bp].items())[:3]:
            print(f"  {sym}:")
            for sub, meds in list(subs.items())[:2]:
                print(f"    - {sub}: {meds}")


if __name__ == "__main__":
    main()
