import json
import re
from collections import defaultdict
from typing import Dict, List, Set
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HomeopathicDataProcessor:
    def __init__(self):
        self.processed_data = {}
        self.remedy_index = defaultdict(set)  # remedy -> set of (body_part, symptom) tuples
        self.symptom_index = defaultdict(set)  # symptom -> set of body_parts
        
    def load_scraped_data(self, filename: str = 'kents_repertory.json') -> Dict:
        """Load the scraped data from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Loaded data from {filename}")
            return data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return {}
    
    def normalize_symptom_name(self, symptom: str) -> str:
        """Normalize symptom names for consistency"""
        # Convert to lowercase and clean up
        symptom = symptom.lower().strip()
        
        # Remove extra whitespace
        symptom = re.sub(r'\s+', ' ', symptom)
        
        # Remove leading/trailing punctuation
        symptom = re.sub(r'^[-:,.\s]+|[-:,.\s]+$', '', symptom)
        
        # Common normalizations
        normalizations = {
            'aching': 'aching',
            'burning': 'burning',
            'pain': 'pain',
            'throbbing': 'throbbing',
            'shooting': 'shooting',
            'stinging': 'stinging',
            'cramping': 'cramping',
            'inflammation': 'inflammation',
            'swelling': 'swelling',
            'numbness': 'numbness',
            'weakness': 'weakness',
            'anxiety': 'anxiety',
            'depression': 'depression',
            'irritability': 'irritability',
            'restlessness': 'restlessness',
            'fever': 'fever',
            'chills': 'chills',
            'nausea': 'nausea',
            'vomiting': 'vomiting',
            'diarrhea': 'diarrhea',
            'constipation': 'constipation',
            'cough': 'cough',
            'congestion': 'congestion',
            'discharge': 'discharge',
            'eruption': 'eruption',
            'itching': 'itching',
            'dryness': 'dryness',
            'moisture': 'moisture'
        }
        
        # Check if symptom contains any of the normalized terms
        for key, value in normalizations.items():
            if key in symptom:
                return value
                
        return symptom
    
    def extract_symptom_categories(self, symptom: str) -> List[str]:
        """Extract symptom categories from symptom text"""
        categories = []
        
        # Pain-related symptoms
        if any(word in symptom.lower() for word in ['pain', 'aching', 'sore', 'hurt']):
            categories.append('pain')
        
        # Burning sensations
        if any(word in symptom.lower() for word in ['burning', 'burn', 'hot']):
            categories.append('burning')
        
        # Inflammatory symptoms
        if any(word in symptom.lower() for word in ['inflammation', 'swelling', 'inflamed']):
            categories.append('inflammation')
        
        # Neurological symptoms
        if any(word in symptom.lower() for word in ['numbness', 'tingling', 'weakness']):
            categories.append('neurological')
        
        # Mental/emotional symptoms
        if any(word in symptom.lower() for word in ['anxiety', 'fear', 'worry', 'depression', 'sad']):
            categories.append('mental')
        
        # Digestive symptoms
        if any(word in symptom.lower() for word in ['nausea', 'vomiting', 'diarrhea', 'constipation']):
            categories.append('digestive')
        
        # Respiratory symptoms
        if any(word in symptom.lower() for word in ['cough', 'congestion', 'breathing']):
            categories.append('respiratory')
        
        # Skin symptoms
        if any(word in symptom.lower() for word in ['itching', 'rash', 'eruption', 'dry']):
            categories.append('skin')
        
        return categories if categories else ['general']
    
    def process_data_structure(self, raw_data: Dict) -> Dict:
        """Process raw data into the required structure: bodypart{symptoms{medicines}}"""
        processed = {}
        
        for body_part, symptoms_dict in raw_data.items():
            if not symptoms_dict:
                continue
                
            processed[body_part] = {}
            
            for symptom, remedies in symptoms_dict.items():
                if not remedies:
                    continue
                
                # Normalize symptom name
                normalized_symptom = self.normalize_symptom_name(symptom)
                
                # Ensure remedies is a list
                if isinstance(remedies, str):
                    remedies = [remedies]
                
                # Clean up remedy names
                clean_remedies = []
                for remedy in remedies:
                    if isinstance(remedy, str) and remedy.strip():
                        clean_remedies.append(remedy.strip())
                
                if clean_remedies:
                    processed[body_part][normalized_symptom] = clean_remedies
                    
                    # Update indices
                    for remedy in clean_remedies:
                        self.remedy_index[remedy].add((body_part, normalized_symptom))
                    
                    self.symptom_index[normalized_symptom].add(body_part)
        
        return processed
    
    def find_common_remedies(self, body_parts: List[str], symptoms: List[str]) -> Dict:
        """Find common remedies for selected body parts and symptoms"""
        if not body_parts and not symptoms:
            return {}
        
        remedy_scores = defaultdict(int)
        remedy_matches = defaultdict(list)
        
        # Score remedies based on matches
        for body_part in body_parts:
            if body_part in self.processed_data:
                for symptom in symptoms:
                    if symptom in self.processed_data[body_part]:
                        for remedy in self.processed_data[body_part][symptom]:
                            remedy_scores[remedy] += 1
                            remedy_matches[remedy].append(f"{body_part}:{symptom}")
        
        # Also check for remedies that match symptoms across different body parts
        for symptom in symptoms:
            for body_part in self.symptom_index.get(symptom, set()):
                if body_part in body_parts:
                    if symptom in self.processed_data.get(body_part, {}):
                        for remedy in self.processed_data[body_part][symptom]:
                            if remedy not in remedy_matches or f"{body_part}:{symptom}" not in remedy_matches[remedy]:
                                remedy_scores[remedy] += 1
                                remedy_matches[remedy].append(f"{body_part}:{symptom}")
        
        # Sort remedies by score
        sorted_remedies = sorted(remedy_scores.items(), key=lambda x: x[1], reverse=True)
        
        result = {}
        for remedy, score in sorted_remedies:
            result[remedy] = {
                'score': score,
                'matches': remedy_matches[remedy],
                'body_parts': list(set(match.split(':')[0] for match in remedy_matches[remedy])),
                'symptoms': list(set(match.split(':')[1] for match in remedy_matches[remedy]))
            }
        
        return result
    
    def get_symptom_alternatives(self, symptom: str, body_part: str = None) -> List[str]:
        """Get alternative symptoms that are similar"""
        alternatives = []
        
        # Find symptoms with similar words
        for bp, symptoms_dict in self.processed_data.items():
            if body_part and bp != body_part:
                continue
            
            for other_symptom in symptoms_dict.keys():
                if other_symptom != symptom:
                    # Check for word overlap
                    symptom_words = set(symptom.split())
                    other_words = set(other_symptom.split())
                    
                    if symptom_words & other_words:  # If there's any word overlap
                        alternatives.append(other_symptom)
        
        return alternatives
    
    def get_remedy_details(self, remedy: str) -> Dict:
        """Get detailed information about a remedy"""
        details = {
            'name': remedy,
            'body_parts': [],
            'symptoms': [],
            'total_indications': 0
        }
        
        if remedy in self.remedy_index:
            for body_part, symptom in self.remedy_index[remedy]:
                details['body_parts'].append(body_part)
                details['symptoms'].append(symptom)
        
        details['body_parts'] = list(set(details['body_parts']))
        details['symptoms'] = list(set(details['symptoms']))
        details['total_indications'] = len(self.remedy_index[remedy])
        
        return details
    
    def create_search_index(self) -> Dict:
        """Create a search index for faster lookups"""
        search_index = {
            'symptoms_by_body_part': {},
            'remedies_by_symptom': {},
            'remedies_by_body_part': {},
            'body_parts_by_symptom': {},
            'all_symptoms': set(),
            'all_remedies': set(),
            'all_body_parts': set()
        }
        
        for body_part, symptoms_dict in self.processed_data.items():
            search_index['all_body_parts'].add(body_part)
            search_index['symptoms_by_body_part'][body_part] = list(symptoms_dict.keys())
            search_index['remedies_by_body_part'][body_part] = set()
            
            for symptom, remedies in symptoms_dict.items():
                search_index['all_symptoms'].add(symptom)
                search_index['remedies_by_symptom'][symptom] = remedies
                search_index['body_parts_by_symptom'].setdefault(symptom, set()).add(body_part)
                
                for remedy in remedies:
                    search_index['all_remedies'].add(remedy)
                    search_index['remedies_by_body_part'][body_part].add(remedy)
        
        # Convert sets to lists for JSON serialization
        for key, value in search_index.items():
            if isinstance(value, set):
                search_index[key] = list(value)
            elif isinstance(value, dict):
                for k, v in value.items():
                    if isinstance(v, set):
                        search_index[key][k] = list(v)
        
        return search_index
    
    def validate_data_structure(self) -> Dict:
        """Validate the processed data structure"""
        validation_report = {
            'total_body_parts': len(self.processed_data),
            'total_symptoms': len(self.symptom_index),
            'total_remedies': len(self.remedy_index),
            'empty_body_parts': [],
            'body_parts_with_most_symptoms': [],
            'most_common_remedies': [],
            'issues': []
        }
        
        # Check for empty body parts
        for body_part, symptoms_dict in self.processed_data.items():
            if not symptoms_dict:
                validation_report['empty_body_parts'].append(body_part)
        
        # Find body parts with most symptoms
        symptom_counts = [(bp, len(symptoms)) for bp, symptoms in self.processed_data.items()]
        symptom_counts.sort(key=lambda x: x[1], reverse=True)
        validation_report['body_parts_with_most_symptoms'] = symptom_counts[:10]
        
        # Find most common remedies
        remedy_counts = [(remedy, len(matches)) for remedy, matches in self.remedy_index.items()]
        remedy_counts.sort(key=lambda x: x[1], reverse=True)
        validation_report['most_common_remedies'] = remedy_counts[:20]
        
        # Check for data consistency issues
        for body_part, symptoms_dict in self.processed_data.items():
            for symptom, remedies in symptoms_dict.items():
                if not remedies:
                    validation_report['issues'].append(f"Empty remedy list for {body_part}:{symptom}")
                elif not isinstance(remedies, list):
                    validation_report['issues'].append(f"Remedy list not a list for {body_part}:{symptom}")
        
        return validation_report
    
    def process_all(self, input_filename: str = 'kents_repertory.json') -> Dict:
        """Main processing function"""
        logger.info("Starting data processing...")
        
        # Load scraped data
        raw_data = self.load_scraped_data(input_filename)
        if not raw_data:
            logger.error("No data loaded")
            return {}
        
        # Process data structure
        self.processed_data = self.process_data_structure(raw_data)
        
        # Create search index
        search_index = self.create_search_index()
        
        # Validate data
        validation_report = self.validate_data_structure()
        
        # Create final structure
        final_structure = {
            'metadata': {
                'source': 'Kent\'s Repertory of the Homoeopathic Materia Medica',
                'processing_date': str(datetime.now()),
                'total_body_parts': len(self.processed_data),
                'total_symptoms': len(self.symptom_index),
                'total_remedies': len(self.remedy_index)
            },
            'data': self.processed_data,
            'search_index': search_index,
            'validation_report': validation_report
        }
        
        logger.info(f"Processing completed:")
        logger.info(f"  Body parts: {len(self.processed_data)}")
        logger.info(f"  Symptoms: {len(self.symptom_index)}")
        logger.info(f"  Remedies: {len(self.remedy_index)}")
        
        return final_structure
    
    def save_processed_data(self, data: Dict, filename: str = 'processed_homeopathic_data.json'):
        """Save processed data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Processed data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving processed data: {e}")
    
    def create_sample_queries(self) -> List[Dict]:
        """Create sample queries for testing"""
        if not self.processed_data:
            return []
        
        samples = []
        
        # Single body part, single symptom
        body_parts = list(self.processed_data.keys())[:5]
        for body_part in body_parts:
            symptoms = list(self.processed_data[body_part].keys())[:3]
            for symptom in symptoms:
                samples.append({
                    'type': 'single_body_part_single_symptom',
                    'body_parts': [body_part],
                    'symptoms': [symptom],
                    'expected_remedies': self.processed_data[body_part][symptom]
                })
        
        # Multiple body parts, single symptom
        common_symptoms = ['pain', 'burning', 'inflammation']
        for symptom in common_symptoms:
            body_parts_with_symptom = []
            for bp in self.processed_data:
                if symptom in self.processed_data[bp]:
                    body_parts_with_symptom.append(bp)
            
            if len(body_parts_with_symptom) >= 2:
                samples.append({
                    'type': 'multiple_body_parts_single_symptom',
                    'body_parts': body_parts_with_symptom[:3],
                    'symptoms': [symptom],
                    'description': f'Multiple body parts with {symptom}'
                })
        
        # Single body part, multiple symptoms
        for body_part in body_parts[:3]:
            symptoms = list(self.processed_data[body_part].keys())[:3]
            if len(symptoms) >= 2:
                samples.append({
                    'type': 'single_body_part_multiple_symptoms',
                    'body_parts': [body_part],
                    'symptoms': symptoms,
                    'description': f'Multiple symptoms in {body_part}'
                })
        
        # Multiple body parts, multiple symptoms
        samples.append({
            'type': 'multiple_body_parts_multiple_symptoms',
            'body_parts': ['head', 'stomach'],
            'symptoms': ['pain', 'nausea'],
            'description': 'Complex multi-part query'
        })
        
        return samples
    
    def test_queries(self, samples: List[Dict]) -> Dict:
        """Test sample queries"""
        results = {}
        
        for i, sample in enumerate(samples):
            query_id = f"query_{i+1}"
            try:
                result = self.find_common_remedies(
                    sample['body_parts'], 
                    sample['symptoms']
                )
                results[query_id] = {
                    'query': sample,
                    'results': result,
                    'remedy_count': len(result),
                    'status': 'success'
                }
            except Exception as e:
                results[query_id] = {
                    'query': sample,
                    'error': str(e),
                    'status': 'error'
                }
        
        return results


import datetime

def main():
    """Main function to process the scraped data"""
    processor = HomeopathicDataProcessor()
    
    try:
        # Process all data
        processed_data = processor.process_all()
        
        if processed_data:
            # Save processed data
            processor.save_processed_data(processed_data)
            
            # Create and test sample queries
            samples = processor.create_sample_queries()
            test_results = processor.test_queries(samples)
            
            # Save sample queries and results
            with open('sample_queries.json', 'w', encoding='utf-8') as f:
                json.dump({
                    'samples': samples,
                    'test_results': test_results
                }, f, indent=2, ensure_ascii=False)
            
            # Print statistics
            print("\n" + "="*60)
            print("DATA PROCESSING COMPLETED")
            print("="*60)
            print(f"Body parts processed: {processed_data['metadata']['total_body_parts']}")
            print(f"Symptoms processed: {processed_data['metadata']['total_symptoms']}")
            print(f"Remedies processed: {processed_data['metadata']['total_remedies']}")
            
            # Show sample structure
            print("\nSample data structure:")
            for body_part, symptoms_dict in list(processed_data['data'].items())[:2]:
                print(f"\n{body_part}:")
                for symptom, remedies in list(symptoms_dict.items())[:3]:
                    print(f"  {symptom}: {remedies[:5]}...")  # Show first 5 remedies
            
            # Show validation report
            validation = processed_data['validation_report']
            print(f"\nValidation Report:")
            print(f"  Issues found: {len(validation['issues'])}")
            print(f"  Empty body parts: {len(validation['empty_body_parts'])}")
            print(f"  Top body parts by symptom count: {validation['body_parts_with_most_symptoms'][:5]}")
            print(f"  Most common remedies: {validation['most_common_remedies'][:10]}")
            
            # Show sample query results
            print(f"\nSample Query Tests:")
            for query_id, result in test_results.items():
                if result['status'] == 'success':
                    print(f"  {query_id}: Found {result['remedy_count']} remedies")
                else:
                    print(f"  {query_id}: Error - {result['error']}")
            
            print("\nFiles created:")
            print("  - processed_homeopathic_data.json (main data)")
            print("  - sample_queries.json (test queries and results)")
            print("="*60)
            
            return processed_data
        else:
            print("No data processed")
            return None
            
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return None


if __name__ == "__main__":
    processed_data = main()