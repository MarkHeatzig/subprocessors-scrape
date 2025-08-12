import json
import pandas as pd
import logging
from typing import List, Dict

import agentql
from agentql.ext.playwright.sync_api import Page
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def load_config(file_path: str) -> List[Dict[str, str]]:
    """Load company configuration from JSON file"""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data['companies']

def get_subprocessors(page: Page) -> List[Dict[str, str]]:
    """Extract subprocessor data using AgentQL"""
    query = """
    {
        subprocessors[] {
            company_name
            usage_or_service
        }
    }
    """
    try:
        response = page.query_data(query)
        return response.get('subprocessors', [])
    except Exception as e:
        log.error(f"Error extracting subprocessors: {str(e)}")
        return []

def create_dataframe(all_data: List[Dict[str, str]]) -> pd.DataFrame:
    """Create DataFrame from collected subprocessor data"""
    df = pd.DataFrame(all_data)
    return df

def main():
    companies = load_config('companies_AI.json')
    all_subprocessors = []

    with sync_playwright() as p, p.chromium.launch(headless=False) as browser:
        page = agentql.wrap(browser.new_page())

        for company in companies:
            log.info(f"Processing {company['name']}...")
            page.goto(company['url'])
            
            try:
                # Wait a bit for page to load
                page.wait_for_timeout(2000)
                
                subprocessors = get_subprocessors(page)
                
                # Add company name to each subprocessor record
                for subprocessor in subprocessors:
                    subprocessor['Company'] = company['name']
                    all_subprocessors.append({
                        'Company': company['name'],
                        'Sub-processor': subprocessor.get('company_name', ''),
                        'Service category / What they do': subprocessor.get('usage_or_service', '')
                    })
                
                log.info(f"{company['name']}: Found {len(subprocessors)} subprocessors")
                
            except Exception as e:
                log.error(f"Error processing {company['name']}: {str(e)}")
                # Add error record
                all_subprocessors.append({
                    'Company': company['name'],
                    'Sub-processor': 'Error',
                    'Service category / What they do': f'Error: {str(e)}'
                })

    # Create DataFrame and save to CSV
    df = create_dataframe(all_subprocessors)
    df.to_csv('subprocessors_AI_new.csv', index=False)
    
    log.info(f"Subprocessor data saved to subprocessors_updated.csv")
    log.info(f"Total records: {len(df)}")
    
    # Display first few rows for verification
    print("\nFirst 5 rows of data:")
    print(df.head())

if __name__ == "__main__":
    main()