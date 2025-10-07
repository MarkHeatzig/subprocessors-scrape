from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import json
import requests
import time

print("Starting LinkedIn company scraping...")

USERNAME = ""
PASSWORD = ""

# Company ID mapping
COMPANY_IDS = {
    'ADBE': '1480',
    'ADSK': '1879',
    'AI': '414478',
    'APP': '3076102',
    'ASAN': '807257',
    'BL': '362833',
    'BSY': '4929',
    'CDNS': '2157',
    'CFLT': '88873',
    'CHKP': '3090',
    'CRM': '3185',
    'CRWD': '2497653',
    'CYBR': '26630',
    'DDOG': '1066442',
    'DOCU': '19022',
    'DSY-FR': '3896',
    'DT': '125999',
    'FIG': '3650502',
    'FRSH': '1377014',
    'FTNT': '6460',
    'GTLB': '5101804',
    'HUBS': '68529',
    'INFA': '3858',
    'INTU': '1666',
    'IOT': '6453825',
    'MDB': '783611',
    'MNDY': '2525169',
    'MSFT': '1035',
    'NET': '407222',
    'NOW': '29352',
    'OKTA': '926041',
    'ORCL': '1028',
    'OS': '1244326',
    'PANW': '30086',
    'PATH': '1523656',
    'PAYC': '37759',
    'PCOR': '1912597',
    'PCTY': '24614',
    'PTC': '1935',
    'QLYS': '8561',
    'ROP': '56657',
    'RPD': '39624',
    'S': '2886771',
    'SNOW': '3653845',
    'SNPS': '2457',
    'TEAM': '22688',
    'TENB': '25452',
    'TRMB': '5160',
    'TWLO': '400528',
    'TYL': '9505',
    'U': '212669',
    'VRNS': '25403',
    'WDAY': '17719',
    'WK': '732400',
    'ZI': '16272',
    'ZM': '2532259',
    'ZS': '234625'
}


def parse_engineering_data(json_data):
   try:
       eng_data = next((d for d in json_data['data']['threeMonthHeadCountsByFunction'] 
                       if d['displayName'] == 'Engineering'), None)
       
       if not eng_data:
           return None
           
       six_month = next((d for d in json_data['data']['sixMonthHeadCountsByFunction'] 
                        if d['displayName'] == 'Engineering'), None)
       
       one_year = next((d for d in json_data['data']['oneYearHeadCountsByFunction'] 
                       if d['displayName'] == 'Engineering'), None)
       
       return {
           'three_month': eng_data['employeePercentageDifference'],
           'six_month': six_month['employeePercentageDifference'] if six_month else None,
           'one_year': one_year['employeePercentageDifference'] if one_year else None,
           'headcount': eng_data['employeeCount']
       }
   except Exception as e:
       print(f"Error parsing engineering data: {e}")
       return None

def parse_sales_data(json_data):
   try:
       sales_data = next((d for d in json_data['data']['threeMonthHeadCountsByFunction'] 
                       if d['displayName'] == 'Sales'), None)
       
       if not sales_data:
           return None
           
       six_month = next((d for d in json_data['data']['sixMonthHeadCountsByFunction'] 
                        if d['displayName'] == 'Sales'), None)
       
       one_year = next((d for d in json_data['data']['oneYearHeadCountsByFunction'] 
                       if d['displayName'] == 'Sales'), None)
       
       return {
           'three_month': sales_data['employeePercentageDifference'],
           'six_month': six_month['employeePercentageDifference'] if six_month else None,
           'one_year': one_year['employeePercentageDifference'] if one_year else None,
           'headcount': sales_data['employeeCount']
       }
   except Exception as e:
       print(f"Error parsing sales data: {e}")
       return None

def parse_total_headcount_data(json_data):
    try:
        print("Debugging monthly headcount data:")
        print(f"Full response: {json.dumps(json_data, indent=2)}")
        
        if 'data' not in json_data:
            print("No 'data' field in response")
            return None
            
        if 'monthlyHeadCounts' not in json_data['data']:
            print("No 'monthlyHeadCounts' field in data")
            return None
            
        if not json_data['data']['monthlyHeadCounts']:
            print("monthlyHeadCounts is empty")
            return None
            
        monthly_data = json_data['data']['monthlyHeadCounts'][0]
        print(f"Monthly data found: {monthly_data}")
        return monthly_data['employeeCount']
    except Exception as e:
        print(f"Error parsing total headcount data: {str(e)}")
        print(f"JSON structure: {json.dumps(json_data, indent=2)}")
        return None

def get_employee_insights(driver, company_symbol, company_id):
   API_URL = f"https://www.linkedin.com/sales-api/salesApiEmployeeInsights/{company_id}?employeeInsightType=FUNCTIONAL_HEADCOUNT"
   
   try:
       print(f"\nProcessing {company_symbol}...")
       company_url = f"https://www.linkedin.com/sales/company/{company_id}"
       driver.get(company_url)
       time.sleep(20)
       
       cookies = driver.get_cookies()
       session = requests.Session()
       cookie_dict = {}
       for cookie in cookies:
           session.cookies.set(cookie['name'], cookie['value'])
           cookie_dict[cookie['name']] = cookie['value']
       
       headers = {
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
           'Accept': 'application/vnd.linkedin.normalized+json+2.1',
           'Accept-Language': 'en-US,en;q=0.5',
           'Referer': company_url,
           'csrf-token': cookie_dict.get('JSESSIONID', '').strip('"'),
           'Connection': 'keep-alive',
           'x-restli-protocol-version': '2.0.0',
           'x-li-lang': 'en_US',
           'x-li-page-instance': 'urn:li:page:d_sales2_company_insights;'
       }
       time.sleep(20)
       response = session.get(API_URL, headers=headers)
       time.sleep(5)

       if response.status_code == 200:
           json_data = response.json()
           print(f"\nAPI Response for {company_symbol}:")
           print(f"Status Code: {response.status_code}")
           
           eng_data = parse_engineering_data(json_data)
           sales_data = parse_sales_data(json_data)
           headcount_data = parse_total_headcount_data(json_data)

           return {
               'Company': company_symbol,
               'Engineering': eng_data,
               'Sales': sales_data,
               'Headcount': headcount_data
           }
       else:
           print(f"Failed to get data for {company_symbol}: {response.status_code}")
           print(f"Response content: {response.text}")
       
       return None
           
   except Exception as e:
       print(f"Error processing {company_symbol}: {str(e)}")
       print(f"Full error details: {e.__class__.__name__}")
       return None

def scrape_all_companies():
   print("About to start Chrome WebDriver...")
   driver = webdriver.Chrome()
   print("Chrome WebDriver started.")
   eng_results = []
   sales_results = []
   headcount_results = []
   
   try:
       print("Logging into LinkedIn...")
       driver.get("https://www.linkedin.com/login")
       
       username_field = WebDriverWait(driver, 10).until(
           EC.presence_of_element_located((By.ID, "username"))
       )
       password_field = driver.find_element(By.ID, "password")
       
       username_field.send_keys(USERNAME)
       password_field.send_keys(PASSWORD)
       password_field.submit()
       
       WebDriverWait(driver, 20).until(
           EC.presence_of_element_located((By.CSS_SELECTOR, ".global-nav"))
       )
       
       driver.get("https://www.linkedin.com/sales/home")
       time.sleep(5)
       
       for symbol, company_id in COMPANY_IDS.items():
           company_data = get_employee_insights(driver, symbol, company_id)
           if company_data:
               if company_data['Engineering']:
                   eng_results.append({
                       'Company': symbol,
                       '3month': company_data['Engineering']['three_month'],
                       '6month': company_data['Engineering']['six_month'],
                       '1year': company_data['Engineering']['one_year'],
                       'Headcount': company_data['Engineering']['headcount']
                   })
               if company_data['Sales']:
                   sales_results.append({
                       'Company': symbol,
                       '3month': company_data['Sales']['three_month'],
                       '6month': company_data['Sales']['six_month'],
                       '1year': company_data['Sales']['one_year'],
                       'Headcount': company_data['Sales']['headcount']
                   })
               if company_data['Headcount']:
                   headcount_results.append({
                       'Company': symbol,
                       'Total Headcount': company_data['Headcount'],
                   })
           time.sleep(2)
           
   except Exception as e:
       print(f"Error during scraping: {e}")
   finally:
       driver.quit()
   
   if eng_results or sales_results or headcount_results:
       with pd.ExcelWriter('company_headcount_analysis.xlsx') as writer:
           if eng_results:
               eng_df = pd.DataFrame(eng_results)
               eng_df.to_excel(writer, sheet_name='Engineering', index=False)
           
           if sales_results:
               sales_df = pd.DataFrame(sales_results)
               sales_df.to_excel(writer, sheet_name='Sales', index=False)
           
           if headcount_results:
               headcount_df = pd.DataFrame(headcount_results)
               headcount_df.to_excel(writer, sheet_name='Total Headcount', index=False)
               
       print("\nData saved to company_headcount_analysis.xlsx")
       return True
   return None

def main():
   print("Starting LinkedIn company scraping...")
   result = scrape_all_companies()
   if result is None:
       print("Process failed")

if __name__ == "__main__":
   main()