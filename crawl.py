# %%
import bs4
import requests

# %%
import os 
from dotenv import load_dotenv

load_dotenv()

from pymongo.mongo_client import MongoClient
uri = os.getenv('MONGO_CONN_URI')
# Create a new client and connect to the server
client = MongoClient(uri)
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    
db = client['all_data_gcp']

# %%
def get_soup(url):
    base_url = 'https://sp1.hso.mohw.gov.tw'
    # set up the request headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Origin': 'https://sp1.hso.mohw.gov.tw',
    }
       
    # timeout = httpx.Timeout(15.0, read=None)
    response = requests.get(base_url + url, headers=headers, timeout=30)
    response.encoding = 'big5'
    
    return bs4.BeautifulSoup(response.text, 'html.parser')

# %%
def get_categories():
    soup = get_soup('/doctor/Index1.php')
    
    result = []
    for category in soup.find_all('div', {'class': 'w3-col l4 m6 s6'}):
        category_name = category.find('a').text
        category_name = category_name.replace('\u3000', '')        
        
        # cat_url = f'/doctor/All/history.php?UrlClass={category_name}&SortBy=q_no&PageNo={page_num}'
        cat_url = f'/doctor/All/history.php?UrlClass={category_name}&SortBy=q_no'
        
        # check if num_pages can be converted to int
        try:
            num_pages = get_soup(cat_url).find_all('option')[-1].text
            num_pages = int(num_pages)
        except:
            num_pages = 1
            
        # print(category_name, num_pages)
        
        result.append({
            'name': category_name,
            'url': cat_url,
            'num_pages': num_pages
        })  
        
    return result

# %%
col = db['categories']
# check if the collection is empty, if it's empty, insert the categories
if col.count_documents({}) == 0:
    categories = get_categories()
    col.insert_many(categories)
else:
    categories = col.find()

# %%
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from time import sleep
import logging

logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import random

num_threads = 24
article_error_count = 0
page_error_count = 0

def crawl_articles(category):
    def get_article_content(article):
        sleep(random.uniform(2.5, 5))
        
        soup = get_soup(article['url'])

        divs = soup.find_all('div', {'class': 'w3-bar-item w3-padding-0'})
        q, a = divs[3].text.strip(), divs[5].text.strip()

        # split by the last comma and strip the date
        q_person, q_date = q[:q.rfind(',')].strip(), q[q.rfind(',') + 1:].strip()
        q_date = q_date.split('\xa0')[0].strip()
        a_person, a_date = a[:a.rfind(',')].strip(), a[a.rfind(',') + 1:].strip()
        a_date = a_date.split('\xa0')[0].strip()

        id = int(divs[0].text[1:])
        
        msgs = soup.find_all('div', {'class': 'msg'})
        msgs = [msg.text.strip() for msg in msgs]
        assert len(msgs) == 2

        question, answer = msgs[0], msgs[1] 
        
        return {
            'id': id,
            'q_person': q_person,
            'q_date': q_date,
            'a_person': a_person,
            'a_date': a_date,
            'question': question,
            'answer': answer
        }
        ###
    
    def crawl_page(page_num):
        nonlocal category
        url = category['url'] + f'&PageNo={page_num}'
        soup = get_soup(url)
        
        table = soup.find('table', {'class': 'table1'})
        
        # replace &lt; and &gt; with < and >
        table = str(table).replace('&lt;', '<').replace('&gt;', '>')
        table = bs4.BeautifulSoup(table, 'html.parser')
        rows = table.find_all('tr')[1:]

        result = []
        for row in rows:
            tds = row.find_all('td')
            
            try:
                article = {
                    'date': tds[0].text.strip(),
                    'patient_name': tds[1].text.strip(),
                    'doctor_name': tds[2].text.strip(),
                    'satisfactory': float(x) if (x := tds[3].text.strip()) else None,
                    'rating': float(x) if (x := tds[4].text.strip()) else None,
                    'views': int(x) if (x := tds[5].text.strip()) else 0,
                    'title': tds[6].text.strip(),
                    'url': '/doctor/All/' + tds[6].find('a')['href']
                }
            except Exception as e:
                print(f"On page {page_num}, error processing row {row}: {e}")
                continue
            
            max_retries = 10
            base_delay = 5
            global article_error_count

            for retry_count in range(max_retries):
                try:
                    a = get_article_content(article)
                    article.update(a)
                    result.append(article)
                    break  # 如果成功，跳出迴圈
                except Exception as e:
                    if retry_count < max_retries - 1:
                        # print(f"Retrying... (Attempt {retry_count + 1}/{max_retries})")
                        delay = base_delay + 5 * retry_count  # 退避策略
                        sleep(delay)
                    else:
                        article_error_count += 1
                        
                        logging.error(f"{article_error_count} - Error processing article {article['title']}: {e}")
                        logging.error(f"URL: {article['url']}")
                        
                        print(f"Error processing article {article['title']}: {e}") 
                        print(f"URL: {article['url']}")

        return result


    num_pages = category['num_pages']
    name = category['name']
    col = db[name]
    
    def retry_crawl_page(page_num):
        max_retries = 10  # 設定最大重試次數
        retry_delay = 5  # 設定重試延遲時間
        
        for retry_count in range(max_retries):
            try:
                return crawl_page(page_num)  # 呼叫爬取頁面的函式
            except Exception as e:
                if retry_count < max_retries - 1:
                    # print(f"Retrying... (Attempt {retry_count + 1}/{max_retries})")
                    # print(f"Waiting for {retry_delay} seconds before retrying...")
                    sleep(retry_delay)
                else:
                    raise e  # 如果達到最大重試次數，則拋出錯誤
    
    # parallelize the crawling of pages   
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {
            executor.submit(retry_crawl_page, page_num): page_num 
            for page_num in range(1, num_pages + 1)
        }

        global page_error_count
        
        for future in as_completed(futures):
            page_num = futures[future]
            try:
                result = future.result()
                col.insert_many(result)
                # print(f"Completed processing for page {page_num}.")
            except Exception as e:        
                page_error_count += 1
                   
                print(f"{page_error_count} - Error processing page {page_num}: {e}")
                print(f"URL: {category['url']}&PageNo={page_num}")
                
                logging.error(f"{page_error_count} - Error processing page {page_num}: {e}")
                logging.error(f"URL: {category['url']}&PageNo={page_num}")
                
# %%
for category in categories:
    print(f'Crawling {category["name"]}...')
    crawl_articles(category)


