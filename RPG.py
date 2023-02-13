from datetime import datetime, timezone
import numpy as np
import pandas as pd
import bs4 as bs
import sqlalchemy
from sqlalchemy import inspect, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import sys
import cloudscraper
import cfscrape
import httpx

import os

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

sys.stdout.write('[LOG] start... \n')
INDEED_BASE_URL = 'http://nl.indeed.com/jobs'

DEFAULT_HEADERS = {
    # lets use Chrome browser on Windows:
    "User-Agent": "Mozilla/4.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=-1.9,image/webp,image/apng,*/*;q=0.8",
}


SERVER = 'rockfeather.database.windows.net'
DATABASE = 'Rockfeather'
ODBC_DRIVER = 'FreeTDS'
# for local testing
ODBC_DRIVER_TEST = 'ODBC Driver 17 for SQL Server'
PORT = 1433
load_dotenv()
sys.stdout.write('[LOG] get credentials \n')


KVUri = f"https://akv-rockfeather.vault.azure.net"
keyVaultName = "akv-rockfeather"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)
username = client.get_secret('rpguser').value
pwd = client.get_secret('rpgpwd').value

# for local testing
username = os.environ['db_username']
pwd =  os.environ['db_pwd']

sys.stdout.write('[LOG] start creating engine \n')

connection_string = "DRIVER={{{driver}}};SERVER={server}, {port};DATABASE={db};UID={username};PWD={pwd}".format(driver=ODBC_DRIVER, server=SERVER, db=DATABASE, username=username, pwd=pwd, port=PORT)
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = sqlalchemy.create_engine(connection_url)
sys.stdout.write('[LOG] engine created \n')


JOBS_TABLE_NAME = "fct_JobPostingHistory"
LOG_TABLE_NAME = "fct_JobLog"
TABLE_NAME_FULL = "rpg.{table}"
SQL_CREATE_LOG_TABLE = """
CREATE TABLE {table} (
    time_start time(0) NULL,
    time_end time(0) NULL,
    date_run date NULL
) 
""" 
SQL_CREATE_JOB_TABLE = """
CREATE TABLE {table} (
        rpg_search_id int NULL,
        search_query nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        search_location nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        job_code nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        job_title nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        job_summary nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        job_url nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        company_name nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        company_location nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        company_url nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        date_job nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        date_scrape date NULL
    ) 
"""
SQL_INSERT = """
INSERT INTO {table}
(rpg_search_id, search_query, search_location, job_code, job_title, job_summary, job_url, company_name, company_location, company_url, date_job, date_scrape)
VALUES({rpg_search_id}, '{search_query}', '{search_location}', '{job_code}', '{job_title}', '{job_summary}', '{job_url}', '{company_name}', '{company_location}',' {company_url}', '{date_job}', '{date_scrape}');
"""
SQL_SELECT_KEYWORD = """
SELECT COUNT(job_code)
FROM {table}
WHERE search_query='{keyword_query}';
"""
SQL_LOG_INSERT = """
INSERT INTO {table}
(time_start, time_end, date_run)
VALUES('{start}', '{end}', '{date}');
"""
LOCATIONS = ['Zuid-Holland', 'Noord-Holland', 'Groningen (provincie)', 'Friesland', 'Drenthe', 'Zeeland',
                    'Overijssel', 'Flevoland', 'Gelderland', 'Utrecht (provincie)', 'Noord-Brabant', 'Limburg']

now = datetime.now
"""
return all keywords from table in database
"""
def get_keywords():
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    keywords_dict = {}

    with engine.connect() as conn:
        keywords = pd.read_sql(
            "select search_query, rpg_search_id from rpg.dim_SearchQuery",
            con = conn
        )
        for i, row in keywords.iterrows():

            keyword = row['search_query']
            search_id = row['rpg_search_id']

            count = conn.execute(text(
                SQL_SELECT_KEYWORD.format(table=TABLE_NAME_FULL.format(table=JOBS_TABLE_NAME), keyword_query=keyword)
                )).fetchone()

            f_count = count[0] if count else [0]
            # check if entries exist already for specifc keyword, route to historic or daily update
            keywords_dict.update({keyword : {
                "age" : 125 if f_count == 0 else 1,
                "rpg_search_id" : search_id
            }})

    return keywords_dict
"""
format url based on pagination
"""
def format_url(url, count, location):
    return f"{url}&start={count}&l={location}"
"""
exract jobs from soup
"""
def extract_jobs(soup, keyword, search_id, loc):
    # array of jobs
    jobs_page_all = []
    # check if next page
    has_next = False
    # Look for errors
    if soup.find('div', {'id': 'increased_radius_result'}) or \
                soup.find('div', {'class': 'bad_query'}) or \
                soup.find('div', {'id': 'suggested_queries'}) or \
                soup.find('div', {'class': 'no_results'}) or \
                soup.find('div', {'id': 'search_suggestions'} or
                                                soup.find('td', {'id': 'resultsCol'}) is None):
        sys.stdout.write("[ERROR] Denied access by website")
        return (jobs_page_all, False)
    # Get pagination
    page_navigation = soup.find_all("nav")

    if len(page_navigation) > 0:
        nl_check = page_navigation[-1].find('a', {'aria-label': 'Volgende'})
        en_check = page_navigation[-1].find('a', {'aria-label': 'Next Page'})
        has_nav_volgende = nl_check or en_check
        has_next = bool(has_nav_volgende)
    # parse jobs
    jobs_page = soup.find_all("ul", attrs={"class": "jobsearch-ResultsList"})
    if len(jobs_page) > 0 :
        for job in jobs_page[-1] :
            if job.find("div", attrs={"class":"mosaic-zone"}):
                continue
            jobs_page_all.append(parse_job(job, keyword, search_id, loc))
    return (jobs_page_all, has_next)
"""
parse jobs from html block
"""
def parse_job(job, keyword, search_id, loc):

    job_title = job.find('h2', attrs={'class': 'jobTitle'})
    company_name = job.find('span', attrs={'class': 'companyName'})
    company_url = company_name.find('a', attrs={'class': 'companyOverviewLink'}) if company_name else None
    company_location = job.find('div', attrs={'class': 'companyLocation'})
    job_url = job.find('a', attrs={'class': 'jcs-JobTitle'})
    job_code = job.find('a', attrs= {'class': 'jcs-JobTitle'})
    job_summary = job.find('div', attrs={'class': 'job-snippet'})
    job_date = job.find('span', attrs= {'class': 'date'})
    if job_title :
        job_title = job_title.find('a', attrs={'class', 'jcs-JobTitle'}).get_text().replace("'", "  ")
    if company_name:
        company_name = company_name.text.strip().replace("'", " ")
    if company_location:
        company_location = company_location.text.strip().replace("'", " ")
    if job_code:
        job_code = job_code['data-jk']
    if job_summary:
        job_summary = job_summary.text.strip().replace("'", " ")
    if job_date:
        job_date = job_date.text.strip()
    if job_url:
        job_url = 'https://nl.indeed.com' + job_url['href'].replace("'", "  ")
    if company_url:
        company_url = 'https://nl.indeed.com' + company_url["href"].replace("'", "  ")
    job_info = {"search_query": keyword,
                    "rpg_search_id": search_id,
                    "job_code": job_code,
                    "job_title": job_title,
                    "company_name": company_name,
                    "company_url": company_url,
                    "company_location": company_location,
                    "search_location": loc,
                    "job_summary": job_summary,
                    "job_url": job_url,
                    "date_job": job_date,
                    "date_scrape": now(timezone.utc).strftime("%Y-%m-%d")}
    return job_info
"""
return the 'create script' for a specific table            
"""
def get_create_script(table):
    if table == "fct_JobPostingHistory":
        return SQL_CREATE_JOB_TABLE
    elif table == "fct_JobLog":
        return SQL_CREATE_LOG_TABLE
    else:
        sys.stdout.write("[ERROR] Unknown table")
        return
""" 
get the amount of results of a particular utility function
"""
def get_amount_results(query_url):
    scraper = cfscrape.create_scraper()
    page_html = scraper.get(query_url).text
    soup = bs.BeautifulSoup(page_html, features="html.parser")
    block = soup.find("div",attrs={'class': 'jobsearch-JobCountAndSortPane-jobCount'} )
    #scrape amount of results
    if block:
        amount_str = block.find("span").text.split(" ")[3].replace(",", "").replace(".", "")
    else :
        return False
    amount = int(amount_str)
    return amount > 1000
"""
Create table if it doesnt exist yet
"""
def prep_table():
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="rpg")

    tables_to_create = [JOBS_TABLE_NAME, LOG_TABLE_NAME]

    with engine.begin() as conn:
        for table in tables_to_create:
            if table not in tables:
                sys.stdout.write(f"[LOG] schema {table} is being created")
                conn.execute(text(get_create_script(table).format(table=TABLE_NAME_FULL.format(table=table))))
"""
return indeed search URL filled with fornatted search query
"""
def create_requests():
    urls = []
    keywords = get_keywords()
    for keyword, info in keywords.items() :
        urls.append(
            (keyword, info['rpg_search_id'], f"{INDEED_BASE_URL}?q={keyword}&fromage={info['age']}&sort=date")
        )
    return urls
"""
get pages soup from webscraping and extract jobs from them  
"""
def get_sources(query_url, keyword, search_id, loc=""):
    sys.stdout.write(f"[LOG] starts scraping {query_url}... \n")
    scraper = cfscrape.create_scraper()
    has_next = True
    jobs_pages  = []
    count = 0
    while has_next:
        # get html page for a single page for a keyword
        page_html = scraper.get(format_url(query_url, count*10, loc)).text
        soup = bs.BeautifulSoup(page_html, features="html.parser")
        # extract info from job
        (job_info, has_next) = (extract_jobs(soup, keyword, search_id, loc))
        # add info to array
        jobs_pages.append(job_info)
        count += 1
    flat_array = np.array(jobs_pages, dtype=object)
    sys.stdout.write(f"[LOG] finished scraping {count} page(s)) for {keyword}, added {(15*(len(flat_array)-1))+(len(job_info))} job(s) \n")
    return flat_array

"""
load all jobs related to one keyword to an array 
"""
def load_jobs(jobskeyword=None):
    if jobskeyword is None:
        return False
    with engine.begin() as conn:
        for jobs in jobskeyword:
            for job in jobs:
                conn.execute(text(SQL_INSERT.format(
                    table=TABLE_NAME_FULL.format(table=JOBS_TABLE_NAME),
                    rpg_search_id= job["rpg_search_id"],
                    search_query= job["search_query"],
                    search_location=job["search_location"],
                    job_code=job["job_code"],
                    job_title=job["job_title"],
                    job_summary=job["job_summary"],
                    job_url=job["job_url"],
                    company_name=job["company_name"],
                    company_location=job["company_location"],
                    company_url= job["company_url"],
                    date_job=job["date_job"],
                    date_scrape=job["date_scrape"],
                )))

"""
load start, end time and date into schema 
"""
def load_log(start, end, date):
    with engine.begin() as conn:
        conn.execute(text(SQL_LOG_INSERT.format(
            table=TABLE_NAME_FULL.format(table=LOG_TABLE_NAME),
            start=start,
            end=end,
            date=date,
        )))

def brain():
    sys.stdout.write('[LOG] brain turns on \n')
    start = now(timezone.utc).strftime("%H:%M:%S")
    prep_table()
    # create URL query request for job posting
    source_urls = create_requests()
    # For each keyword, extract, parse and store jobs
    for (keyword, search_id, url) in source_urls:
        lot_of_results = get_amount_results(url)
        clusters = LOCATIONS if lot_of_results else [""]
        for location in clusters:
            alljobskeyword = get_sources(url, keyword, search_id, loc=location)
            load_jobs(alljobskeyword)
    end = now(timezone.utc).strftime("%H:%M:%S")
    load_log(start=start, end=end, date=now().strftime("%m/%d/%Y"))
    sys.stdout.write('[LOG] successfull  \n')
brain()
