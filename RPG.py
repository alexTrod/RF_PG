from datetime import datetime, timezone
from sqlalchemy import inspect, text
from sqlalchemy.engine import URL
import urllib.parse
import pandas as pd
import numpy as np
import http.client
import sqlalchemy
import bs4 as bs
import sys
import html
import time
import re
# import os

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

sys.stdout.write('[LOG] start... \n')
INDEED_BASE_URL = 'http://nl.indeed.com'

DEFAULT_HEADERS = {
    # lets use Chrome browser on Windows:
    "User-Agent": "Mozilla/4.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=-1.9,image/webp,image/apng,*/*;q=0.8",
}

SERVER = 'rockfeather.database.windows.net'
DATABASE = 'Rockfeather'
ODBC_DRIVER = 'FreeTDS'
# for local testing
# ODBC_DRIVER_TEST = 'ODBC Driver 17 for SQL Server'
PORT = 1433
# load_dotenv()
sys.stdout.write('[LOG] get credentials \n')

KVUri = f"https://akv-rockfeather.vault.azure.net"
keyVaultName = "akv-rockfeather"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)
username = client.get_secret('rpguser').value
pwd = client.get_secret('rpgpwd').value
api_key = client.get_secret('ScrapingAnt-API-Key').value

# for local testing
# username = os.environ['db_username']
# pwd = os.environ['db_pwd']

sys.stdout.write('[LOG] start creating engine \n')

connection_string = f"DRIVER={{{ODBC_DRIVER}}};SERVER={SERVER}, {PORT};DATABASE={DATABASE};UID={username};PWD={pwd}"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = sqlalchemy.create_engine(connection_url)
sys.stdout.write('[LOG] engine created \n')

JOBS_TABLE_NAME = "fct_JobPostingHistory"
LOG_TABLE_NAME = "fct_JobLog"
TABLE_NAME_FULL = "rpg.{table}"

now = datetime.now


def get_keywords():
    # return all keywords from table in database
    with engine.connect() as conn:
        keywords_set = pd.read_sql(
            '''SELECT TOP (1)
                    dim_SearchQuery.rpg_search_id
                    , dim_SearchQuery.search_query
                    , CASE WHEN fct_JobPostingHistory.rpg_search_id IS NULL THEN 125 ELSE 1 END AS age
                FROM rpg.dim_SearchQuery AS dim_SearchQuery
                LEFT JOIN (
                    SELECT
                        DISTINCT rpg_search_id
                    FROM rpg.fct_JobPostingHistory
                ) AS fct_JobPostingHistory ON fct_JobPostingHistory.rpg_search_id = dim_SearchQuery.rpg_search_id''',
            con=conn
        )
    return keywords_set.to_dict(orient='records')


def extract_jobs(soup, keyword, search_id, loc):
    # exract jobs from soup
    # array of jobs
    jobs_page_all = []
    # check if next page
    next_url = ''
    # Look for errors
    if soup.find('div', {'id': 'increased_radius_result'}) or \
       soup.find('div', {'class': 'bad_query'}) or \
       soup.find('div', {'id': 'suggested_queries'}) or \
       soup.find('div', {'class': 'no_results'}) or \
       soup.find('div', {'id': 'search_suggestions'} or
       soup.find('td', {'id': 'resultsCol'}) is None):
        sys.stdout.write("[ERROR] Denied access by website")
        return jobs_page_all, False
    # Get pagination
    page_navigation = soup.find_all("nav")

    if len(page_navigation) > 0:
        nl_check = page_navigation[-1].find('a', {'aria-label': 'Volgende'})
        en_check = page_navigation[-1].find('a', {'aria-label': 'Next Page'})
        if nl_check:
            next_url = INDEED_BASE_URL + nl_check['href']
        if en_check:
            next_url = INDEED_BASE_URL + en_check['href']
    # parse jobs
    jobs_page = soup.find_all("ul", attrs={"class": "jobsearch-ResultsList"})
    if len(jobs_page) > 0:
        for job in jobs_page[-1]:
            if job.find("div", attrs={"class": "mosaic-zone"}):
                continue
            jobs_page_all.append(parse_job(job, keyword, search_id, loc))
    return jobs_page_all, next_url


def parse_job(job, keyword, search_id, loc):
    # parse jobs from html block
    job_title = job.find('h2', attrs={'class': 'jobTitle'})
    company_name = job.find('span', attrs={'class': 'companyName'})
    company_url = company_name.find('a', attrs={'class': 'companyOverviewLink'}) if company_name else None
    company_location = job.find('div', attrs={'class': 'companyLocation'})
    job_url = job.find('a', attrs={'class': 'jcs-JobTitle'})
    job_code = job.find('a', attrs= {'class': 'jcs-JobTitle'})
    job_summary = job.find('div', attrs={'class': 'job-snippet'})
    job_date = job.find('span', attrs= {'class': 'date'})
    if job_title:
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


def get_create_script(table):
    # return the 'create script' for a specific table
    sql_create_log_table = """
    CREATE TABLE {table} (
        time_start time(0) NULL,
        time_end time(0) NULL,
        date_run date NULL
    ) 
    """
    sql_create_job_table = """
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
    if table == "fct_JobPostingHistory":
        return sql_create_job_table
    elif table == "fct_JobLog":
        return sql_create_log_table
    else:
        sys.stdout.write("[ERROR] Unknown table")
        return


def get_soup(url):
    # get the html conten of a url
    conn = http.client.HTTPSConnection("api.webscrapingapi.com")

    try_count = 0
    page_html = ''
    sys.stdout.write(f"[LOG] url to scrape: {url} \n")
    html_decoded_url = html.unescape(url)
    decoded_url = urllib.parse.unquote_plus(html_decoded_url)
    encoded_url = urllib.parse.quote_plus(decoded_url)
    sys.stdout.write(f"[LOG] Encoded url to scrape: {encoded_url} \n\n")
    conn.request("GET",
                 f"/v1?url={encoded_url}&api_key={api_key}&device=desktop&proxy_type=datacenter&render_js=1&wait_until=domcontentloaded&wait_for=5000")

    res = conn.getresponse()
    data = res.read()
    page_html = data.decode("utf-8")

    soup = bs.BeautifulSoup(page_html, features="html.parser")
    return soup


def get_amount_results(query_url):
    # get the amount of results of a particular utility function
    soup = get_soup(query_url)
    block = soup.find("div", attrs={'class': 'jobsearch-JobCountAndSortPane-jobCount'})
    # scrape amount of results
    if block:
        amount_str = re.sub("[^0-9]", "", block.find("span").text)
    else:
        return False
    amount = int(amount_str)
    return amount > 1000


def prep_table():
    # Create table if it doesnt exist yet
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="rpg")

    tables_to_create = [JOBS_TABLE_NAME, LOG_TABLE_NAME]

    with engine.begin() as conn:
        for table in tables_to_create:
            if table not in tables:
                sys.stdout.write(f"[LOG] schema {table} is being created")
                conn.execute(text(get_create_script(table).format(table=TABLE_NAME_FULL.format(table=table))))


def create_requests():
    # return indeed search URL filled with fornatted search query
    urls = []
    keywords = get_keywords()
    for keyword in keywords:
        urls.append(
            (keyword['search_query'],
             keyword['rpg_search_id'],
             f"{INDEED_BASE_URL}/jobs?q={keyword['search_query']}&fromage={keyword['age']}&sort=date")
        )
    return urls


def get_sources(query_url, keyword, search_id, loc=""):
    # get pages soup from webscraping and extract jobs from them
    sys.stdout.write(f"[LOG] starts scraping {query_url}... \n")
    count = 0
    job_info = []
    jobs_pages = []
    while len(query_url) != 0:
        # get html page for a single page for a keyword
        soup = get_soup(query_url)
        # extract info from job
        (job_info, query_url) = (extract_jobs(soup, keyword, search_id, loc))
        # add info to array
        jobs_pages.append(job_info)
        count += 1
    flat_array = np.array(jobs_pages, dtype=object)
    sys.stdout.write(f"[LOG] finished scraping {count} page(s)) for {keyword}, added {(15*(len(flat_array)-1))+(len(job_info))} job(s) \n")
    return flat_array


def load_jobs(jobskeyword=None):
    # load all jobs related to one keyword to an array
    sql_insert = """
    INSERT INTO {table}
    (rpg_search_id, search_query, search_location, job_code, job_title, job_summary, job_url, company_name, company_location, company_url, date_job, date_scrape)
    VALUES({rpg_search_id}, '{search_query}', '{search_location}', '{job_code}', '{job_title}', '{job_summary}', '{job_url}', '{company_name}', '{company_location}',' {company_url}', '{date_job}', '{date_scrape}');
    """
    if jobskeyword is None:
        return False
    with engine.begin() as conn:
        for jobs in jobskeyword:
            for job in jobs:
                conn.execute(text(sql_insert.format(
                    table=TABLE_NAME_FULL.format(table=JOBS_TABLE_NAME),
                    rpg_search_id=job["rpg_search_id"],
                    search_query=job["search_query"],
                    search_location=job["search_location"],
                    job_code=job["job_code"],
                    job_title=job["job_title"],
                    job_summary=job["job_summary"],
                    job_url=job["job_url"],
                    company_name=job["company_name"],
                    company_location=job["company_location"],
                    company_url=job["company_url"],
                    date_job=job["date_job"],
                    date_scrape=job["date_scrape"],
                )))


def load_log(start, end, date):
    # load start, end time and date into schema
    sql_log_insert = """
    INSERT INTO {table}
    (time_start, time_end, date_run)
    VALUES('{start}', '{end}', '{date}');
    """

    with engine.begin() as conn:
        conn.execute(text(sql_log_insert.format(
            table=TABLE_NAME_FULL.format(table=LOG_TABLE_NAME),
            start=start,
            end=end,
            date=date,
        )))


def brain():
    sys.stdout.write('[LOG] brain turns on \n')
    start = now(timezone.utc).strftime("%H:%M:%S")

    locations = ['Zuid-Holland', 'Noord-Holland', 'Groningen (provincie)', 'Friesland', 'Drenthe', 'Zeeland',
                 'Overijssel', 'Flevoland', 'Gelderland', 'Utrecht (provincie)', 'Noord-Brabant', 'Limburg']

    prep_table()
    # create URL query request for job posting
    source_urls = create_requests()
    # For each keyword, extract, parse and store jobs
    for (keyword, search_id, url) in source_urls:
        lot_of_results = get_amount_results(url)
        clusters = locations if lot_of_results else [""]
        for location in clusters:
            alljobskeyword = get_sources(url, keyword, search_id, loc=location)
            load_jobs(alljobskeyword)
    end = now(timezone.utc).strftime("%H:%M:%S")
    load_log(start=start, end=end, date=now().strftime("%m/%d/%Y"))
    sys.stdout.write('[LOG] successfull  \n')


brain()
