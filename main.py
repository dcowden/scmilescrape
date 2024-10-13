import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import polars as pl
import logging
import os

logger = logging.getLogger(__name__)

def list_meets():
    r = requests.get('https://sc.milesplit.com/results')
    soup = BeautifulSoup(r.content,'html.parser')
    t = soup.find("table",class_='results')
    all_a = t.find_all('a')
    all_meets = []
    for a in all_a:
        url = a['href']
        if url.endswith('results'):
            all_meets.append(url)
        else:
            logger.warning(f"SKipping url {url}, doesnt look like race results")
    return all_meets


def list_races_at_meet(meet_url):
    logger.info(f"List Races for meet: {meet_url}")
    r = requests.get(meet_url)
    soup = BeautifulSoup(r.content,'html.parser')
    events = soup.find(id='resultFileList').find_all('a')
    r = [ a['href'] for a in events]
    logger.info(f"Found {len(r)} races.")
    logger.debug(f"Races: {r}")
    return r

def scrape_formatted_race(race_url) -> ( pl.DataFrame, pl.DataFrame):

    raw_race_url  = os.path.dirname(race_url.rstrip("/")) + "/raw"
    logger.info(f"Loading Race Url: {raw_race_url}")
    r = requests.get(raw_race_url)
    soup = BeautifulSoup(r.content,'html.parser')
    data = soup.find(id='meetResultsBody').find('pre').contents

    data_lines = data[0].splitlines()
    logger.info(f"Data has {len(data_lines)} lines ")

    #find row dividers
    divider_rows =[]
    for idx,line in enumerate(data_lines):
        if line.startswith("========================"):
            divider_rows.append(idx)
    logger.info(f"Divider Rows:{divider_rows}")

    TOP_TAIL_ROWS_TO_IGNORE=10 # i think this is constant
    race_name = data_lines[ divider_rows[0]-1]
    logger.info(f"Printing result Data for Event '{race_name}'")

    top_lines = [data_lines[ divider_rows[0]+ 1] ]
    top_lines.extend(data_lines[ divider_rows[1] + 1: divider_rows[2] - TOP_TAIL_ROWS_TO_IGNORE]) # top data

    bottom_lines = [  data_lines[ divider_rows[2] + 1 ]]
    bottom_lines.extend( data_lines [ divider_rows[3]+1 : -1]) #remaining rows

    top_section = "\n".join(top_lines)
    bottom_section = "\n".join(bottom_lines)

    tr = pl.DataFrame(pd.read_fwf(StringIO(top_section),infer_nrows=min(120,len(top_lines)))).with_columns(
        pl.lit(race_name).alias("race")
    )
    ir = pl.DataFrame(pd.read_fwf(StringIO(bottom_section),infer_nrows=min(120,len(bottom_lines)))).with_columns(
        pl.lit(race_name).alias("race")
    )
    logger.info(f"Race {race_name}, TeamResults shape {tr.shape} IndividualResults: shape{ir.shape}")
    return tr,ir

def crawl_meet_list(meet_url_list: list[str])-> ( pl.DataFrame, pl.DataFrame):

    all_team_results = []
    all_individual_results = []
    meet_urls = []
    for meet_url in meet_url_list:
        logger.info(f"Crawling Meet {meet_url}")
        try:
            tr,ir = crawl_meet(meet_url)
            print ( ir)
            print ( tr)
            if tr is not None and ir is not None:
                all_team_results.append(tr)
                all_individual_results.append(ir)
                meet_urls.append(meet_url)
            logger.info(f"Loaded {meet_url}: {len(tr)} Races Found.")
        except Exception as e:
            logger.warning(f"Error Crawling meet {meet_url}, run in debug mode for trace")
            logger.debug(e)

    return pl.concat(all_team_results), pl.concat(all_individual_results)

def crawl_meet(meet_url) -> ( pl.DataFrame, pl.DataFrame):
    meet_name = meet_url.split('/')[4]

    logger.info(f"Loading Races at Meet {meet_name},  meet_url={meet_url}")
    races = list_races_at_meet(meet_url)

    all_team_results = []
    all_individual_results = []

    for race in list_races_at_meet(meet_url):
        logger.info(f"Loading Race Results for: {race}")
        WEIRD_EXTRA_COL_NAME="Unnamed: 2"
        try:
            (tr,ir)= scrape_formatted_race(race)
            if WEIRD_EXTRA_COL_NAME in tr.columns:
                tr = tr.drop(WEIRD_EXTRA_COL_NAME)
            if WEIRD_EXTRA_COL_NAME in ir.columns:
                ir = ir.drop(WEIRD_EXTRA_COL_NAME)

            all_team_results.append(
                tr.with_columns(
                    pl.lit(meet_name).alias('meet'),
                    pl.col('Rank').cast(pl.Int64),
                    pl.col('Score').cast(pl.Int64),
                    pl.col('1').cast(pl.Int64),
                    pl.col('2').cast(pl.Int64),
                    pl.col('3').cast(pl.Int64),
                    pl.col('4').cast(pl.Int64),
                    pl.col('5').cast(pl.Int64),
                    pl.col('6').cast(pl.Int64),
                    pl.col('7').cast(pl.Int64),
                )
            )
            all_individual_results.append(
                ir.with_columns(
                    pl.lit(meet_name).alias('meet'),
                    pl.col('Pl').cast(pl.Int64),
                    pl.col('#').cast(pl.Int64),
                    pl.col('Score').cast(pl.Int64)
                )
            )
            logger.info(f"Loaced Race. Team Results={len(tr)}, Individual Results={len(ir)}")
            logger.debug(tr)
            logger.debug(ir)
            return pl.concat(all_team_results), pl.concat(all_individual_results)
        except Exception as e:
            logger.warning(f"Could not load Results for Meet {meet_name}, race={race} ")
            logger.info(e)
            return None,None



def setup_logging():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s',
                        handlers=[stream_handler])


if __name__ == '__main__':
    setup_logging()
    all_meets = list(set(list_meets()))
    print("\n".join(all_meets))
    #(tr, ir ) = crawl_meet_list(all_meets)
    (tr, ir) = crawl_meet(all_meets[0])
    #races = list_races_at_meet(all_meets[1])
    #print(races)
    with pl.Config(set_tbl_cols=14,set_tbl_rows=20):
        print(ir)
        print(tr)
