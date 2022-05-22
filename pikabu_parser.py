from bs4 import BeautifulSoup
from datetime import date, timedelta
from fake_useragent import UserAgent
from logger_service import logger
import pandas as pd
from random import random
import re
import requests
import string
import sys
import time

current_data_count = 0
all_story_count = 0
number_of_errors = 0
required_records_number = 12000 #100000

page_num = 54

from_date = date(2017, 1, 1)
until_date = date(2017, 12, 31)

def pikabu_parser():    
    start_time = time.time()
    try:
        parse_pages()
        logger.info(f"Parser finished the job. Added {current_data_count} stories. "
            f"Parsed {all_story_count} stories. Total time: "
            f"{str(timedelta(seconds=time.time() - start_time))}")
    except KeyboardInterrupt:
        logger.info(f"The parser was interrupted. Added {current_data_count} stories. "
            f"Parsed {all_story_count} stories. Total time: "
            f"{str(timedelta(seconds=time.time() - start_time))}")
        sys.exit()

def parse_pages():
    global from_date, until_date, page_num
    current_date = from_date
    while page_num <= 100:
        while current_date <= until_date: 
            parse_page(page_num, current_date)
            if current_data_count >= required_records_number:
                break
            current_date += timedelta(days = 1)
            time.sleep(0.1 + 0.1 * random())
        if current_data_count >= required_records_number:
            break
        page_num += 1
        current_date = from_date

def parse_page(page_num, current_date):
    page_url = get_search_page_url(page_num, current_date)
    logger.info(f"start parse page #{page_num}, stories date: {current_date}")
    write_page_data(page_url)

def get_search_page_url(page_num, stories_date):
    return f"https://pikabu.ru/search?n=2&st=3&d={get_d_param(stories_date)}&page={page_num}"

def get_d_param(stories_date):
    # d=5245 => 12.05.2022, d=5244 => 11.05.2022 ( 5245 - 5244 = 1 (day) )
    return 5245 + (stories_date - date(2022, 5, 11)).days

def write_page_data(page_url):
    global current_data_count, all_story_count, number_of_errors
    headers = {'User-Agent': UserAgent().random}
    try:
        page_response = requests.get(page_url, headers = headers, timeout=6)
    except:
        logger.info(f"Timeout on page \"{page_url}\". Sleep 1 minute.")
        time.sleep(60)
        return
    soup = BeautifulSoup(page_response.text, 'lxml')
    story_blocks = soup.find_all('article', class_='story')
    for story_block in story_blocks:
        try:
            if is_ad(story_block):
                logger.info(f"story {story_block.get('data-story-id')} is ad")
                continue
            story_data = get_story_data(story_block)
            all_story_count += 1
            if story_data['text_len'][0] >= 2000:
                pd.DataFrame(story_data).to_csv('db.csv', mode='a', index=False, header=False)
                current_data_count += 1
                logger.info(f"story {story_block.get('data-story-id')} added. "
                            f"Added {current_data_count} stories")
                if current_data_count >= required_records_number:
                    break
            else:
                logger.info(f"story {story_block.get('data-story-id')} is short")
        except:
            number_of_errors += 1
            logger.exception(f"exception #{number_of_errors}")
            pass

def is_ad(story_block):
    return story_block.get('data-author-id') == None or \
        story_block.get('data-author-name') == "pikabu.deals" or \
        story_block.find('time', class_='caption story__datetime hint') == None or \
        story_block.get('data-rating') == None
      

def get_story_data(story_block):
    story_text = get_text(story_block)
    return { 'story_id': [story_block.get('data-story-id')],
    'author_id': [story_block.get('data-author-id')],
    'text_len': [len(story_text)],
    'rating': [story_block.get('data-rating')],
    'comments_count': [story_block.get('data-comments')],
    'tags': [get_tags(story_block)],
    'story_datetime': [get_datetime(story_block)],
    'title': [get_title(story_block)],
    'text': [story_text] }

def get_tags(story_block):
    tag_blocks = story_block.find_all('a',  class_='tags__tag')
    return list(map(lambda x: x.text, tag_blocks))

def get_datetime(story_block):
    return story_block.find('time', class_='caption story__datetime hint').get('datetime')

def get_title(story_block):
    return filter_str(story_block.find('a', class_='story__title-link').text)

def get_text(story_block):
    story_block = story_block.find('div', class_='story-block story-block_type_text')
    if story_block == None:
        return ""
    
    return filter_str(story_block.text)

def filter_str(text):
    link_regex = re.compile("(https?:\/\/)(\s)*(www\.)?(\s)*((\w|\s|\-)+\.)*([\w\-\s]+\/)*([\w\-?\.]+)((\?)?[\w\s]*=\s*[\w\%&]*)*")
    punctuation_regex = re.compile('[%s]' % re.escape(string.punctuation))
    filtered = link_regex.sub('<some_link>', text)
    filtered = " ".join(punctuation_regex.sub(' ', filtered).split())
    return filtered

def main():
    pikabu_parser()

if __name__ == "__main__":
    main()