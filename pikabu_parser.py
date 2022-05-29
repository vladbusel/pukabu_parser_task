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

from_date = date(2017, 1, 1)
until_date = date(2022, 5, 28)
required_records_number = 100000

class PikabuParser:
    """Service for parsing pages with listing posts from Pikabu"""
    def __init__(self, from_date, until_date, required_records_number):
        self.from_date = from_date
        self.until_date = until_date
        self.current_date = self.from_date
        self.page_num = 1
        self.required_records_number = required_records_number
        self.current_data_count = 0
        self.all_story_count = 0
        self.number_of_errors = 0

    def call(self):
        """The main function for calling parser"""
        start_time = time.time()
        try:
            self.parse_pages()
            logger.info(f"Parser finished the job. Added {self.current_data_count} stories. "
                f"Parsed {self.all_story_count} stories. Total time: "
                f"{str(timedelta(seconds=time.time() - start_time))}")
        except KeyboardInterrupt:
            logger.info(f"The parser was interrupted. Added {self.current_data_count} stories. "
                f"Parsed {self.all_story_count} stories. Total time: "
                f"{str(timedelta(seconds=time.time() - start_time))}")
            sys.exit()

    def parse_pages(self):
        """Parse the every 100 pages from date <from_date> to date <until_date> and
           write suitable posts to a file"""
        while self.page_num <= 100:
            while self.current_date <= self.until_date: 
                self.parse_page_and_write_data()
                if self.current_data_count >= self.required_records_number:
                    break
                self.current_date += timedelta(days = 1)
                time.sleep(0.1 + 0.1 * random())
            if self.current_data_count >= self.required_records_number:
                break
            self.page_num += 1
            self.current_date = self.from_date

    def parse_page_and_write_data(self):
        """"Parse page and write data to file"""
        page_url = self.get_search_page_url()
        logger.info(f"start parse page #{ self.page_num }, stories date: { self.current_date }")
        headers = { 'User-Agent': UserAgent().random }
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
                if self.is_ad(story_block):
                    logger.info(f"story {story_block.get('data-story-id')} is ad")
                    continue
                story_data = self.get_story_data(story_block)
                self.all_story_count += 1
                if story_data['text_len'][0] >= 2000:
                    pd.DataFrame(story_data).to_csv('db.csv', mode='a', index=False, header=False)
                    self.current_data_count += 1
                    logger.info(f"story {story_block.get('data-story-id')} added. "
                                f"Added {self.current_data_count} stories")
                    if self.current_data_count >= self.required_records_number:
                        break
                else:
                    logger.info(f"story {story_block.get('data-story-id')} is short")
            except:
                self.number_of_errors += 1
                logger.exception(f"exception #{self.number_of_errors}")
                pass

    def get_search_page_url(self):
        """Get the url of the page with posts. 
           Additional parameters passed: day, page number,  posts type is text, sort by relevance"""
        return f"https://pikabu.ru/search?n=2&st=3&d={self.get_day_param(self.current_date)}&page={self.page_num}"

    def get_day_param(self, stories_date):
        """Calculate the parameter <d> (day) responsible for the date of posts"""
        # d=5245 => 12.05.2022, d=5244 => 11.05.2022 ( 5245 - 5244 = 1 (day) )
        return 5245 + (stories_date - date(2022, 5, 11)).days

    def is_ad(self, story_block):
        """Determine the advertising block or not"""
        return story_block.get('data-author-id') == None or \
            story_block.get('data-author-name') == "pikabu.deals" or \
            story_block.find('time', class_='caption story__datetime hint') == None or \
            story_block.get('data-rating') == None

    def get_story_data(self, story_block):
        """Return dictionary with post info"""
        story_text = self.get_text(story_block)
        return { 'story_id': [story_block.get('data-story-id')],
        'author_id': [story_block.get('data-author-id')],
        'text_len': [len(story_text)],
        'rating': [story_block.get('data-rating')],
        'comments_count': [story_block.get('data-comments')],
        'tags': [self.get_tags(story_block)],
        'story_datetime': [self.get_datetime(story_block)],
        'title': [self.get_title(story_block)],
        'text': [story_text] }

    def get_tags(self, story_block):
        tag_blocks = story_block.find_all('a',  class_='tags__tag')
        return list(map(lambda x: x.text, tag_blocks))

    def get_datetime(self, story_block):
        return story_block.find('time', class_='caption story__datetime hint').get('datetime')

    def get_title(self, story_block):
        return self.filter_str(story_block.find('a', class_='story__title-link').text)

    def get_text(self, story_block):
        story_block = story_block.find('div', class_='story-block story-block_type_text')
        if story_block == None:
            return ""
        
        return self.filter_str(story_block.text)

    def filter_str(self, text):
        """Delete punctuation, extra whitespaces and links"""
        link_regex = re.compile("(https?:\/\/)(\s)*(www\.)?(\s)*((\w|\s|\-)+\.)*([\w\-\s]+\/)*([\w\-?\.]+)((\?)?[\w\s]*=\s*[\w\%&]*)*")
        punctuation_regex = re.compile('[%s]' % re.escape(string.punctuation))
        filtered = link_regex.sub('<some_link>', text)
        filtered = " ".join(punctuation_regex.sub(' ', filtered).split())
        return filtered

def main():
    PikabuParser(from_date, until_date, required_records_number).call()

if __name__ == "__main__":
    main()
