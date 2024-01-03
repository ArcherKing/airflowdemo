# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlRomanItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    bookid = scrapy.Field()
    latest_info = scrapy.Field()
