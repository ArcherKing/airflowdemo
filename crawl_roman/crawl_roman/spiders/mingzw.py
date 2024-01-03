import scrapy

from crawl_roman.items import CrawlRomanItem


class MingzwSpider(scrapy.Spider):
    name = "mingzw"
    allowed_domains = ["tw.mingzw.net"]
    start_urls = ["https://tw.mingzw.net"]

    def start_requests(self):
        url = self.start_urls[0]
        bookids = getattr(self, "bookids", None)
        print(bookids)
        if bookids is not None:
            for bookid in bookids.split(","):
                url = url + "/mzwbook/" + bookid
                yield scrapy.Request(url, self.parse, meta={"bookid": bookid})

    def parse(self, response):
        html_a_list = response.xpath('//*[@id="section-free"]//li/a')
        assert len(html_a_list) > 0
        latest_html_a = html_a_list[0]  # 取得最新一集

        crawl_roman_item = CrawlRomanItem()
        crawl_roman_item["bookid"] = response.meta["bookid"]
        crawl_roman_item["latest_info"] = {
            "href": latest_html_a.xpath(".//@href").get(),
            "title": latest_html_a.xpath(".//@title").get(),
            "text": latest_html_a.xpath(".//text()").get(),
        }
        yield crawl_roman_item
