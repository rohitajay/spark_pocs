import scrapy
from scrapy.crawler import Crawler, CrawlerProcess
from scrapy.utils.project import get_project_settings
# import pydispatch
from scrapy import signals

class QuoteItem(scrapy.Item):
    """Define the Item fields that will be scraped.

    Field() is used to specify the meta data for the Item field, such
    as the serializer. Most of the time we don't need to specify any
    meta data for the Item field.
    """

    author = scrapy.Field()
    quote = scrapy.Field()
    tags = scrapy.Field()


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["http://quotes.toscrape.com"]

    # @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     spider = super(QuotesSpider, cls).from_crawler(crawler, *args, **kwargs)
    #     crawler.signals.connect(spider.spider_closed, signals.spider_closed)
    #     return spider

    def start_requests(self):
        for url in self.start_urls:
            # If a tag is provided, then only scrape quotes with the
            # specific tag.
            if tag := getattr(self, "tag", None):
                url = f"{url}/tag/{tag}"
                print(url)
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        # // means to search in the scope of the whole document.
        # @class in square brackets means to select by class name of
        # the HTML element.
        for quote_selector in response.xpath('//div[@class="quote"]'):
            # ./ means to search in the context of the current selector.
            # text() means to select the text of the HTML element.
            author = quote_selector.xpath(
                './span/small[@class="author"]/text()'
            ).get()
            # print(author)
            quote = quote_selector.xpath('./span[@class="text"]/text()').get()
            # Remove the special quotes.
            quote = quote.replace("“", "").replace("”", "")
            tags = quote_selector.xpath(
                './div[@class="tags"]/a[@class="tag"]/text()'
            ).getall()

            quote_item = QuoteItem(author=author, quote=quote, tags=tags)
            yield quote_item

        if next_page_url := response.xpath(
            '//li[@class="next"]/a/@href'
        ).get():
            # With response.follow() we can scrape the next page with
            # the same parse function, recursively.
            yield response.follow(next_page_url, self.parse)




if __name__ == '__main__' :
    settings = get_project_settings()
    quote_crawler = Crawler(
        QuotesSpider,
        settings={
            **settings,
            "FEEDS": {
                "quotes.json": {"format": "json"},
            },
        },
    )
    print(quote_crawler)

"""To run the program,--->> scrapy runspider quotes_spider.py"""