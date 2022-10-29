from datetime import datetime

import pytz
import scrapy
from scrapy.crawler import Crawler, CrawlerProcess
from scrapy.utils.project import get_project_settings
# import pydispatch
from scrapy import signals

time_zone = {'EST': pytz.timezone('US/Eastern'),
    'UTC': pytz.timezone('UTC'),
    'iex_stock': pytz.timezone('US/Eastern'),
    'iex_forex': pytz.timezone('UTC')}


class QuoteItem(scrapy.Item):
    """Define the Item fields that will be scraped.

    Field() is used to specify the meta data for the Item field, such
    as the serializer. Most of the time we don't need to specify any
    meta data for the Item field.
    """

    author = scrapy.Field()
    quote = scrapy.Field()
    tags = scrapy.Field()


class EconomicIndicatorsSpiderSpider(scrapy.Spider):
    name = 'economic_indicators_spider'
    allowed_domains = ['www.investing.com']
    start_urls = ['https://www.investing.com/economic-calendar/']
    custom_settings = {
        'ITEM_PIPELINES': {
            'economic_indicators_spider.IndicatorCollectorPipeline': 100
        }
    }
    # start_urls = ["http://quotes.toscrape.com"]

    # @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     spider = super(QuotesSpider, cls).from_crawler(crawler, *args, **kwargs)
    #     crawler.signals.connect(spider.spider_closed, signals.spider_closed)
    #     return spider

    # def start_requests(self):
    #     for url in self.start_urls:
    #         # If a tag is provided, then only scrape quotes with the
    #         # specific tag.
    #         if tag := getattr(self, "tag", None):
    #             url = f"{url}/tag/{tag}"
    #             print(url)
    #         yield scrapy.Request(url, self.parse)

    def parse(self, response):
        events = response.xpath("//tr[contains(@id, 'eventRowId')]")
        for event in events:
            # Extract event datetime in format: '2019/11/26 16:30:00' (EST)
            datetime_str = event.xpath(".//@data-event-datetime").extract_first()

            if not datetime_str:
                continue

            event_datetime = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S")
            event_datetime = event_datetime.replace(tzinfo=time_zone['EST'])
            current_dt_str = datetime.strftime(self.current_dt, "%Y-%m-%d %H:%M:%S")
            if not self.current_dt >= event_datetime:
                continue

            country = event.xpath(".//td/span/@title").extract_first()

            importance_label = event.xpath(".//td[@class='left textNum sentiment noWrap']/@data-img_key") \
                .extract_first()

            if country not in self.countries or importance_label not in self.importance:
                continue

            if not importance_label:
                logging.warning("Empty importance label for: {} {}".format(country, datetime_str))
                continue

            event_name = event.xpath(".//td[@class='left event']/a/text()").extract_first()
            event_name = event_name.strip(' \r\n\t ')
            event_name_regex = re.findall(r"(.*?)(?=.\([a-zA-Z]{3}\))", event_name)
            if event_name_regex:
                event_name = event_name_regex[0].strip()

            if event_name not in self.event_list:
                continue

            actual = event.xpath(".//td[contains(@id, 'eventActual')]/text()").extract_first().strip('%M BK')

            previous = event.xpath(".//td[contains(@id, 'eventPrevious')]/span/text()").extract_first().strip('%M BK')

            forecast = event.xpath(".//td[contains(@id, 'eventForecast')]/text()").extract_first().strip('%M BK')

            if actual == '\xa0':
                continue

            previous_actual_diff = float(previous) - float(actual)

            if forecast != '\xa0':
                forecast_actual_diff = float(forecast) - float(actual)

            yield {'Timestamp': current_dt_str,
                   'Schedule_datetime': datetime_str,
                   'Event': event_name.replace(" ", "_"),
                   '{}'.format(event_name.replace(" ", "_")): {
                       'Actual': float(actual),
                       'Prev_actual_diff': previous_actual_diff,
                       'Forc_actual_diff': forecast_actual_diff if forecast != '\xa0' else None
                   }
                   }
        # // means to search in the scope of the whole document.
        # @class in square brackets means to select by class name of
        # the HTML element.
        # for quote_selector in response.xpath('//div[@class="quote"]'):
        #     # ./ means to search in the context of the current selector.
        #     # text() means to select the text of the HTML element.
        #     author = quote_selector.xpath(
        #         './span/small[@class="author"]/text()'
        #     ).get()
        #     # print(author)
        #     quote = quote_selector.xpath('./span[@class="text"]/text()').get()
        #     # Remove the special quotes.
        #     quote = quote.replace("“", "").replace("”", "")
        #     tags = quote_selector.xpath(
        #         './div[@class="tags"]/a[@class="tag"]/text()'
        #     ).getall()
        #
        #     quote_item = QuoteItem(author=author, quote=quote, tags=tags)
        #     yield quote_item

        # if next_page_url := response.xpath(
        #     '//li[@class="next"]/a/@href'
        # ).get():
        #     # With response.follow() we can scrape the next page with
        #     # the same parse function, recursively.
        #     yield response.follow(next_page_url, self.parse)




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