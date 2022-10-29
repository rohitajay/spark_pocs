import pytz
from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.utils.project import get_project_settings
import logging
from datetime import datetime
import re
# Set logger level
logging.basicConfig(level=logging.DEBUG)


time_zone = {'EST': pytz.timezone('US/Eastern'),
    'UTC': pytz.timezone('UTC'),
    'iex_stock': pytz.timezone('US/Eastern'),
    'iex_forex': pytz.timezone('UTC')}

current_dt = datetime.now()

class EconomicIndicatorsSpiderSpider(Spider):

    name = 'economic_indicators_spider'
    allowed_domains = ['www.investing.com']
    start_urls = ['https://www.investing.com/economic-calendar/']

    def parse(self, response):

        events = response.xpath("//tr[contains(@id, 'eventRowId')]")
        for event in events:
            print("----------------->",event)
            # Extract event datetime in format: '2019/11/26 16:30:00' (EST)
            datetime_str = event.xpath(".//@data-event-datetime").extract_first()
            print("--------------->", datetime_str )
            if not datetime_str:
                continue

            event_datetime = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S")
            event_datetime = event_datetime.replace(tzinfo=time_zone['EST'])
            print('event_date', event_datetime)

            current_dt_str = datetime.strftime(current_dt, "%Y-%m-%d %H:%M:%S")
            print("--->",current_dt_str)
            # Return only events that passed

            # if not current_dt >= event_datetime:
            #     print("blah blah")
            #     continue      ## ------Issue 1
            print("lets get country")
            country = event.xpath(".//td/span/@title").extract_first()
            print("country--->", country )
            importance_label = event.xpath(".//td[@class='left textNum sentiment noWrap']/@data-img_key") \
                .extract_first()
            print("importance_label", importance_label)
            # if country not in self.countries or importance_label not in self.importance:
            #     continue
            #
            # if not importance_label:
            #     logging.warning("Empty importance label for: {} {}".format(country, datetime_str))
            #     continue

            event_name = event.xpath(".//td[@class='left event']/a/text()").extract_first()
            print('event_name1----.',event_name, 'abc')
            event_name = event_name.strip(' \r\n\t ')
            event_name_regex = re.findall(r"(.*?)(?=.\([a-zA-Z]{3}\))", event_name)

            if event_name_regex:
                event_name = event_name_regex[0].strip()

            if event_name not in self.event_list:
                continue

            actual = event.xpath(".//td[contains(@id, 'eventActual')]/text()").extract_first().strip('%M BK')

            previous = event.xpath(".//td[contains(@id, 'eventPrevious')]/span/text()").extract_first().strip('%M BK')

            forecast = event.xpath(".//td[contains(@id, 'eventForecast')]/text()").extract_first().strip('%M BK')
            print("forecast------->",forecast)
            if actual == '\xa0':
                print("Yes")
                continue

            previous_actual_diff = float(previous) - float(actual)
            print("previous_actual_diff--->",previous_actual_diff)

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


if __name__ == '__main__' :
    settings = get_project_settings()
    quote_crawler = Crawler(
        EconomicIndicatorsSpiderSpider,
        settings={
            **settings,
            "FEEDS": {
                "quotes.json": {"format": "json"},
            },
        },
    )
    print(quote_crawler)

    """------------->  scrapy runspider economic_indicator_spider.py"""