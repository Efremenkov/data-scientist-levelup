import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field
from scrapy.http.request import Request
from scrapy.selector import Selector
from itemloaders.processors import Join, MapCompose, TakeFirst, Identity
from scrapy.loader import ItemLoader


def to_float(value):
    return float(value.replace(',', '.')
                 .replace('\xa0', '')
                 .replace('\U00002013', '-')
                 .replace('k', '000'))


def comment_counter_to_int(value):
    if value != 'Комментировать':
        return int(value.replace('\xa0', ''))
    else:
        return 0


class HabrNews(Item):
    news_id = Field()
    title = Field()
    author = Field()
    author_karma = Field()
    author_rating = Field()
    author_specialization = Field()
    comments_counter = Field()
    hubs = Field()
    tags = Field()
    text = Field()


class HabrNewsLoader(ItemLoader):
    default_output_processor = TakeFirst()
    default_input_processor = MapCompose(lambda v: v.strip())
    tags_out = Identity()
    hubs_out = Identity()
    author_karma_in = MapCompose(to_float)
    author_rating_in = MapCompose(to_float)
    comments_counter_in = MapCompose(comment_counter_to_int)
    news_id_in = MapCompose(lambda v: int(v.strip().replace('post_', '').replace('\xa0', '')))
    text_out = Join()


class HabrNewsSpider(scrapy.Spider):
    name = 'habr_news'
    allowed_domains = ['habr.com']
    start_urls = ['https://habr.com/ru/news/']

    def parse(self, response):
        hxs = Selector(response)
        posts = hxs.xpath('//li[contains(@id, "post_")]')
        for post in posts:
            link = post.xpath('.//article/h2/a//@href').get()
            yield Request(link, callback=self.parse_news_item)

        next_page = hxs.xpath('//a[@id="next_page"]/@href').extract()
        if len(next_page) > 0:
            yield Request('https://habr.com' + next_page[0].strip(),
                          callback=self.parse)

    @staticmethod
    def parse_news_item(response):
        loader = HabrNewsLoader(HabrNews(), response)
        loader.add_xpath('news_id', '//article[contains(@id, "post_")]/@id')
        loader.add_xpath('title', '//span[@class="post__title-text"]/text()')
        loader.add_xpath('author', '//div[@class="user-info__links"]/a/text()')
        loader.add_xpath('author_specialization', '//div[@class="user-info__specialization"]/text()')
        loader.add_xpath('author_karma',
                         '//div[@class="user-info"]/div[@class="user-info__stats"]/div/div/a[1]/div[1]/text()')
        loader.add_xpath('author_rating',
                         '//div[@class="user-info"]/div[@class="user-info__stats"]/div/div/a[2]/div[1]/text()')
        loader.add_xpath('hubs', '//dl[@class="post__tags"][2]/dd/ul/li/a/text()')
        loader.add_xpath('tags', '//dl[@class="post__tags"][1]/dd/ul/li/a/text()')
        loader.add_xpath('comments_counter', '//*[@id="post-stats-comments-count"]/text()')
        loader.add_xpath('text', 'normalize-space(//*[@id="post-content-body"])')

        yield loader.load_item()


if __name__ == '__main__':
    process = CrawlerProcess(settings={
        "FEEDS": {
            "./../../habr_news.json": {"format": "json"},
            "./../../habr_news.csv": {"format": "csv"},
        },
        "LOG_LEVEL": 'ERROR'
    })
    process.crawl(HabrNewsSpider)
    process.start()
