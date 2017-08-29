import scrapy
import logging
import os

class GovtDataSpider(scrapy.Spider):
    name = "govt_data"

    output_dir = "/f/datasets/govt_data"

    url_to_filepath_dictionary = {}

    def start_requests(self):

        start_urls = ['https://catalogue.data.govt.nz']
        urls = [
            'https://catalogue.data.govt.nz/group/health',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        num_results = response.css('#group-datasets-search-form h2::text').extract_first().strip().split(" ")[0]

        logging.log(logging.INFO, num_results)

        on_page_files = response.css(".dataset-list.unstyled li")

        logging.log(logging.INFO, 'About to try the OPFs')

        for opf in on_page_files:

            opf_url = response.urljoin(opf.css('a::attr(href)').extract()[0])

            logging.log(logging.INFO, 'Going into %s' % opf_url)

            yield scrapy.Request(opf_url, callback=self.file_parse)

        next_page = response.css('.pagination.pagination-centered li.active + li>a::attr(href)').extract_first()
        if next_page is not None:
            next_url = response.urljoin(next_page)
            yield scrapy.Request(next_url)


        logging.log(logging.INFO,'Saved file %s' % num_results)

    def file_parse(self, response):
        logging.log(logging.INFO, 'URL is %s' % response.url)
        
        title = response.css('.primary h1::text').extract_first().strip()
        title = self.dirname_safe(title)

        files_list = response.css('.resource-url-analytics::attr(href)').extract()

        logging.log(logging.INFO, files_list)

        for li in files_list:
            li_url = response.urljoin(li)
            
            self.url_to_filepath_dictionary[li] = title

            yield scrapy.Request(li_url, callback=self.save_file, meta={ 'title':title})


        logging.log(logging.INFO, 'SUCCESS for page %s' % title)

    def save_file(self, response):

        title_path = self.url_to_filepath_dictionary[response.url]
        
        logging.log(logging.INFO, 'SAVING A FILE FOR %s' % title_path)

        file_dir = os.path.join(self.output_dir, title_path)

        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        
        path = os.path.join(file_dir, response.url.split('/')[-1])

        self.logger.info('Saving file %s', path)
        with open(path, 'wb') as f:
            f.write(response.body)


    def dirname_safe(self, s, keepcharacters = (' ','.','_')):
        logging.log(logging.INFO, 'sanifying %s' % s)        
        return "".join(c for c in s if c.isalnum() or c in keepcharacters).rstrip()
