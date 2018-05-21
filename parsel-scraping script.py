from collections import OrderedDict
from multiprocessing import Pool
from io import open
import requests
import parsel
import time
import csv


def get_page(url, retries=5, retry_wait=10):
    print('Retrieving {}'.format(url))
    for _ in range(retries):
        try:
            return parsel.Selector(
                text=requests.get(
                    url=url,
                    headers={
                        'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                      "Chrome/66.0.3359.181 Safari/537.36"
                    }
                ).text
            )
        except Exception as e:
            print('Failed to get the page due to error [{}], waiting and retrying...'.format(e))
            time.sleep(retry_wait)
    return None


def process_watch_page(page):
    if page is None:
        return None
    data = OrderedDict()
    data['brand'] = page.xpath('//*[@class="product-form"]/h1/text()').extract_first()
    data['model'] = page.xpath('//*[@class="product-form"]/h2/text()').extract_first()
    data['image'] = page.xpath('//*[@class="slides"]/li[1]/img/@src').extract_first(str()).strip('/')
    data['price'] = page.xpath('//*[@class="geolizr-currency"]/@data-geolizr-price').extract_first()
    data['url'] = page.xpath('//*[@itemprop="url"]/@content').extract_first()
    if data['price']:
        data['price'] = data['price'][:-2] + '.' + data['price'][-2:]
    if data['image']:
        data['image'] = 'https://{}'.format(data['image'])
    # print(data)
    return data


def extract_data(url):
    return process_watch_page(get_page(url=url))


if __name__ == '__main__':
    base_url = 'https://watchesworld.co.uk'
    start_url = "https://watchesworld.co.uk/collections/all"
    batch_size = 5

    master_data = list()

    print('Process started...')
    while True:
        page = get_page(url=start_url)
        if page is None:
            time.sleep(10)
            continue

        next_page = page.xpath('//*[@class="pagination-next"]/a/@href').extract_first()

        watches = page.xpath('//*[@class="product-list-item"]/figure/a/@href').extract()

        for w in [watches[i:i+batch_size] for i in range(0, len(watches), batch_size)]:
            with Pool(batch_size) as p:
                ress = p.map(extract_data, [base_url + u for u in w])
            for d in ress:
                if d is not None:
                    master_data.append(d)
        if not next_page:
            break
        else:
            start_url = base_url + next_page

    # print(master_data)
    with open('output_{}.csv'.format(time.strftime('%d%m%Y_%H%M%S')), mode='w', newline='', encoding='utf-8', errors='ignore') as fp:
        writer = csv.DictWriter(fp, fieldnames=master_data[0].keys())
        writer.writeheader()
        writer.writerows(master_data)

    print('Completed...')
