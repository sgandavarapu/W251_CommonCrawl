import time
import re
import json
import StringIO
import gzip
import glob
import csv
import sys
import requests
from bs4 import BeautifulSoup
from Get_CC_data import get_cc_record
from requests.exceptions import ConnectionError

def get_frys_files(month,folder_path):
    #get all files for the given month from the folder 
    all_files = glob.glob(folder_path+month+'-frys*.gz')
    for each_file in all_files:
        print each_file
        #out_file = each_file[:-3]+'_processed_products.csv'
        product = {}

        with gzip.GzipFile(each_file,'r') as data_file:
            for line in data_file:
                line = data_file.readline()
                record = json.loads(line)

                try:
                    response = get_cc_record(record)
                    parser = BeautifulSoup(response, "lxml")
                except ConnectionError as e: 
                    parser = 'no response'
                
                price_regex = re.findall('(?<=\$)(.*?)(?=\</label\>)', response)
                if len(price_regex) > 0:
                    product_name = parser.find("meta", property="og:title")['content']
                    price = price_regex[0]
                    try:
                        product[product_name] = {
                            'price': parser.find('input', {'id': 'ciItemPrice'}).get('value'),
                            'url': record['url'],
                            'timestamp': record['timestamp']
                            }
                    except (KeyError,AttributeError) as e:
                        continue

        with open(each_file[:-3]+'_processed_products.csv','wb') as csv_file:
            writer = csv.writer(csv_file)
            attrs=[]
            for key,dict_list in product.items():
                #for key,value in dict_list.items():
                #attrs.append(dict_list[url])
                try:
                    writer.writerow([key,dict_list['url'],dict_list['price'],dict_list['timestamp']])
                except KeyError:
                    continue
        with open(each_file[:-3]+'_processed_products.json', 'w') as f:
            json.dump(product, f, indent=2)

if __name__ == "__main__":
    month_val = sys.argv[1]
    input_file_path = sys.argv[2]
    get_frys_files(month_val,input_file_path)