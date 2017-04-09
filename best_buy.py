import requests
import time
import json
import StringIO
import gzip
from bs4 import BeautifulSoup
import sys
import re
import glob
import csv
from Get_CC_data import get_cc_record
from requests.exceptions import ConnectionError

def get_bestbuy_files(month,folder_path):
    #get all files for the given month from the folder 
    all_files = glob.glob(folder_path+month+'-bestbuy*.gz')
    for each_file in all_files:
        print each_file
        #out_file = each_file[:-3]+'_processed_products.csv'
        product = {}

        with gzip.GzipFile(each_file,'r') as data_file:
            for line in data_file:
                line = data_file.readline()

                try:
                    record = json.loads(line)

                if record['url'].split('/')[3] == 'site': ##product page for Best Buy

                    try:
                        response = get_cc_record(record)
                        parser = BeautifulSoup(response, "lxml")
                    
                        if parser.find_all("div", class_="item-price") != []:
                            product_name = parser.find("title").renderContents()
                            product[product_name] = {
                            'url': record['url'],
                            'price': parser.find("div", class_="item-price").text.replace('\n', ''),
                            'timestamp': record['timestamp']
                            }
                        
                        for line in re.findall('{(.*?)}', response):
                            if line[0:8] == 'upc_code':
                                try:
                                    category = (re.findall('(?<=category:)(.*?)(?=,)', line)[0]).strip()
                                    category = re.sub("\'", '', category)
                                    product[product_name]['category'] = category
                                except KeyError:
                                    continue
                                try:
                                    subcategory = (re.findall('(?<=subcategory:)(.*?)(?=,)', line)[0]).strip()
                                    subcategory = re.sub("\'", '', subcategory)
                                    product[product_name]['subcategory'] = subcategory
                                except KeyError:
                                    continue
                    except: 
                        next
                except:
                    next
        with open(each_file[:-3]+'_processed_products.csv','wb') as csv_file:
            writer = csv.writer(csv_file)
            attrs=[]
            for key,dict_list in product.items():
            #for key,value in dict_list.items():
            #attrs.append(dict_list[url])
                try:
                    writer.writerow([key,dict_list['url'],dict_list['price'],dict_list['timestamp'],dict_list['section'],dict_list['category']])
                except KeyError:
                    continue

if __name__ == "__main__":
    month_val = sys.argv[1]
    input_file_path = sys.argv[2]
    get_bestbuy_files(month_val,input_file_path)
