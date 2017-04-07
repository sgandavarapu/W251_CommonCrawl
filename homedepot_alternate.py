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

def get_homedepot_files(month,folder_path):
    #get all files for the given month from the folder 
    all_files = glob.glob(folder_path+month+'-homedepot*.gz')
    for each_file in all_files:
        print each_file
        out_file = each_file[:-3]+'_processed_products.csv'
        product = {}

        with gzip.GzipFile(each_file,'r') as data_file:
            for line in data_file:
                line = data_file.readline()
                record = json.loads(line)
        
                    if record['url'].split('/')[3] == 'p': ##product page for Home Depot

                        response = get_cc_record(record)

                        parser = BeautifulSoup(response, "lxml")
                        if parser.find('input', {'id': 'ciItemPrice'}) != None:
                            product_name = parser.find("title").renderContents()
                            product_name = re.sub(' - The Home Depot', '', product_name)
                            product[product_name] = {
                                'price': parser.find('input', {'id': 'ciItemPrice'}).get('value'),
                                'url': record['url'],
                                'timestamp': record['timestamp']
                                }
                            string = re.findall('(?<=\"bcEnsightenData\":)(.*?)(?=},)', response)[0]
                            #print string
                            if string is not None:
                                try:
                                    section = (re.findall('(?<=\"siteSection\":)(.*?)(?=,)', string)[0])
                                    category = re.findall('(?<=\"contentCategory\":)(.*?)(?=,)', string)[0]
                                    section = re.sub('\"', '', section)
                                    category = re.findall('(?<=\>)(.*?)(?=\")', category)[0]
                                    product[product_name]['section'] = section
                                    product[product_name]['category'] = category
                                except:
                                    pass
        with open(out_file,'wb') as csv_file:
            writer = csv.writer(csv_file)
            attrs=[]
            for product,dict_list in product.items():
                for key,value in dict_list.items():
                        attrs.append(dict_list[key])
                writer.writerow([product]+attrs)

if __name__ == "__main__":
    month_val = sys.argv[1]
    input_file_path = sys.argv[2]
    get_homedepot_files(month_val,input_file_path)
