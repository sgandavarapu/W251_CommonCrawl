import requests
import time
import json
import StringIO
import gzip
from bs4 import BeautifulSoup
import sys
import gzip
import re
import glob
import csv

def get_homedepot_files(month,folder_path):
    #get all files for the given month from the folder 
    all_files = glob.glob(folder_path+month+'-homedepot*.gz')
    for each_file in all_files:
        print each_file
        #out_file = each_file[:-3]+'_processed_products.csv'
        product = {}

        with gzip.GzipFile(each_file,'r') as data_file:
                for line in data_file:
                        line = data_file.readline()
                        record = json.loads(line)
            
                        if record['url'].split('/')[3] == 'p': ##product page for Home Depot

                            offset, length = int(record['offset']), int(record['length'])
                            offset_end = offset + length - 1

                            # We'll get the file via HTTPS so we don't need to worry about S3 credentials
                            # Getting the file on S3 is equivalent however - you can request a Range
                            prefix = 'https://commoncrawl.s3.amazonaws.com/'

                            # We can then use the Range header to ask for just this set of bytes
                            resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})

                            # The page is stored compressed (gzip) to save space
                            # We can extract it using the GZIP library
                            raw_data = StringIO.StringIO(resp.content)
                            f = gzip.GzipFile(fileobj=raw_data)

                            # What we have now is just the WARC response, formatted:
                            try :
                                data = f.read()
                            except:
                                resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})

                                # The page is stored compressed (gzip) to save space
                                # We can extract it using the GZIP library
                                raw_data = StringIO.StringIO(resp.content)
                                f = gzip.GzipFile(fileobj=raw_data)
                                data = f.read()

                            response = ""

                            if len(data):
                                try:
                                    warc, header, response = data.strip().split('\r\n\r\n', 2)
                                except:
                                    pass

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
    get_homedepot_files(month_val,input_file_path)

