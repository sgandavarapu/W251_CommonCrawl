from elasticsearch import Elasticsearch
import certifi
import json
import re
from datetime import datetime
import os

##credentials stored for elasticsearch
with open('credentials.txt', 'r')  as f: 
	username, pw, hostname = f.readlines()
    username = username.split('\n'[0])
    pw = pw.split('\n'[0])

es = Elasticsearch(
    [
        'https://%s:%s@%s' %(username, pw, hostname)
    ]
    ,verify_certs=True
    ,ca_certs=r'~\\cacert.pem' ##check with certifi.where()
)

## Assumes all target files loaded in /data
file_list = [file_name for file_name in os.listdir('./data') if '.json' in file_name]
os.chdir('./data')

## Create elasticsearch index
mapping = '''
{  
  "mappings":{  
    "log":{
        "properties" : {
            "retailer" : {"type": "string"},
            "product" : {"type": "string"},
            "category" : {"type": "string"},
            "subcategory": {"type": "string"},
            "url" : {"type": "string", "index" : "not_analyzed" },
            "timestamp" : { "type" : "date", "format":"yyyy-MM-dd" },
            "price" : { "type" : "float" }
           }
        }
      }
    }
  }
}'''

es.indices.create(index='cc-index-new', ignore=400, body=mapping)


## Example usage for best buy data
## Clean up scraped data prior to inserting to elasticsearch
## Each keys are product name
for file_name in file_list:
    with open(file_name, 'r') as f:
        data = json.load(f)

    for key in list(data.keys()):
        if '"' in key:
            new_key = re.sub('"', ' inches', key) ##convert double quotes to inches and re-insert data
            data[new_key] = data[key]
            del data[key]
            try: ## Check if price can be converted
                float(re.sub(',', '', data[new_key]['price'][1:])) ## will fail for #12.00/mo
            except:
                del data[new_key] ##if not delete
        else:
            try:
                float(re.sub(',', '', data[key]['price'][1:]))
            except:
                del data[key]

    for key, value in data.items(): ## clean-up prices and dates for elasticsearch
        value['price'] = float(re.sub(',', '', value['price'][1:]))
        value['timestamp'] = datetime.strptime(value['timestamp'][0:8], '%Y%m%d').strftime('%Y-%m-%d')    

## Start bulk insert    
    for product in data.keys():
        bulk_body = ''
        product_dict = data[product]
        if 'category' in product_dict.keys():
            bulk_body += '{ "index" : { "_index" : "best_buy", "_type" : "product", "_id" : %d } }\n' %index
            bulk_body += '{ "retailer": "best_buy", "product": "%s", "category": "%s", "subcategory": "%s", "url": "%s", "timestamp": "%s", "price": %.2f}\n'  \
                %(product, product_dict['category'], product_dict['subcategory'], product_dict['url'], product_dict['timestamp'], product_dict['price'])
        else:
            next
        index += 1
        try: ##insert one at a time
            es.bulk(index='cc-index-new', body=bulk_body) 
        except:
            next