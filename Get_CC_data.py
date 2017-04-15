import requests
import time
import json
import StringIO
import gzip

def get_cc_index(index_cc, domain):

    cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index_cc
    cc_url += "url=%s&matchType=domain&output=json" % domain
b
    response = requests.get(cc_url)

    if response.status_code == 200:
        record_list = []
        records = response.content.splitlines()

        for record in records:
            record_list.append(json.loads(record))

        return record_list

def get_cc_record(record):

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
        ## Trying a 2nd time
        resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
        raw_data = StringIO.StringIO(resp.content)
        f = gzip.GzipFile(fileobj=raw_data)
        data = f.read()

    response = ""

    if len(data):
        try:
            warc, header, response = data.strip().split('\r\n\r\n', 2)
        except:
            pass
    
    return response