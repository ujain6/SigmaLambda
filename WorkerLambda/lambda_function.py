import json
import boto3

from urllib.request import urlopen
from link_finder import LinkFinder
import decimal
import time

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
        
        
def generateLinks(page_url):
    gathered_links = set()
    html_string = ''
    try:
        page_url = page_url[1:-1]
        response = urlopen(page_url)
        if 'text/html' in response.getheader('Content-Type'):
            html_bytes = response.read()
            html_string = html_bytes.decode("utf-8")
            finder = LinkFinder(page_url, page_url)
            finder.feed(html_string)
            gathered_links = finder.page_links()
    except Exception as e:
        print(str(e))
        
    return gathered_links



def exists(db, table, pk_name, pk_value):
    
   
    item = table.get_item(Key={pk_name: pk_value})
   
    return item
    
def lambda_handler(event, context):
    # TODO implement
    print('\n\n************* CODE STARTS EXCEC ******************\n\n')
    
    print('\n\n EVENT INPUT: \n')
    print('\n\n', event)
    print('\n\n ***************** END EVENT DUMP ********************\n\n')
    
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='corpus')
    # queue_visited = sqs.get_queue_by_name(QueueName='visited')
    
    dynamodb = boto3.resource('dynamodb')
    db = boto3.client('dynamodb')
    crawled_table = dynamodb.Table('visited')
    
    for record in event['Records']:
        
        body = record['body']
        print('\n\n***************** BODY OR PERCEIVED URL ********************\n\n')
        print(body)
        print('\n\n***************** BODY OR PERCEIVED URL END ********************\n\n')
        
        # exists(table, pk_name, pk_value):
        db_resp = exists(db, crawled_table, 'Link', body)
        
        
        try:
            print(str(db_resp['Item']))
        except KeyError:
            print('key error invocation')
            db_resp = None
        
        if db_resp==None:
            print('Enters as it did not find the required link in the database')
            millis = int(round(time.time() *100))
            crawled_table.put_item(
                 Item = {
                  'Link': body
                }    
            )
            gatherLinks = generateLinks(body)

            # manage deletion of the message here
            print('generated links into the queue')
            for link in gatherLinks:
                response = queue.send_message(MessageBody=json.dumps(link))
            
            print('\n\n************* CODE ENDS EXCEC ******************\n\n')
                
    
        
    
    
   
