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
    
def lambda_handler(event, context):
    # TODO implement
    
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='corpus')
 
    dynamodb = boto3.resource('dynamodb')
    db_work = dynamodb.Table('work')
    
    page_url = event['url']
    gatherLinks = generateLinks(page_url)
        
    # manage deletion of the message here
    for link in gatherLinks:
        response = queue.send_message(MessageBody=json.dumps(link))
        
    # write to the write db
    # for link in gatherLinks:
    #     response = db_work.put_item(
    #         Item={
    #             'Link': link
    #         }
    #     )
    return response
        
        
    
    
   
