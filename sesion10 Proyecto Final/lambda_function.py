import json
import base64
import boto3

def lambda_handler(event, context):
    
    comprehend = boto3.client("comprehend")
    output = []
    
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode("utf-8")
        response = comprehend.detect_sentiment(Text = payload, LanguageCode = "es")
        sentiment = response.get("Sentiment")
        json_object = {'sentiment': sentiment}

        output_record = {
       'recordId': record['recordId'],
       'result': 'Ok',
       'data': base64.b64encode(json.dumps(json_object).encode('utf-8')).decode('utf-8')
        }
    output.append(output_record)
    
    print(str(output))
    
  
    return {
        'records': output
    }
