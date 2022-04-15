import json
from json import JSONEncoder
import boto3
import email
import urllib
import numpy as np
from botocore.exceptions import ClientError
from botocore.vendored import requests
import io

from sms_encoder import one_hot_encode
from sms_encoder import vectorize_sequences

def send_email(sender, recipient, subject, body):
   # This address must be verified with Amazon SES.
   SENDER = sender
 
   # If your account is still in the sandbox, this address must be verified.
   RECIPIENT = recipient
 
   # Specify a configuration set. If you do not want to use a configuration
   # set, comment the following variable, and the 
   # ConfigurationSetName=CONFIGURATION_SET argument below.
   # CONFIGURATION_SET = "ConfigSet"
 
   # The AWS Region you're using for Amazon SES.
   AWS_REGION = "us-east-1"
 
   # The subject line for the email.
   SUBJECT = subject
 
   # The email body for recipients with non-HTML email clients.
   BODY_TEXT = body
             
   # The HTML body of the email.
   BODY_HTML = """<html> <head></head> <body> <h1>Amazon SES Test (SDK for Python)</h1> <p>This email was sent with <a href='https://aws.amazon.com/ses/'> Amazon SES </a> using <a href='https://aws.amazon.com/sdk-for-python/'> AWS SDK for Python (Boto)</a>.</p> </body> </html>"""            
 
   # The character encoding for the email.
   CHARSET = "UTF-8"
 
   # Create a new SES resource and specify a region.
   client = boto3.client('ses', region_name=AWS_REGION)
 
   # Try to send the email.
   try:
       #Provide the contents of the email.
       response = client.send_email(
           Destination={
               'ToAddresses': [
                   RECIPIENT,
               ],
           },
           Message={
               'Body': {
                   'Text': {
                       'Charset': CHARSET,
                       'Data': BODY_TEXT,
                   },
               },
               'Subject': {
                   'Charset': CHARSET,
                   'Data': SUBJECT,
               },
           },
           Source=SENDER,
           # If you are not using a configuration set, comment or delete the
           # following line
           # ConfigurationSetName=CONFIGURATION_SET,
        )
   except ClientError as e:
       return(e.response['Error']['Message'])
   
   return("Email sent! Message ID:" + response['MessageId'] )


def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client("s3")
    message = ""
    if event: 
        filename = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')
            
        # filename = "iatkgp2u8ol0ii0fag9mj59ilp7v6k9k6cuqv481"
        print(filename)
        # print("filename: ", filename)
        fileObj = s3.get_object(Bucket = "assignment3-kerem-nana-emails", Key=filename)
        date = fileObj['ResponseMetadata']['HTTPHeaders']['date']
        em = email.message_from_bytes(fileObj['Body'].read())
        subject = em["Subject"]
        
        for payload in em.get_payload():
            message = payload.get_payload()
            mylist = message.splitlines()
            while("" in mylist) :
                mylist.remove("")
            message = ' '.join(mylist)
            break
    
        print(message)
    
    ENDPOINT_NAME = "sms-spam-classifier-mxnet-2022-04-13-00-30-27-865"
    vocabulary_length = 9013
    runtime= boto3.client('runtime.sagemaker')
    # payload = ["This is your chance to CLAIM your FREE CAR! Send us $1000 to be part of the raffle and you can win yourself a FREE CAR. Text 224-714-8729 for any questions."]
    payload = [message]
    
    one_hot_test_messages = one_hot_encode(payload, vocabulary_length)
    encoded_test_messages = vectorize_sequences(one_hot_test_messages, vocabulary_length)
    payload_list = np.ndarray(shape=(1,9013), buffer=encoded_test_messages)
    payload = json.dumps(payload_list.tolist())
    
    response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                      ContentType='application/json',
                                      Body=payload)
                                      
     
    result = json.loads(response['Body'].read().decode())
    print(result)
    classification = "SPAM" if result["predicted_label"][0][0] > 0.5 else "HAM"
    probability = result["predicted_probability"][0][0] * 100
    
    
    email_body = "We received your email sent at {} with the subject {}.\n\n".format(date, subject)
    email_body += "Here is a 240 character sample of the email body: {} \n\n".format(message)
    email_body += "The email was categorized as {} with a {}% confidence.".format(classification, probability)
    
    print(send_email("kerem@keremnana.com","kg2900@columbia.edu","result", email_body))
        
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
