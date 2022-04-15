import json
from json import JSONEncoder
import boto3
import email
import urllib
import numpy as np
from botocore.exceptions import ClientError
from botocore.vendored import requests
import io
import string
import sys
import numpy as np

from hashlib import md5

if sys.version_info < (3,):
    maketrans = string.maketrans
else:
    maketrans = str.maketrans
    
def vectorize_sequences(sequences, vocabulary_length):
    results = np.zeros((len(sequences), vocabulary_length))
    for i, sequence in enumerate(sequences):
       results[i, sequence] = 1. 
    return results

def one_hot_encode(messages, vocabulary_length):
    data = []
    for msg in messages:
        temp = one_hot(msg, vocabulary_length)
        data.append(temp)
    return data

def text_to_word_sequence(text,
                          filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
                          lower=True, split=" "):
    """Converts a text to a sequence of words (or tokens).
    # Arguments
        text: Input text (string).
        filters: list (or concatenation) of characters to filter out, such as
            punctuation. Default: `!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n`,
            includes basic punctuation, tabs, and newlines.
        lower: boolean. Whether to convert the input to lowercase.
        split: str. Separator for word splitting.
    # Returns
        A list of words (or tokens).
    """
    if lower:
        text = text.lower()

    if sys.version_info < (3,):
        if isinstance(text, unicode):
            translate_map = dict((ord(c), unicode(split)) for c in filters)
            text = text.translate(translate_map)
        elif len(split) == 1:
            translate_map = maketrans(filters, split * len(filters))
            text = text.translate(translate_map)
        else:
            for c in filters:
                text = text.replace(c, split)
    else:
        translate_dict = dict((c, split) for c in filters)
        translate_map = maketrans(translate_dict)
        text = text.translate(translate_map)

    seq = text.split(split)
    return [i for i in seq if i]

def one_hot(text, n,
            filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
            lower=True,
            split=' '):
    """One-hot encodes a text into a list of word indexes of size n.
    This is a wrapper to the `hashing_trick` function using `hash` as the
    hashing function; unicity of word to index mapping non-guaranteed.
    # Arguments
        text: Input text (string).
        n: int. Size of vocabulary.
        filters: list (or concatenation) of characters to filter out, such as
            punctuation. Default: `!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n`,
            includes basic punctuation, tabs, and newlines.
        lower: boolean. Whether to set the text to lowercase.
        split: str. Separator for word splitting.
    # Returns
        List of integers in [1, n]. Each integer encodes a word
        (unicity non-guaranteed).
    """
    return hashing_trick(text, n,
                         hash_function='md5',
                         filters=filters,
                         lower=lower,
                         split=split)


def hashing_trick(text, n,
                  hash_function=None,
                  filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
                  lower=True,
                  split=' '):
    """Converts a text to a sequence of indexes in a fixed-size hashing space.
    # Arguments
        text: Input text (string).
        n: Dimension of the hashing space.
        hash_function: defaults to python `hash` function, can be 'md5' or
            any function that takes in input a string and returns a int.
            Note that 'hash' is not a stable hashing function, so
            it is not consistent across different runs, while 'md5'
            is a stable hashing function.
        filters: list (or concatenation) of characters to filter out, such as
            punctuation. Default: `!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n`,
            includes basic punctuation, tabs, and newlines.
        lower: boolean. Whether to set the text to lowercase.
        split: str. Separator for word splitting.
    # Returns
        A list of integer word indices (unicity non-guaranteed).
    `0` is a reserved index that won't be assigned to any word.
    Two or more words may be assigned to the same index, due to possible
    collisions by the hashing function.
    The [probability](
        https://en.wikipedia.org/wiki/Birthday_problem#Probability_table)
    of a collision is in relation to the dimension of the hashing space and
    the number of distinct objects.
    """
    if hash_function is None:
        hash_function = hash
    elif hash_function == 'md5':
        hash_function = lambda w: int(md5(w.encode()).hexdigest(), 16)

    seq = text_to_word_sequence(text,
                                filters=filters,
                                lower=lower,
                                split=split)
    return [int(hash_function(w) % (n - 1) + 1) for w in seq]

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
