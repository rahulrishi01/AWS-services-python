import boto3
import logging
import config
from botocore.exceptions import ClientError


# create logger
logger = logging.getLogger('aws-services')
logger.setLevel(logging.ERROR)


class S3Connection:
    def __init__(self):
        self.access_key = config.aws_access_key
        self.secret_key = config.aws_secret_access_key

    def create_connection(self, aws_service):
        """
            Method to Creates a AWS S3 Connection
        :return: S3 Client
        """
        try:
            session = boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
            s3_client = session.client(aws_service)
            return s3_client
        except Exception as e:
            logger.error("Error in creating S3 Connection. Message {}".format(e))
            return None


def send_sqs_message(QueueName, msg_body):
    """

    :param sqs_queue_url: String URL of existing SQS queue
    :param msg_body: String message body
    :return: Dictionary containing information about the sent message. If
        error, returns None.
    """
    # Create SQS connection
    sqs_client = S3Connection.create_connection('sqs')
    sqs_queue_url = sqs_client.get_queue_url(
        QueueName=QueueName
        )['QueueUrl']
    try:
        # Send the SQS message
        logger.info("Sending msg to sqs {}".format(msg_body))
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageBody=msg_body)
    except ClientError as e:
        logging.error(e)
        return None
    logger.info(msg['MessageId'])
    return msg['MessageId']


def send_sqs_message_multiple_queue(sqs_queue_url_list, msg_body):
    """

    :param sqs_queue_url: SQS queue urls
    :param msg_body: String message body
    :return: N/A.
    """

    # Send the SQS message
    try:
        # Create SQS connection
        sqs_client = S3Connection.create_connection('sqs')
        for sqs_queue_url in sqs_queue_url_list:
            try:
                msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                              MessageBody=msg_body)
                logger.info("Message pushed to sqs", extra={"Queue": sqs_queue_url, "MessageId": msg['MessageId']})
            except ClientError as e:
                logging.error("Message not pushed to sqs queue.", extra={"Queue": sqs_queue_url, "sqs_message": msg_body, "type": "EXCEPTION", "Message": e})
    except Exception as e:
        logger.error("Some exception occured", extra={"Queue list": sqs_queue_url_list, "sqs_message": msg_body, "type": "Exception", "Message": e})


def recieve_message(sqs, queue_url):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    return response


def delete_message(sqs, queue_url, receipt_handle):
    sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )


def get_sqs_message(sqs_queue_url):
    # Create SQS connection
    sqs_client = S3Connection.create_connection('sqs')

    # Receive message from SQS queue
    response = recieve_message(sqs_client, sqs_queue_url)
    try:
        if response.get('Messages'):
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            # Delete received message from queue
            delete_message(sqs_client, sqs_queue_url, receipt_handle)
            return message
        else:
            return None
    except ClientError as e:
        return None
    except Exception as e:
        return None