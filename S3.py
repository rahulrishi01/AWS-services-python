import boto3
import logging
import config

# create logger
logger = logging.getLogger('aws-services')
logger.setLevel(logging.ERROR)


class S3Connection:
    def __init__(self):
        self.access_key = config.aws_access_key
        self.secret_key = config.aws_secret_key

    def create_s3_connection(self):
        """
            Method to Creates a AWS S3 Connection
        :return: S3 Client
        """
        try:
            session = boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
            s3_client = session.client('s3')
            return s3_client
        except Exception as e:
            logger.error("Error in creating S3 Connection. Message {}".format(e))
            return None

    def get_s3_file(self, bucket, subfolder, filename):
        """
            Get S3 Files
        :return:
        """
        try:
            if subfolder:
                filename = str(subfolder) + '/' + str(filename)
            s3_client = self.create_s3_connection()
            file_object = s3_client.get_object(Bucket=bucket, Key=filename)
            return file_object
        except Exception as e:
            logger.error("Error in fetching s3 file. Message: {}".format(e))
            return None

    def get_matching_s3_objects(self, bucket, prefix='', suffix=''):
        """
        Generate objects in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch objects whose key starts with
            this prefix (optional).
        :param suffix: Only fetch objects whose keys end with
            this suffix (optional).
        """
        s3 = self.create_s3_connection()
        kwargs = {'Bucket': bucket}

        # If the prefix is a single string (not a tuple of strings), we can
        # do the filtering directly in the S3 API.
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:

            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = s3.list_objects_v2(**kwargs)

            try:
                contents = resp['Contents']
            except KeyError:
                return

            for obj in contents:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield obj

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def get_matching_s3_keys(self, bucket, prefix='', suffix=''):
        """
        Generate the keys in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        for obj in self.get_matching_s3_objects(bucket, prefix, suffix):
            yield obj['Key']