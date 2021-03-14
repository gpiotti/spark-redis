import boto3
from app.models import Dataset
import datetime


def get_boto3_session():
    session = boto3.session.Session(
        aws_access_key_id="admin",
        aws_secret_access_key="admin1234",
    )
    return session


