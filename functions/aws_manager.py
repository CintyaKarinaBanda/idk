import boto3
from config import AWS_CONFIG

def setup_aws():
    boto3.setup_default_session(region_name=AWS_CONFIG["REGION"])
    sts = boto3.client('sts')
    current_account = sts.get_caller_identity()['Account']
    
    try:
        creds = sts.assume_role(RoleArn=AWS_CONFIG["ROLE_ARN"], RoleSessionName=AWS_CONFIG["ROLE_SESSION_NAME"])['Credentials']
        boto3.setup_default_session(aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'], region_name=AWS_CONFIG["REGION"])
        accounts = [a for page in boto3.client('organizations').get_paginator('list_accounts').paginate() for a in page['Accounts']]
        return {a['Id']: a['Name'] for a in accounts if a['Status'] == 'ACTIVE'}
    except:
        return {current_account: f"Cuenta {current_account}"}