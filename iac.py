import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
import configparser
import pandas as pd
from time import time, sleep

CONFIG_FILE = 'dwh.cfg'


# 1.0 - Load configuration
print('---\n1.0 - Load configuration')

config = configparser.ConfigParser()
config.read_file(open(CONFIG_FILE))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
REGION                 = config.get('AWS','REGION')

DWH_CLUSTER_TYPE       = config.get("IAC","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("IAC","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("IAC","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("IAC","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("IAC","DWH_DB")
DWH_DB_USER            = config.get("IAC","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("IAC","DWH_DB_PASSWORD")
DWH_PORT               = config.get("IAC","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("IAC", "DWH_IAM_ROLE_NAME")

JSON_PATH_BUCKET       = config.get("S3", "JSON_PATH_BUCKET")

# Configure AWS Environment Variables
os.environ['AWS_ACCESS_KEY_ID'] = KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

print('1.0 - Configuration loaded')

# 1.1 - Create AWS Clients
print('---\n1.1 - Create AWS Clients')

ec2 = boto3.resource('ec2')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
iam = boto3.client('iam')
redshift = boto3.client('redshift')

print('- AWS Clients created')

# 1.2 - Create IAM Role
print('---\n1.2 - Create AWS Clients')
try:
    print('- Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps({
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {
                    'Service': 
                    'redshift.amazonaws.com'
                }
            }],
            'Version': '2012-10-17'
        }),
        Description='Makes Redshift able to access S3 bucket (ReadOnly)'
    )
    print("- New IAM Role created.")
except Exception as e:
    print(e)

# 1.3 - Create IAM Role
print('---\n1.3 Attaching Policy')

attached = iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE_NAME, 
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)

print('- Policy attached')


# 2.0 - Create Redshift Cluster
print('---\n2.0 - Create Redshift Cluster')
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
try:
    response = redshift.create_cluster(
        # TODO: add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        VpcSecurityGroupIds=[
            
        ],

        # TODO: add parameters for identifiers & credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        # TODO: add parameter for role (to allow s3 access)
        IamRoles=[
            roleArn,
        ]
    )
except Exception as e:
    print(e)
    
print('- Redshift Cluster created.')

# 2.1 - Waiting Redshift Cluster to be available
print('---\n2.1 - Waiting Redshift Cluster to be available')
redshift = boto3.client('redshift')
cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

sec = 1
while cluster['ClusterStatus'] != "available":
    cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print("Waiting " + "." * i)
    sec += 1
    sys.stdout.write("\033[F") # Cursor up one line
    sleep(1) # Wait 1 sec.
    
# Get Cluster Endpoint and ARN
if cluster['ClusterStatus'] == "available":
    DWH_ENDPOINT = cluster['Endpoint']['Address']
    DWH_ROLE_ARN = cluster['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    print("2.1 - Redshift Cluster available! [Total time: {s} sec.]")

# 3.0 - Open an incoming TCP port to access the cluster ednpoint
print('---\n3.0 - Open an incoming TCP port to access the cluster ednpoint')
try:
    vpc = ec2.Vpc(id=cluster['VpcId'])
    sg = list(vpc.security_groups.filter(GroupNames=["default"]))[0]
    print(f"Authorizing {sg.group_name} :: {sg.id}")
    authorized = sg.authorize_ingress(
        GroupName=sg.group_name,  # TODO: fill out
        CidrIp= '0.0.0.0/0',  # TODO: fill out
        IpProtocol='TCP',  # TODO: fill out
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )

    print(f"{sg.group_name} authorized")
except Exception as e:
    print(e)

# 4.0 - Updating config
print('---\n4.0 - Uploading json_paths ')
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True

s3_client.create_bucket(
    ACL         = 'authenticated-read',
    CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'
    },
    Bucket      = JSON_PATH_BUCKET
)

upload_file('json_path/log_json_path.json', JSON_PATH_BUCKET, 'log_json_path.json')
upload_file('json_path/song_json_path.json', JSON_PATH_BUCKET, 'song_json_path.json')
print('- Json paths uploaded')

# 5.0 - Updating config
print('---\n5.0 - Updating config')

config.set('CLUSTER', 'HOST', DWH_ENDPOINT)
config.set('CLUSTER', 'DB_NAME', DWH_DB)
config.set('CLUSTER', 'DB_USER', DWH_DB_USER)
config.set('CLUSTER', 'DB_PASSWORD', DWH_DB_PASSWORD)
config.set('CLUSTER', 'DB_PORT', DWH_PORT)

config.set('IAM_ROLE', 'ARN', DWH_ROLE_ARN)

config.set('S3', 'LOG_JSONPATH', f"s3://{JSON_PATH_BUCKET}/log_json_path.json")
config.set('S3', 'SONG_JSONPATH', f"s3://{JSON_PATH_BUCKET}/song_json_path.json")

cfgfile = open(CONFIG_FILE,'w')
config.write(cfgfile, space_around_delimiters=False)  # use flag in case case you need to avoid white space.
cfgfile.close()
print('- Config updated.')