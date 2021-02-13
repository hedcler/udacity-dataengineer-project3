import os
import sys
import json
import boto3
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

os.environ['AWS_ACCESS_KEY_ID'] = KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

print('1.0 - Configuration loaded')

# 1.1 - Create AWS Clients
print('---\n1.1 - Create AWS Clients')

ec2 = boto3.resource('ec2', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
s3 = boto3.resource('s3', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

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
redshift = boto3.client('redshift', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
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
print('---\n4.0 - Updating config')

config.set('CLUSTER', 'HOST', DWH_ENDPOINT)
config.set('CLUSTER', 'DB_NAME', DWH_DB)
config.set('CLUSTER', 'DB_USER', DWH_DB_USER)
config.set('CLUSTER', 'DB_PASSWORD', DWH_DB_PASSWORD)
config.set('CLUSTER', 'DB_PORT', DWH_PORT)

config.set('IAM_ROLE', 'ARN', DWH_ROLE_ARN)

cfgfile = open(CONFIG_FILE,'w')
config.write(cfgfile, space_around_delimiters=False)  # use flag in case case you need to avoid white space.
cfgfile.close()