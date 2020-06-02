from troposphere import (
    And, Equals, Export, FindInMap, GetAtt, If,
    ImportValue, Join, Not, Output, Parameter, Ref, Sub,
    Template, Base64,
    Tags)
from troposphere.autoscaling import AutoScalingGroup, LaunchConfiguration
from troposphere.ec2 import SecurityGroup
from troposphere.ecr import Repository
from troposphere.iam import InstanceProfile, PolicyType, Role, Policy
from troposphere.policies import AutoScalingRollingUpdate, UpdatePolicy
from troposphere.batch import ComputeEnvironment, ComputeResources, LaunchTemplateSpecification, JobQueue, \
    ComputeEnvironmentOrder, JobDefinition, ContainerProperties, Environment, Timeout


def asg_tag(key, value):
    return {"Key": key, "Value": value, "PropagateAtLaunch": True}


t = Template()

t.set_description("Airflow Batch for Pan BBC Churn Prediction")

pipeline = t.add_parameter(Parameter(
    "Pipeline",
    Type="String",
    Default="Airflow-Pan-BBC-Churn"
))

image_name = t.add_parameter(Parameter(
    "ImageName",
    Type="String",
    Default="pan-bbc-churn-predictions-2"
))

image_tag = t.add_parameter(Parameter(
    "ImageTag",
    Type="String",
    Default="0.0.1"
))

environment = t.add_parameter(Parameter(
    "Environment",
    Type="String",
    AllowedValues=["int", "test", "live"]
))

memory = t.add_parameter(Parameter(
    "Memory",
    Type="Number",
    Default='512'
))

core_infrastructure_stack_name = t.add_parameter(Parameter(
    "CoreInfraStackName",
    Type="String",
    Default="core-infrastructure",
    Description="Core infrastructure stack name"
))

vpc_id = ImportValue(Sub("${CoreInfraStackName}-VpcId"))

private_subnets = [
    ImportValue(Sub("${CoreInfraStackName}-PrivateSubnet0")),
    ImportValue(Sub("${CoreInfraStackName}-PrivateSubnet1")),
    ImportValue(Sub("${CoreInfraStackName}-PrivateSubnet2")),
]

# ECR Repo
ecr_repo = t.add_resource(
    Repository(
        'ECRRepo',
        RepositoryName=Ref(image_name),
    )
)

# Batch
BatchServiceRole = t.add_resource(Role(
    'BatchServiceRole',
    RoleName=Sub('${Environment}-${Pipeline}-ServiceRole'),
    Path='/',
    Policies=[Policy(
        PolicyName='access_s3_buckets',
        PolicyDocument={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "VisualEditor0",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetAccessPoint",
                        "s3:PutAccountPublicAccessBlock",
                        "s3:GetAccountPublicAccessBlock",
                        "s3:ListAllMyBuckets",
                        "s3:ListAccessPoints",
                        "s3:ListJobs",
                        "s3:CreateJob",
                        "s3:HeadBucket"
                    ],
                    "Resource": "*"
                },
                {
                    "Sid": "VisualEditor1",
                    "Effect": "Allow",
                    "Action": "s3:*",
                    "Resource": [
                        Sub("arn:aws:s3:::${Environment}-insights-pan-bbc-churn-predictions"),
                        Sub("arn:aws:s3:::${Environment}-insights-pan-bbc-churn-predictions/*"),
                        Sub("arn:aws:s3:::${Environment}-airflow-temp-for-redshift"),
                        Sub("arn:aws:s3:::${Environment}-airflow-temp-for-redshift/*")
                    ]
                }
            ]
        }
    )],
    ManagedPolicyArns=[
        'arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole',
    ],
    AssumeRolePolicyDocument={'Statement': [{
        'Action': ['sts:AssumeRole'],
        'Effect': 'Allow',
        'Principal': {'Service': ['batch.amazonaws.com']}
    },
    {
        'Action': ['sts:AssumeRole'],
        'Effect': 'Allow',
        'Principal': {'Service': ['ecs-tasks.amazonaws.com']}
    }
    ]},
))

BatchInstanceRole = t.add_resource(Role(
    'InstanceRole',
    RoleName=Sub('${Environment}-${Pipeline}-InstanceRole'),
    Path='/',
    Policies=[],
    ManagedPolicyArns=[
        'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role',  # NOQA
    ],
    AssumeRolePolicyDocument={'Statement': [{
        'Action': ['sts:AssumeRole'],
        'Effect': 'Allow',
        'Principal': {'Service': ['ec2.amazonaws.com']}
    }]},
))

SpotRole = t.add_resource(Role(
    'SpotRole',
    RoleName=Sub('${Environment}-${Pipeline}-SpotRole'),
    Path='/',
    Policies=[],
    ManagedPolicyArns=[
        'arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole'
    ],
    AssumeRolePolicyDocument={'Statement': [{
        'Action': ['sts:AssumeRole'],
        'Effect': 'Allow',
        'Principal': {'Service': ['spotfleet.amazonaws.com']}
    }]},
))


BatchInstanceProfile = t.add_resource(InstanceProfile(
    'InstanceProfile',
    Path='/',
    Roles=[Ref(BatchInstanceRole)],
))

BatchSecurityGroup = t.add_resource(SecurityGroup(
    'BatchSecurityGroup',
    VpcId=vpc_id,
    GroupDescription='Enable access to Batch instances',
    Tags=Tags(Name=Sub('${Environment}-${Pipeline}--sg'))
))

BatchComputeEnvironment = t.add_resource(ComputeEnvironment(
    'ComputeEnvironment',
    Type='MANAGED',
    ServiceRole=Ref(BatchServiceRole),
    ComputeResources=ComputeResources(
        'ComputeResources',
        Type='SPOT',
        DesiredvCpus=0,
        MinvCpus=0,
        MaxvCpus=10,
        PlacementGroup="ExampleClusterGroup",
        InstanceTypes=['m4.large'],
        SpotIamFleetRole=Ref(SpotRole),
        InstanceRole=Ref(BatchInstanceProfile),
        SecurityGroupIds=[GetAtt(BatchSecurityGroup, 'GroupId')],
        Subnets=private_subnets,
        Tags=dict(
            Name='batch-compute-environment',
            Project=Ref(pipeline)
        )
    )
))

JobQueue = t.add_resource(JobQueue(
    'JobQueue',
    ComputeEnvironmentOrder=[
        ComputeEnvironmentOrder(
            ComputeEnvironment=Ref(BatchComputeEnvironment),
            Order=1
        ),
    ],
    Priority=1,
    State='ENABLED',
    JobQueueName=Sub('${Environment}-${Pipeline}--JobQueue')
))

BatchJobDefinition = t.add_resource(JobDefinition(
    'BatchJobDefinition',
    JobDefinitionName=Sub('${Environment}-${Pipeline}-Classification'),
    Timeout=Timeout(AttemptDurationSeconds=600),
    Type='container',
    ContainerProperties=ContainerProperties(
        Vcpus=1,
        Image=Join("", [Ref('AWS::AccountId'), '.dkr.ecr.', Ref('AWS::Region'), '.amazonaws.com/', Ref(ecr_repo), ':', Ref(image_tag)]),
        Memory=Ref(memory),
        Command=["usr/local/bin/run-pan-bbc-churn-predictions.sh"],
        JobRoleArn=GetAtt(BatchServiceRole, 'Arn'),
        Environment=[
            Environment(
                Name="ENV",
                Value=Ref(environment)
            )
        ]
    )
))


t.add_output([
    Output('BatchComputeEnvironment', Value=Ref(BatchComputeEnvironment)),
    Output('BatchSecurityGroup', Value=Ref(BatchSecurityGroup)),
    Output('ExampleJobQueue', Value=Ref(JobQueue))
])

import os
import sys
with open(os.path.join(os.path.dirname(sys.argv[0]), 'airflow_batch_stack.json'), 'w') as f:
    f.write(t.to_json())


