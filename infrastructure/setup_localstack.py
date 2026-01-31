"""
AWS Infrastructure Setup for LocalStack.

Simulates the production AWS architecture locally:
- S3 buckets (raw + processed)
- SQS queues (processing + DLQ)
- SNS topics (notifications)
- EventBridge rules (scheduling)
- Lambda functions (extract, transform, load)

Usage:
    # Start LocalStack first
    docker-compose up -d
    
    # Setup infrastructure
    python -m infrastructure.setup_localstack
"""
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

# Try to load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# LocalStack configuration
LOCALSTACK_ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
AWS_REGION = "us-east-1"
AWS_ACCOUNT_ID = "000000000000"

# Resource names
S3_BUCKET = "gfn-data-lake"
SQS_EXTRACT_QUEUE = "gfn-extract-queue"
SQS_TRANSFORM_QUEUE = "gfn-transform-queue"
SQS_LOAD_QUEUE = "gfn-load-queue"
SQS_DLQ = "gfn-dlq"
SNS_NOTIFICATIONS = "gfn-pipeline-notifications"

# Lambda configuration
LAMBDA_FUNCTIONS = {
    "gfn-extract": {
        "handler": "lambda_handlers.handler_extract",
        "description": "Extract data from GFN API",
        "timeout": 300,
        "memory": 512,
    },
    "gfn-transform": {
        "handler": "lambda_handlers.handler_transform",
        "description": "Transform and validate raw data",
        "timeout": 300,
        "memory": 512,
    },
    "gfn-load": {
        "handler": "lambda_handlers.handler_load",
        "description": "Load processed data to Snowflake",
        "timeout": 300,
        "memory": 512,
    },
}


def get_client(service: str):
    """Get boto3 client configured for LocalStack."""
    return boto3.client(
        service,
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
    )


def setup_s3():
    """Create S3 bucket with folder structure."""
    s3 = get_client("s3")

    # Create bucket
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"✓ Created S3 bucket: {S3_BUCKET}")
    except s3.exceptions.BucketAlreadyExists:
        print(f"  S3 bucket already exists: {S3_BUCKET}")

    # Create folder structure (placeholder files - LocalStack 3.0 has bug with empty objects)
    folders = [
        "raw/carbon_footprint/.keep",
        "processed/carbon_footprint/.keep",
        "failed/.keep",
    ]
    for folder in folders:
        s3.put_object(Bucket=S3_BUCKET, Key=folder, Body=b"placeholder")
    print(f"✓ Created folder structure in {S3_BUCKET}")


def setup_sqs():
    """Create SQS queues with DLQ configuration."""
    sqs = get_client("sqs")

    # Create DLQ first
    dlq_response = sqs.create_queue(
        QueueName=SQS_DLQ,
        Attributes={
            "MessageRetentionPeriod": "1209600",  # 14 days
        },
    )
    dlq_arn = f"arn:aws:sqs:{AWS_REGION}:{AWS_ACCOUNT_ID}:{SQS_DLQ}"
    print(f"✓ Created DLQ: {SQS_DLQ}")

    # Redrive policy for main queues
    redrive_policy = json.dumps({
        "deadLetterTargetArn": dlq_arn,
        "maxReceiveCount": 3,
    })

    # Create processing queues
    queues = [SQS_EXTRACT_QUEUE, SQS_TRANSFORM_QUEUE, SQS_LOAD_QUEUE]
    for queue_name in queues:
        sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                "VisibilityTimeout": "300",  # 5 minutes
                "RedrivePolicy": redrive_policy,
            },
        )
        print(f"✓ Created queue: {queue_name}")


def setup_sns():
    """Create SNS topic for notifications."""
    sns = get_client("sns")

    response = sns.create_topic(Name=SNS_NOTIFICATIONS)
    topic_arn = response["TopicArn"]
    print(f"✓ Created SNS topic: {SNS_NOTIFICATIONS}")

    # Subscribe SQS DLQ to receive failure notifications
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:{AWS_REGION}:{AWS_ACCOUNT_ID}:{SQS_DLQ}",
    )
    print("✓ Subscribed DLQ to notifications")

    return topic_arn


def setup_eventbridge():
    """Create EventBridge rule for scheduled extraction."""
    events = get_client("events")

    # Daily extraction at 6 AM UTC
    rule_name = "gfn-daily-extraction"
    events.put_rule(
        Name=rule_name,
        ScheduleExpression="cron(0 6 * * ? *)",
        State="ENABLED",
        Description="Daily GFN carbon footprint extraction",
    )
    print(f"✓ Created EventBridge rule: {rule_name}")

    # Target: SQS extract queue
    events.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "extract-queue-target",
                "Arn": f"arn:aws:sqs:{AWS_REGION}:{AWS_ACCOUNT_ID}:{SQS_EXTRACT_QUEUE}",
                "Input": json.dumps({
                    "action": "extract",
                    "start_year": 2010,
                    "end_year": 2024,
                    "incremental": True,
                }),
            }
        ],
    )
    print("✓ Added SQS target to rule")


def setup_iam_role():
    """Create IAM role for Lambda execution."""
    iam = get_client("iam")

    role_name = "gfn-lambda-execution-role"

    # Trust policy allowing Lambda to assume the role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Execution role for GFN Lambda functions",
        )
        print(f"✓ Created IAM role: {role_name}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"  IAM role already exists: {role_name}")

    # Attach policies for S3, SQS, SNS, CloudWatch access
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket",
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}",
                    f"arn:aws:s3:::{S3_BUCKET}/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "sqs:SendMessage",
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                ],
                "Resource": f"arn:aws:sqs:{AWS_REGION}:{AWS_ACCOUNT_ID}:gfn-*",
            },
            {
                "Effect": "Allow",
                "Action": ["sns:Publish"],
                "Resource": f"arn:aws:sns:{AWS_REGION}:{AWS_ACCOUNT_ID}:{SNS_NOTIFICATIONS}",
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }

    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName="gfn-lambda-policy",
            PolicyDocument=json.dumps(policy_document),
        )
        print("✓ Attached policy to role")
    except Exception as e:
        print(f"  Policy attachment skipped: {e}")

    return f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{role_name}"


def create_lambda_package(include_dependencies: bool = True) -> str:
    """
    Create a deployment package (ZIP) for Lambda functions.
    
    Args:
        include_dependencies: If True, install and bundle pip dependencies.
                            This creates a larger package but works in isolated Lambda.
    
    Returns the path to the ZIP file.
    """
    project_root = Path(__file__).parent.parent

    # Create a temporary directory for the package
    temp_dir = tempfile.mkdtemp(prefix="lambda_package_")
    package_dir = os.path.join(temp_dir, "package")
    os.makedirs(package_dir)
    zip_path = os.path.join(temp_dir, "lambda_package.zip")

    print("  Packaging Lambda code...")

    # Install dependencies if requested
    if include_dependencies:
        print("  Installing dependencies (this may take a moment)...")
        # Core dependencies needed for Lambda
        # Note: DuckDB excluded as it requires Linux binaries; Lambda uses S3+Snowpipe instead
        dependencies = [
            "aiohttp",
            "boto3",
            "pydantic",
            "pydantic-settings",
            "python-dotenv",
        ]

        # Install to package directory
        result = subprocess.run(
            ["uv", "pip", "install", "--target", package_dir] + dependencies,
            capture_output=True,
            text=True,
            cwd=str(project_root),
        )

        if result.returncode != 0:
            print(f"  Warning: Some dependencies may have failed: {result.stderr[:200]}")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add lambda_handlers.py at root level
        handlers_path = project_root / "infrastructure" / "lambda_handlers.py"
        if handlers_path.exists():
            zf.write(handlers_path, "lambda_handlers.py")

        # Add src/gfn_pipeline module
        src_path = project_root / "src" / "gfn_pipeline"
        if src_path.exists():
            for file_path in src_path.rglob("*.py"):
                arcname = f"gfn_pipeline/{file_path.relative_to(src_path)}"
                zf.write(file_path, arcname)

        # Add __init__.py for gfn_pipeline if not exists
        if not (src_path / "__init__.py").exists():
            zf.writestr("gfn_pipeline/__init__.py", "")

        # Add installed dependencies
        if include_dependencies and os.path.exists(package_dir):
            for root, dirs, files in os.walk(package_dir):
                # Skip __pycache__ and dist-info directories
                dirs[:] = [d for d in dirs if d != "__pycache__" and not d.endswith(".dist-info")]
                for file in files:
                    if file.endswith(".pyc"):
                        continue
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, package_dir)
                    zf.write(file_path, arcname)

    package_size = os.path.getsize(zip_path)
    print(f"  Package created: {package_size / 1024 / 1024:.1f} MB")

    return zip_path


def setup_lambda_functions(role_arn: str):
    """Deploy Lambda functions to LocalStack."""
    lambda_client = get_client("lambda")

    # Create deployment package
    zip_path = create_lambda_package()

    try:
        with open(zip_path, "rb") as f:
            zip_content = f.read()

        for function_name, config in LAMBDA_FUNCTIONS.items():
            try:
                # Check if function exists
                try:
                    lambda_client.get_function(FunctionName=function_name)
                    # Update existing function
                    lambda_client.update_function_code(
                        FunctionName=function_name,
                        ZipFile=zip_content,
                    )
                    print(f"✓ Updated Lambda function: {function_name}")
                except lambda_client.exceptions.ResourceNotFoundException:
                    # Create new function
                    lambda_client.create_function(
                        FunctionName=function_name,
                        Runtime="python3.11",
                        Role=role_arn,
                        Handler=config["handler"],
                        Code={"ZipFile": zip_content},
                        Description=config["description"],
                        Timeout=config["timeout"],
                        MemorySize=config["memory"],
                        Environment={
                            "Variables": {
                                "S3_BUCKET": S3_BUCKET,
                                "AWS_ENDPOINT_URL": "http://host.docker.internal:4566",
                                "LOCALSTACK_HOSTNAME": "localhost",
                                "GFN_API_KEY": os.getenv("GFN_API_KEY", ""),
                            }
                        },
                    )
                    print(f"✓ Created Lambda function: {function_name}")
            except Exception as e:
                print(f"  Failed to deploy {function_name}: {e}")
    finally:
        # Cleanup temp directory
        shutil.rmtree(os.path.dirname(zip_path), ignore_errors=True)


def setup_lambda_triggers():
    """Configure Lambda triggers (SQS, S3 events)."""
    lambda_client = get_client("lambda")

    # Map queues to Lambda functions
    triggers = [
        (SQS_EXTRACT_QUEUE, "gfn-extract"),
        (SQS_TRANSFORM_QUEUE, "gfn-transform"),
        (SQS_LOAD_QUEUE, "gfn-load"),
    ]

    for queue_name, function_name in triggers:
        try:
            queue_arn = f"arn:aws:sqs:{AWS_REGION}:{AWS_ACCOUNT_ID}:{queue_name}"

            # Create event source mapping
            lambda_client.create_event_source_mapping(
                EventSourceArn=queue_arn,
                FunctionName=function_name,
                BatchSize=1,
                Enabled=True,
            )
            print(f"✓ Created trigger: {queue_name} -> {function_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  Trigger already exists: {queue_name} -> {function_name}")
            else:
                print(f"  Failed to create trigger for {function_name}: {e}")


def setup_step_functions():
    """Create Step Functions state machine for orchestration."""
    sfn = get_client("stepfunctions")

    # State machine definition
    definition = {
        "Comment": "GFN Pipeline Orchestration",
        "StartAt": "Extract",
        "States": {
            "Extract": {
                "Type": "Task",
                "Resource": f"arn:aws:lambda:{AWS_REGION}:{AWS_ACCOUNT_ID}:function:gfn-extract",
                "Next": "CheckExtractResult",
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 60,
                        "MaxAttempts": 3,
                        "BackoffRate": 2.0,
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "NotifyFailure",
                    }
                ],
            },
            "CheckExtractResult": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.records_count",
                        "NumericGreaterThan": 0,
                        "Next": "Transform",
                    }
                ],
                "Default": "NoNewData",
            },
            "NoNewData": {
                "Type": "Pass",
                "Result": {"status": "no_new_data"},
                "End": True,
            },
            "Transform": {
                "Type": "Task",
                "Resource": f"arn:aws:lambda:{AWS_REGION}:{AWS_ACCOUNT_ID}:function:gfn-transform",
                "Next": "Load",
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 2,
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "NotifyFailure",
                    }
                ],
            },
            "Load": {
                "Type": "Task",
                "Resource": f"arn:aws:lambda:{AWS_REGION}:{AWS_ACCOUNT_ID}:function:gfn-load",
                "Next": "NotifySuccess",
                "Retry": [
                    {
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 60,
                        "MaxAttempts": 3,
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "NotifyFailure",
                    }
                ],
            },
            "NotifySuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{AWS_REGION}:{AWS_ACCOUNT_ID}:{SNS_NOTIFICATIONS}",
                    "Message.$": "States.Format('Pipeline completed successfully. Records loaded: {}', $.records_loaded)",
                    "Subject": "GFN Pipeline - Success",
                },
                "End": True,
            },
            "NotifyFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": f"arn:aws:sns:{AWS_REGION}:{AWS_ACCOUNT_ID}:{SNS_NOTIFICATIONS}",
                    "Message.$": "States.Format('Pipeline failed: {}', $.error)",
                    "Subject": "GFN Pipeline - FAILURE",
                },
                "End": True,
            },
        },
    }

    try:
        sfn.create_state_machine(
            name="gfn-pipeline-orchestrator",
            definition=json.dumps(definition),
            roleArn=f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/step-functions-role",
        )
        print("✓ Created Step Functions state machine")
    except Exception as e:
        print(f"  Step Functions setup skipped: {e}")


def setup_cloudwatch():
    """Create CloudWatch log groups and alarms."""
    logs = get_client("logs")
    cloudwatch = get_client("cloudwatch")

    # Log groups
    log_groups = [
        "/aws/lambda/gfn-extract",
        "/aws/lambda/gfn-transform",
        "/aws/lambda/gfn-load",
        "/gfn/pipeline",
    ]

    for log_group in log_groups:
        try:
            logs.create_log_group(logGroupName=log_group)
            print(f"✓ Created log group: {log_group}")
        except logs.exceptions.ResourceAlreadyExistsException:
            pass

    # Alarm for DLQ messages (failures)
    # Note: CloudWatch alarms have limited support in LocalStack Community
    try:
        cloudwatch.put_metric_alarm(
            AlarmName="gfn-pipeline-failures",
            MetricName="ApproximateNumberOfMessagesVisible",
            Namespace="AWS/SQS",
            Dimensions=[{"Name": "QueueName", "Value": SQS_DLQ}],
            Statistic="Sum",
            Period=300,
            EvaluationPeriods=1,
            Threshold=1,
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            AlarmDescription="Alert when messages appear in DLQ",
        )
        print("✓ Created CloudWatch alarm for DLQ")
    except Exception as e:
        print(f"  Skipping CloudWatch alarm (not fully supported in LocalStack Community): {type(e).__name__}")


def print_summary():
    """Print infrastructure summary."""
    print("\n" + "=" * 60)
    print("LOCALSTACK INFRASTRUCTURE READY")
    print("=" * 60)
    print(f"""
Resources Created:
  S3:
    - s3://{S3_BUCKET}/raw/carbon_footprint/
    - s3://{S3_BUCKET}/processed/carbon_footprint/
  
  SQS:
    - {SQS_EXTRACT_QUEUE} (triggers extraction)
    - {SQS_TRANSFORM_QUEUE} (triggers transformation)
    - {SQS_LOAD_QUEUE} (triggers loading)
    - {SQS_DLQ} (failed messages)
  
  SNS:
    - {SNS_NOTIFICATIONS} (pipeline notifications)
  
  Lambda Functions:
    - gfn-extract (Extract data from GFN API)
    - gfn-transform (Transform and validate raw data)
    - gfn-load (Load processed data to Snowflake)
  
  EventBridge:
    - gfn-daily-extraction (cron: 0 6 * * ? *)
  
  Step Functions:
    - gfn-pipeline-orchestrator (ETL orchestration)
  
  CloudWatch:
    - Log groups for each Lambda
    - Alarm for DLQ messages

Endpoint: {LOCALSTACK_ENDPOINT}

Test Commands:
  # List Lambda functions
  uv run awslocal lambda list-functions
  
  # Invoke extract Lambda
  uv run awslocal lambda invoke --function-name gfn-extract \\
    --payload '{{"start_year": 2023}}' output.json
  
  # Check S3 bucket
  uv run awslocal s3 ls s3://{S3_BUCKET}/ --recursive
""")


def setup_s3_notifications():
    """Configure S3 event notifications (must be called after SQS setup)."""
    s3 = get_client("s3")

    # Enable event notifications (for transform trigger)
    s3.put_bucket_notification_configuration(
        Bucket=S3_BUCKET,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": f"arn:aws:sqs:{AWS_REGION}:{AWS_ACCOUNT_ID}:{SQS_TRANSFORM_QUEUE}",
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {"Name": "prefix", "Value": "raw/"},
                                {"Name": "suffix", "Value": ".json"},
                            ]
                        }
                    },
                }
            ]
        },
    )
    print("✓ Configured S3 event notifications")


def main():
    """Setup all LocalStack infrastructure."""
    print("Setting up LocalStack infrastructure...\n")

    # Core infrastructure
    setup_s3()                    # Create bucket and folders
    setup_sqs()                   # Create queues (needed for S3 notifications)
    setup_s3_notifications()      # Configure S3 -> SQS triggers
    setup_sns()
    setup_eventbridge()
    setup_cloudwatch()

    # Lambda deployment
    print("\nDeploying Lambda functions...")
    role_arn = setup_iam_role()   # Create execution role
    setup_lambda_functions(role_arn)  # Deploy Lambda code
    setup_lambda_triggers()       # Configure SQS -> Lambda triggers

    # Orchestration
    setup_step_functions()

    print_summary()


if __name__ == "__main__":
    main()
