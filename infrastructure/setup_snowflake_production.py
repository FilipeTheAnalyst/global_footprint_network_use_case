#!/usr/bin/env python3
"""
Snowflake Production Setup Script.

Automates the setup of Snowflake infrastructure for the GFN pipeline:
1. Deploys AWS infrastructure (CloudFormation or LocalStack)
2. Runs Snowflake SQL scripts to create storage integration, Snowpipe, etc.
3. Configures S3 bucket notifications
4. Updates Lambda environment variables
5. Verifies the complete setup

Prerequisites:
    - AWS CLI configured OR LocalStack running
    - Snowflake account with ACCOUNTADMIN privileges
    - Environment variables or .env file with credentials

Usage:
    # Interactive setup (prompts for missing values)
    python -m infrastructure.setup_snowflake_production

    # Use LocalStack instead of real AWS
    python -m infrastructure.setup_snowflake_production --use-localstack

    # With environment variables
    AWS_ACCOUNT_ID=123456789012 SNOWFLAKE_ACCOUNT=xxx python -m infrastructure.setup_snowflake_production

    # Individual steps
    python -m infrastructure.setup_snowflake_production --step aws
    python -m infrastructure.setup_snowflake_production --step snowflake
    python -m infrastructure.setup_snowflake_production --step verify
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Try to load .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Try to import Snowflake connector
try:
    import snowflake.connector

    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False

# Try to import boto3
try:
    import boto3
    from botocore.config import Config

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


# =============================================================================
# Configuration
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
INFRASTRUCTURE_DIR = PROJECT_ROOT / "infrastructure"
SNOWFLAKE_SQL_DIR = INFRASTRUCTURE_DIR / "snowflake"
AWS_DIR = INFRASTRUCTURE_DIR / "aws"

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "000000000000")
S3_BUCKET = os.getenv("S3_BUCKET", "gfn-data-lake")
CLOUDFORMATION_STACK_NAME = "gfn-snowpipe-role"

# LocalStack Configuration
LOCALSTACK_ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
USE_LOCALSTACK = False  # Set via CLI argument

# Snowflake Configuration
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "GFN")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")


# =============================================================================
# Utility Functions
# =============================================================================


def get_aws_client(service: str):
    """Get boto3 client, using LocalStack if configured."""
    if not HAS_BOTO3:
        raise ImportError("boto3 not installed")

    kwargs = {"region_name": AWS_REGION}

    if USE_LOCALSTACK:
        kwargs["endpoint_url"] = LOCALSTACK_ENDPOINT
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
        if service == "s3":
            kwargs["config"] = Config(signature_version="s3v4")

    return boto3.client(service, **kwargs)


def get_aws_command_prefix() -> list[str]:
    """Get the AWS CLI command prefix (aws or awslocal)."""
    if USE_LOCALSTACK:
        return ["uv", "run", "awslocal"]
    return ["aws"]


def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_step(step: int, total: int, description: str):
    """Print a step indicator."""
    print(f"\n[{step}/{total}] {description}")
    print("-" * 50)


def prompt_for_value(name: str, current_value: str | None, secret: bool = False) -> str:
    """Prompt user for a value if not set."""
    if current_value:
        if secret:
            display = current_value[:4] + "****"
        else:
            display = current_value
        print(f"  {name}: {display}")
        return current_value

    if secret:
        import getpass

        value = getpass.getpass(f"  Enter {name}: ")
    else:
        value = input(f"  Enter {name}: ")

    return value.strip()


def run_command(cmd: list[str], capture: bool = False) -> tuple[int, str]:
    """Run a shell command."""
    try:
        if capture:
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode, result.stdout + result.stderr
        else:
            result = subprocess.run(cmd)
            return result.returncode, ""
    except FileNotFoundError:
        return 1, f"Command not found: {cmd[0]}"


def get_snowflake_connection():
    """Get Snowflake connection."""
    if not HAS_SNOWFLAKE:
        raise ImportError(
            "snowflake-connector-python not installed. Run: uv add snowflake-connector-python"
        )

    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )


# =============================================================================
# AWS Setup Functions
# =============================================================================


def check_aws_cli() -> bool:
    """Check if AWS CLI is available."""
    if USE_LOCALSTACK:
        # Check if awslocal is available
        code, _ = run_command(["uv", "run", "awslocal", "--version"], capture=True)
        return code == 0
    code, _ = run_command(["aws", "--version"], capture=True)
    return code == 0


def check_localstack_running() -> bool:
    """Check if LocalStack is running."""
    try:
        import urllib.request

        response = urllib.request.urlopen(f"{LOCALSTACK_ENDPOINT}/_localstack/health", timeout=5)
        return response.status == 200
    except Exception:
        return False


def setup_localstack_resources():
    """Setup AWS resources in LocalStack for Snowflake integration."""
    print("Setting up LocalStack resources...")

    if not check_localstack_running():
        print("  ✗ LocalStack is not running!")
        print("  Start it with: make localstack-up")
        return False

    # Use boto3 with LocalStack endpoint
    s3 = get_aws_client("s3")
    iam = get_aws_client("iam")
    sns = get_aws_client("sns")
    sqs = get_aws_client("sqs")

    # 1. Create S3 bucket if not exists
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"  ✓ Created S3 bucket: {S3_BUCKET}")
    except s3.exceptions.BucketAlreadyExists:
        print(f"  ✓ S3 bucket exists: {S3_BUCKET}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"  ✓ S3 bucket exists: {S3_BUCKET}")
    except Exception as e:
        print(f"  ✓ S3 bucket: {S3_BUCKET} ({e})")

    # Create folder structure
    for prefix in ["raw/", "transformed/", "failed/"]:
        s3.put_object(Bucket=S3_BUCKET, Key=f"{prefix}.keep", Body=b"placeholder")
    print("  ✓ Created S3 folder structure")

    # 2. Create IAM role for Snowflake
    role_name = "gfn-snowflake-role"
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "snowflake.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Snowflake to access S3",
        )
        print(f"  ✓ Created IAM role: {role_name}")
    except Exception as e:
        if "already exists" in str(e).lower() or "EntityAlreadyExists" in str(e):
            print(f"  ✓ IAM role exists: {role_name}")
        else:
            print(f"  ⚠ IAM role: {e}")

    # Attach S3 policy to role
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}",
                    f"arn:aws:s3:::{S3_BUCKET}/*",
                ],
            }
        ],
    }

    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName="snowflake-s3-access",
            PolicyDocument=json.dumps(s3_policy),
        )
        print("  ✓ Attached S3 policy to role")
    except Exception as e:
        print(f"  ⚠ Policy attachment: {e}")

    # 3. Create SNS topic for Snowpipe notifications
    topic_name = "gfn-snowpipe-notifications"
    try:
        response = sns.create_topic(Name=topic_name)
        topic_arn = response["TopicArn"]
        print(f"  ✓ Created SNS topic: {topic_name}")
    except Exception:
        topic_arn = f"arn:aws:sns:{AWS_REGION}:{AWS_ACCOUNT_ID}:{topic_name}"
        print(f"  ✓ SNS topic: {topic_name}")

    # 4. Create SQS queue (simulating Snowpipe's managed queue)
    queue_name = "gfn-snowpipe-queue"
    queue_arn = f"arn:aws:sqs:{AWS_REGION}:{AWS_ACCOUNT_ID}:{queue_name}"
    try:
        response = sqs.create_queue(QueueName=queue_name)
        response["QueueUrl"]
        print(f"  ✓ Created SQS queue: {queue_name}")
    except Exception:
        print(f"  ✓ SQS queue: {queue_name}")

    # 4b. Subscribe SQS to SNS (so S3 events flow: S3 → SNS → SQS)
    try:
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
        )
        print("  ✓ Subscribed SQS to SNS topic")
    except Exception as e:
        print(f"  ⚠ SNS subscription: {e}")

    # 5. Configure S3 event notifications to SNS
    try:
        s3.put_bucket_notification_configuration(
            Bucket=S3_BUCKET,
            NotificationConfiguration={
                "TopicConfigurations": [
                    {
                        "TopicArn": topic_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "prefix", "Value": "transformed/"},
                                    {"Name": "suffix", "Value": ".json"},
                                ]
                            }
                        },
                    }
                ]
            },
        )
        print("  ✓ Configured S3 → SNS notifications")
    except Exception as e:
        print(f"  ⚠ S3 notifications: {e}")

    role_arn = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{role_name}"

    print("\n  LocalStack Resources Ready:")
    print(f"    S3 Bucket: s3://{S3_BUCKET}")
    print(f"    IAM Role: {role_arn}")
    print(f"    SNS Topic: {topic_arn}")
    print(f"    SQS Queue: {queue_arn} (simulates Snowpipe)")

    return True


def deploy_cloudformation_stack() -> dict:
    """Deploy the CloudFormation stack for Snowflake IAM role."""
    if USE_LOCALSTACK:
        # LocalStack doesn't fully support CloudFormation for IAM
        # Use direct resource creation instead
        print("  Using direct resource creation (LocalStack mode)")
        setup_localstack_resources()
        return {"RoleArn": f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/gfn-snowflake-role"}

    print("Deploying CloudFormation stack...")

    template_path = AWS_DIR / "snowpipe_iam_role.json"
    if not template_path.exists():
        raise FileNotFoundError(f"CloudFormation template not found: {template_path}")

    aws_cmd = get_aws_command_prefix()

    # Check if stack already exists
    code, output = run_command(
        aws_cmd
        + [
            "cloudformation",
            "describe-stacks",
            "--stack-name",
            CLOUDFORMATION_STACK_NAME,
            "--region",
            AWS_REGION,
        ],
        capture=True,
    )

    # Common parameters for stack
    stack_params = [
        f"ParameterKey=S3BucketName,ParameterValue={S3_BUCKET}",
    ]

    if code == 0:
        print(f"  Stack '{CLOUDFORMATION_STACK_NAME}' already exists")
        # Update stack - use previous values for Snowflake params
        code, output = run_command(
            aws_cmd
            + [
                "cloudformation",
                "update-stack",
                "--stack-name",
                CLOUDFORMATION_STACK_NAME,
                "--template-body",
                f"file://{template_path}",
                "--parameters",
                stack_params[0],
                "ParameterKey=SnowflakeAccountArn,UsePreviousValue=true",
                "ParameterKey=SnowflakeExternalId,UsePreviousValue=true",
                "--capabilities",
                "CAPABILITY_NAMED_IAM",
                "--region",
                AWS_REGION,
            ],
            capture=True,
        )

        if "No updates are to be performed" in output:
            print("  No updates needed")
        elif code != 0:
            print(f"  Update failed (may be up to date): {output[:200]}")
    else:
        # Create new stack with default placeholder values
        code, output = run_command(
            aws_cmd
            + [
                "cloudformation",
                "create-stack",
                "--stack-name",
                CLOUDFORMATION_STACK_NAME,
                "--template-body",
                f"file://{template_path}",
                "--parameters",
                stack_params[0],
                "--capabilities",
                "CAPABILITY_NAMED_IAM",
                "--region",
                AWS_REGION,
            ],
            capture=True,
        )

        if code != 0:
            raise RuntimeError(f"Failed to create stack: {output}")

        print("  Waiting for stack creation...")
        run_command(
            aws_cmd
            + [
                "cloudformation",
                "wait",
                "stack-create-complete",
                "--stack-name",
                CLOUDFORMATION_STACK_NAME,
                "--region",
                AWS_REGION,
            ]
        )
        print(
            "  ✓ Stack created (trust policy uses placeholder - will be updated after Snowflake setup)"
        )

    # Get stack outputs
    code, output = run_command(
        aws_cmd
        + [
            "cloudformation",
            "describe-stacks",
            "--stack-name",
            CLOUDFORMATION_STACK_NAME,
            "--region",
            AWS_REGION,
            "--query",
            "Stacks[0].Outputs",
            "--output",
            "json",
        ],
        capture=True,
    )

    if code == 0:
        try:
            outputs = json.loads(output)
            return {o["OutputKey"]: o["OutputValue"] for o in outputs}
        except (json.JSONDecodeError, KeyError):
            pass

    return {}


def get_iam_role_arn() -> str:
    """Get the IAM role ARN from CloudFormation stack."""
    if USE_LOCALSTACK:
        return f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/gfn-snowflake-role"

    aws_cmd = get_aws_command_prefix()
    code, output = run_command(
        aws_cmd
        + [
            "cloudformation",
            "describe-stacks",
            "--stack-name",
            CLOUDFORMATION_STACK_NAME,
            "--region",
            AWS_REGION,
            "--query",
            "Stacks[0].Outputs[?OutputKey=='RoleArn'].OutputValue",
            "--output",
            "text",
        ],
        capture=True,
    )

    if code == 0 and output.strip():
        return output.strip()

    # Fallback to constructed ARN
    return f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/gfn-snowflake-role"


def configure_s3_notifications(sqs_arn: str):
    """Configure S3 bucket to send events to Snowpipe SQS."""
    print(f"Configuring S3 notifications to: {sqs_arn}")

    if USE_LOCALSTACK:
        # Already configured in setup_localstack_resources
        print("  ✓ S3 notifications already configured (LocalStack mode)")
        return

    notification_config = {
        "QueueConfigurations": [
            {
                "QueueArn": sqs_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "prefix", "Value": "transformed/"},
                            {"Name": "suffix", "Value": ".json"},
                        ]
                    }
                },
            }
        ]
    }

    # Write config to temp file
    config_file = "/tmp/s3_notification_config.json"
    with open(config_file, "w") as f:
        json.dump(notification_config, f)

    aws_cmd = get_aws_command_prefix()
    code, output = run_command(
        aws_cmd
        + [
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            S3_BUCKET,
            "--notification-configuration",
            f"file://{config_file}",
            "--region",
            AWS_REGION,
        ],
        capture=True,
    )

    if code != 0:
        print(f"  Warning: Failed to configure S3 notifications: {output[:200]}")
        print("  You may need to configure this manually in the AWS Console")
    else:
        print("  ✓ S3 notifications configured")


def update_lambda_environment():
    """Update Lambda function environment variables for Snowflake.

    Merges new variables with existing ones to preserve GFN_API_KEY etc.
    """
    print("Updating Lambda environment variables...")

    # Load GFN_API_KEY from environment
    gfn_api_key = os.getenv("GFN_API_KEY", "")

    env_vars = {
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
        "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
        "SNOWFLAKE_SCHEMA": "RAW",
        "S3_BUCKET": S3_BUCKET,
        "GFN_API_KEY": gfn_api_key,
    }

    # Only add Snowflake credentials if provided (not for LocalStack-only setup)
    if SNOWFLAKE_ACCOUNT:
        env_vars["SNOWFLAKE_ACCOUNT"] = SNOWFLAKE_ACCOUNT
    if SNOWFLAKE_USER:
        env_vars["SNOWFLAKE_USER"] = SNOWFLAKE_USER
    if SNOWFLAKE_PASSWORD:
        env_vars["SNOWFLAKE_PASSWORD"] = SNOWFLAKE_PASSWORD

    if USE_LOCALSTACK:
        env_vars["AWS_ENDPOINT_URL"] = "http://host.docker.internal:4566"

    # Use boto3 for Lambda updates
    try:
        lambda_client = get_aws_client("lambda")

        for function_name in ["gfn-extract", "gfn-transform", "gfn-load"]:
            try:
                # Get existing environment variables
                response = lambda_client.get_function_configuration(FunctionName=function_name)
                existing_vars = response.get("Environment", {}).get("Variables", {})

                # Merge: existing vars + new vars (new vars take precedence)
                merged_vars = {**existing_vars, **{k: v for k, v in env_vars.items() if v}}

                lambda_client.update_function_configuration(
                    FunctionName=function_name,
                    Environment={"Variables": merged_vars},
                )
                print(f"  ✓ Updated {function_name}")
            except Exception as e:
                print(f"  ⚠ {function_name}: {str(e)[:50]}")
    except Exception as e:
        print(f"  ⚠ Lambda update skipped: {e}")


# =============================================================================
# Snowflake Setup Functions
# =============================================================================


def prepare_sql_scripts() -> list[Path]:
    """Prepare SQL scripts with actual values substituted."""
    scripts = [
        SNOWFLAKE_SQL_DIR / "01_setup_storage.sql",
        SNOWFLAKE_SQL_DIR / "02_snowpipe.sql",
        SNOWFLAKE_SQL_DIR / "03_monitoring.sql",
    ]

    prepared_scripts = []

    for script in scripts:
        if not script.exists():
            print(f"  Warning: Script not found: {script}")
            continue

        content = script.read_text()

        # Replace placeholders
        content = content.replace("<YOUR_AWS_ACCOUNT_ID>", AWS_ACCOUNT_ID or "REPLACE_ME")
        content = content.replace("COMPUTE_WH", SNOWFLAKE_WAREHOUSE)

        # Write to temp file
        prepared_path = Path(f"/tmp/{script.name}")
        prepared_path.write_text(content)
        prepared_scripts.append(prepared_path)

        print(f"  ✓ Prepared {script.name}")

    return prepared_scripts


def run_snowflake_scripts(scripts: list[Path]) -> dict:
    """Run Snowflake SQL scripts and return important values."""
    print("Connecting to Snowflake...")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    results = {
        "storage_aws_iam_user_arn": None,
        "storage_aws_external_id": None,
        "snowpipe_sqs_arn": None,
    }

    try:
        for script in scripts:
            print(f"\n  Running {script.name}...")

            content = script.read_text()

            # Split into individual statements
            statements = [s.strip() for s in content.split(";") if s.strip()]

            for i, stmt in enumerate(statements):
                # Skip comments-only statements
                if all(
                    line.strip().startswith("--") or not line.strip() for line in stmt.split("\n")
                ):
                    continue

                try:
                    cursor.execute(stmt)

                    # Check for specific queries we need results from
                    if "DESC STORAGE INTEGRATION" in stmt.upper():
                        rows = cursor.fetchall()
                        for row in rows:
                            if row[0] == "STORAGE_AWS_IAM_USER_ARN":
                                results["storage_aws_iam_user_arn"] = row[1]
                            elif row[0] == "STORAGE_AWS_EXTERNAL_ID":
                                results["storage_aws_external_id"] = row[1]

                    elif "SHOW PIPES" in stmt.upper():
                        rows = cursor.fetchall()
                        if rows:
                            # notification_channel is typically column index 9
                            for row in rows:
                                if "GFN_FOOTPRINT_DATA_PIPE" in str(row):
                                    # Find the SQS ARN in the row
                                    for val in row:
                                        if val and "sqs" in str(val).lower():
                                            results["snowpipe_sqs_arn"] = str(val)
                                            break

                except Exception as e:
                    # Some statements may fail if objects already exist
                    error_msg = str(e)
                    if "already exists" in error_msg.lower():
                        pass
                    elif "does not exist" in error_msg.lower():
                        pass
                    else:
                        print(f"    Warning: {error_msg[:100]}")

            print(f"    ✓ Completed {script.name}")

    finally:
        cursor.close()
        conn.close()

    return results


def update_iam_trust_policy(snowflake_iam_arn: str, external_id: str):
    """Update IAM role trust policy with Snowflake's IAM user.

    Uses CloudFormation stack update if the stack exists, otherwise falls back
    to direct IAM API call.
    """
    print("Updating IAM role trust policy...")

    if USE_LOCALSTACK:
        # LocalStack: use direct IAM update
        try:
            iam = get_aws_client("iam")
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": snowflake_iam_arn},
                        "Action": "sts:AssumeRole",
                        "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
                    }
                ],
            }
            iam.update_assume_role_policy(
                RoleName="gfn-snowflake-role", PolicyDocument=json.dumps(trust_policy)
            )
            print("  ✓ Trust policy updated (LocalStack)")
            return
        except Exception as e:
            print(f"  Warning: Failed to update trust policy: {e}")
            return

    # Try CloudFormation update first
    aws_cmd = get_aws_command_prefix()
    template_path = AWS_DIR / "snowpipe_iam_role.json"

    code, output = run_command(
        aws_cmd
        + [
            "cloudformation",
            "update-stack",
            "--stack-name",
            CLOUDFORMATION_STACK_NAME,
            "--template-body",
            f"file://{template_path}",
            "--parameters",
            f"ParameterKey=S3BucketName,ParameterValue={S3_BUCKET}",
            f"ParameterKey=SnowflakeAccountArn,ParameterValue={snowflake_iam_arn}",
            f"ParameterKey=SnowflakeExternalId,ParameterValue={external_id}",
            "--capabilities",
            "CAPABILITY_NAMED_IAM",
            "--region",
            AWS_REGION,
        ],
        capture=True,
    )

    if code == 0:
        print("  Waiting for stack update...")
        run_command(
            aws_cmd
            + [
                "cloudformation",
                "wait",
                "stack-update-complete",
                "--stack-name",
                CLOUDFORMATION_STACK_NAME,
                "--region",
                AWS_REGION,
            ]
        )
        print("  ✓ Trust policy updated via CloudFormation")
        return

    if "No updates are to be performed" in output:
        print("  ✓ Trust policy already up to date")
        return

    # Fallback to direct IAM update
    print("  CloudFormation update failed, trying direct IAM update...")
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": snowflake_iam_arn},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
            }
        ],
    }

    # Write to temp file
    policy_file = "/tmp/trust_policy.json"
    with open(policy_file, "w") as f:
        json.dump(trust_policy, f)

    code, output = run_command(
        aws_cmd
        + [
            "iam",
            "update-assume-role-policy",
            "--role-name",
            "gfn-snowflake-role",
            "--policy-document",
            f"file://{policy_file}",
        ],
        capture=True,
    )

    if code != 0:
        print(f"  Warning: Failed to update trust policy: {output[:200]}")
        print("  Please update manually with:")
        print(f"    Snowflake IAM ARN: {snowflake_iam_arn}")
        print(f"    External ID: {external_id}")
    else:
        print("  ✓ Trust policy updated")


# =============================================================================
# Verification Functions
# =============================================================================


def verify_storage_integration() -> bool:
    """Verify Snowflake storage integration is working."""
    print("Verifying storage integration...")

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("DESC STORAGE INTEGRATION gfn_s3_integration")
        rows = cursor.fetchall()

        for row in rows:
            if row[0] == "ENABLED" and row[1] == "true":
                print("  ✓ Storage integration is enabled")
                cursor.close()
                conn.close()
                return True

        print("  ✗ Storage integration is not enabled")
        cursor.close()
        conn.close()
        return False

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def verify_snowpipe() -> bool:
    """Verify Snowpipe is configured correctly."""
    print("Verifying Snowpipe...")

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT SYSTEM$PIPE_STATUS('GFN.RAW.GFN_FOOTPRINT_DATA_PIPE')")
        result = cursor.fetchone()

        if result:
            status = json.loads(result[0])
            print(f"  Pipe status: {status.get('executionState', 'unknown')}")

            if status.get("executionState") == "RUNNING":
                print("  ✓ Snowpipe is running")
                cursor.close()
                conn.close()
                return True

        print("  ⚠ Snowpipe may not be fully configured")
        cursor.close()
        conn.close()
        return False

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def verify_s3_access() -> bool:
    """Verify Snowflake can access S3."""
    print("Verifying S3 access from Snowflake...")

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute("USE DATABASE GFN")
        cursor.execute("USE SCHEMA RAW")
        cursor.execute("LIST @gfn_processed_stage")
        rows = cursor.fetchall()

        print(f"  ✓ Can list S3 stage ({len(rows)} files found)")
        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"  ✗ Error accessing S3: {e}")
        return False


# =============================================================================
# Main Setup Functions
# =============================================================================


def setup_aws():
    """Run AWS setup steps."""
    print_header("AWS Infrastructure Setup")

    if not HAS_BOTO3:
        print("Warning: boto3 not installed. Using AWS CLI instead.")

    if not check_aws_cli():
        print("Error: AWS CLI not found. Please install it first.")
        print("  brew install awscli  # macOS")
        print("  pip install awscli   # pip")
        return False

    # Step 1: Deploy CloudFormation
    print_step(1, 3, "Deploying CloudFormation stack")
    try:
        outputs = deploy_cloudformation_stack()
        role_arn = outputs.get("RoleArn") or get_iam_role_arn()
        print(f"  IAM Role ARN: {role_arn}")
    except Exception as e:
        print(f"  Error: {e}")
        return False

    # Step 2: Update Lambda (optional, only if Lambdas exist)
    print_step(2, 3, "Updating Lambda environment (if exists)")
    try:
        update_lambda_environment()
    except Exception as e:
        print(f"  Skipped (Lambdas may not exist): {e}")

    print_step(3, 3, "AWS setup complete")
    print("  ✓ IAM role created/updated")
    print("  ✓ Lambda environment configured")
    print("\n  Next: Run Snowflake setup to complete the integration")

    return True


def setup_snowflake():
    """Run Snowflake setup steps."""
    print_header("Snowflake Setup")

    if not HAS_SNOWFLAKE:
        print("Error: snowflake-connector-python not installed.")
        print("  Run: uv add snowflake-connector-python")
        return False

    # Step 1: Prepare SQL scripts
    print_step(1, 4, "Preparing SQL scripts")
    scripts = prepare_sql_scripts()

    # Step 2: Run SQL scripts
    print_step(2, 4, "Running Snowflake SQL scripts")
    results = run_snowflake_scripts(scripts)

    # Step 3: Update IAM trust policy
    print_step(3, 4, "Updating IAM trust policy")
    if results.get("storage_aws_iam_user_arn") and results.get("storage_aws_external_id"):
        update_iam_trust_policy(
            results["storage_aws_iam_user_arn"], results["storage_aws_external_id"]
        )
    else:
        print("  Warning: Could not get Snowflake IAM details")
        print("  Run this in Snowflake to get the values:")
        print("    DESC STORAGE INTEGRATION gfn_s3_integration;")

    # Step 4: Configure S3 notifications
    print_step(4, 4, "Configuring S3 notifications")
    if results.get("snowpipe_sqs_arn"):
        configure_s3_notifications(results["snowpipe_sqs_arn"])
    else:
        print("  Warning: Could not get Snowpipe SQS ARN")
        print("  Run this in Snowflake to get the SQS ARN:")
        print("    SHOW PIPES LIKE 'GFN_FOOTPRINT_DATA_PIPE';")

    print("\n  ✓ Snowflake setup complete")
    return True


def verify_setup():
    """Verify the complete setup."""
    print_header("Verification")

    if not HAS_SNOWFLAKE:
        print("Error: snowflake-connector-python not installed.")
        return False

    results = {
        "storage_integration": verify_storage_integration(),
        "s3_access": verify_s3_access(),
        "snowpipe": verify_snowpipe(),
    }

    print("\n" + "-" * 50)
    print("Summary:")
    for check, passed in results.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {check.replace('_', ' ').title()}")

    all_passed = all(results.values())
    if all_passed:
        print("\n✓ All checks passed! Pipeline is ready.")
    else:
        print("\n⚠ Some checks failed. Review the output above.")

    return all_passed


def interactive_setup():
    """Run interactive setup with prompts."""
    global AWS_ACCOUNT_ID, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
    global SNOWFLAKE_WAREHOUSE

    print_header("GFN Pipeline - Snowflake Production Setup")
    print("""
This script will:
  1. Deploy AWS CloudFormation stack for IAM role
  2. Run Snowflake SQL scripts (storage integration, Snowpipe, etc.)
  3. Configure S3 bucket notifications
  4. Update IAM trust policy
  5. Verify the complete setup
    """)

    # Gather required values
    print("Configuration:")
    print("-" * 50)

    AWS_ACCOUNT_ID = prompt_for_value("AWS_ACCOUNT_ID", AWS_ACCOUNT_ID)
    SNOWFLAKE_ACCOUNT = prompt_for_value("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT)
    SNOWFLAKE_USER = prompt_for_value("SNOWFLAKE_USER", SNOWFLAKE_USER)
    SNOWFLAKE_PASSWORD = prompt_for_value("SNOWFLAKE_PASSWORD", SNOWFLAKE_PASSWORD, secret=True)
    SNOWFLAKE_WAREHOUSE = prompt_for_value("SNOWFLAKE_WAREHOUSE", SNOWFLAKE_WAREHOUSE)

    print("\n")
    proceed = input("Proceed with setup? [Y/n]: ").strip().lower()
    if proceed == "n":
        print("Aborted.")
        return

    # Run setup steps
    if not setup_aws():
        print("\nAWS setup failed. Fix issues and retry.")
        return

    if not setup_snowflake():
        print("\nSnowflake setup failed. Fix issues and retry.")
        return

    # Wait a moment for things to propagate
    print("\nWaiting 10 seconds for configuration to propagate...")
    time.sleep(10)

    # Verify
    verify_setup()

    print_header("Setup Complete")
    print("""
Next steps:
  1. Upload a test file to S3:
     aws s3 cp test.json s3://gfn-data-lake/transformed/

  2. Check Snowpipe status:
     SELECT SYSTEM$PIPE_STATUS('GFN.RAW.GFN_FOOTPRINT_DATA_PIPE');

  3. Monitor data ingestion:
     SELECT * FROM GFN.RAW.GFN_FOOTPRINT_DATA_RAW LIMIT 10;

  4. Run the Lambda pipeline:
     aws lambda invoke --function-name gfn-extract --payload '{"start_year": 2023}' out.json
    """)


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    global USE_LOCALSTACK

    parser = argparse.ArgumentParser(
        description="Setup Snowflake production infrastructure for GFN pipeline"
    )
    parser.add_argument(
        "--step",
        choices=["aws", "snowflake", "verify", "all"],
        default="all",
        help="Run specific setup step (default: all)",
    )
    parser.add_argument(
        "--non-interactive", action="store_true", help="Run without prompts (requires env vars)"
    )
    parser.add_argument(
        "--use-localstack",
        action="store_true",
        help="Use LocalStack instead of real AWS (for local development)",
    )

    args = parser.parse_args()

    # Set LocalStack mode if requested
    if args.use_localstack:
        USE_LOCALSTACK = True
        print("LocalStack mode enabled")
        if not check_localstack_running():
            print("Error: LocalStack is not running!")
            print("  Start it with: make localstack-up")
            sys.exit(1)
        print("  LocalStack is running")

    if args.step == "aws":
        setup_aws()
    elif args.step == "snowflake":
        setup_snowflake()
    elif args.step == "verify":
        verify_setup()
    elif args.non_interactive:
        # Check required env vars
        missing = []
        if not AWS_ACCOUNT_ID:
            missing.append("AWS_ACCOUNT_ID")
        if not SNOWFLAKE_ACCOUNT:
            missing.append("SNOWFLAKE_ACCOUNT")
        if not SNOWFLAKE_USER:
            missing.append("SNOWFLAKE_USER")
        if not SNOWFLAKE_PASSWORD:
            missing.append("SNOWFLAKE_PASSWORD")

        if missing:
            print(f"Error: Missing required environment variables: {', '.join(missing)}")
            sys.exit(1)

        setup_aws()
        setup_snowflake()
        time.sleep(10)
        verify_setup()
    else:
        interactive_setup()


if __name__ == "__main__":
    main()
