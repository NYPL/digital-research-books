#!/bin/bash

# Create S3 buckets
awslocal s3 mb s3://drb-files-local
awslocal s3 mb s3://drb-files-limited-local
awslocal s3 mb s3://ump-pdf-repository-local

# Configure CORS
awslocal s3api put-bucket-cors --bucket drb-files-local --cors-configuration '{
  "CORSRules": [
    {
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET", "POST", "PUT", "DELETE", "HEAD"],
      "AllowedHeaders": ["*"]
    }
  ]
}'

# Create SQS queues for testing
awslocal sqs create-queue --queue-name records
awslocal sqs create-queue --queue-name test-queue
