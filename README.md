# LambdaS3Trigger
Serverless CSV Processing Demo: Automate CSV-to-DynamoDB Transformation with AWS Lambda, Secrets Manager, and Email Notifications

This AWS Lambda function is a demonstration designed to automate the processing of CSV files uploaded to an S3 bucket. When a new CSV file is added to the specified S3 bucket, this Lambda function triggers. It starts by retrieving the necessary database connection credentials stored securely in AWS Secrets Manager. Then, it downloads and processes the CSV data, converting it into DynamoDB table entries. Additionally, it sends an email notification upon successful completion, including the name of the processed file and the timestamp of the operation. This Lambda function efficiently manages data processing tasks while ensuring secure credential management and providing timely notifications for successful operations.

![ogo](https://raw.githubusercontent.com/rimega92/LambdaS3Trigger/main/Arquitectura.png)

