# AWS Lambda Restaurant Scraper

This AWS Lambda function scrapes restaurant data from TripAdvisor and saves it to an S3 bucket.

## Features

- Fetches random restaurants from the Viberoam API that haven't been scraped yet
- Scrapes basic restaurant information from TripAdvisor
- Extracts structured data (JSON-LD) when available
- Saves scraped data to S3 in JSON format
- Can be triggered manually via API or scheduled to run automatically
- Includes error handling and retry logic

## Files

- `AWS_scrape_restaurant_data.py` - Main Lambda handler function
- `requirements.txt` - Python dependencies
- `template.yaml` - AWS SAM template for deployment
- `serverless.yml` - Serverless Framework configuration (alternative deployment method)
- `Makefile` - Build and deployment automation
- `.env.example` - Example environment variables file

## Deployment Options

### Option 1: AWS SAM (Recommended)

1. Install AWS SAM CLI:
   ```bash
   pip install aws-sam-cli
   ```

2. Build the application:
   ```bash
   sam build
   ```

3. Deploy to AWS:
   ```bash
   sam deploy --guided
   ```

   During the guided deployment, you'll be prompted for:
   - Stack name (e.g., `restaurant-scraper-stack`)
   - AWS Region
   - S3 bucket name for scraped data
   - API endpoints
   - Confirmation of IAM role creation

4. After deployment, note the outputs:
   - Function ARN
   - API Gateway endpoint URL
   - S3 bucket name

### Option 2: Serverless Framework

1. Install Serverless Framework:
   ```bash
   npm install -g serverless
   npm install --save-dev serverless-python-requirements
   ```

2. Configure AWS credentials:
   ```bash
   serverless config credentials --provider aws --key YOUR_KEY --secret YOUR_SECRET
   ```

3. Deploy:
   ```bash
   serverless deploy --stage prod --region us-east-1
   ```

### Option 3: Manual Deployment

1. Create deployment package:
   ```bash
   make package
   ```

2. Create Lambda function in AWS Console:
   - Runtime: Python 3.11
   - Handler: AWS_scrape_restaurant_data.lambda_handler
   - Memory: 512 MB
   - Timeout: 5 minutes

3. Upload the `deployment-package.zip` file

4. Configure environment variables:
   - `S3_BUCKET`: Your S3 bucket name
   - `API_ENDPOINT`: Viberoam API endpoint
   - `UPDATE_API_ENDPOINT`: (Optional) Endpoint to update scraping status

5. Add IAM permissions for S3 access

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_BUCKET` | S3 bucket for storing scraped data | `restaurant-scraper-data` |
| `API_ENDPOINT` | API endpoint to fetch restaurants | Viberoam API URL |
| `UPDATE_API_ENDPOINT` | Optional endpoint to update scraping status | None |
| `MAX_RETRIES` | Maximum retry attempts | 3 |
| `TIMEOUT` | Request timeout in seconds | 30 |

## Usage

### Manual Trigger (via API)

```bash
curl -X POST https://YOUR_API_GATEWAY_URL/prod/scrape
```

### Manual Trigger (via AWS CLI)

```bash
aws lambda invoke \
  --function-name RestaurantScraperLambda \
  --payload '{}' \
  response.json
```

### Scheduled Execution

The function can be configured to run automatically:

1. For SAM deployment: Set `Enabled: true` in the ScheduledEvent section of `template.yaml`
2. For Serverless: Uncomment the schedule event in `serverless.yml`

Default schedule is every 30 minutes, but can be adjusted.

## S3 Output Structure

Scraped data is saved to S3 with the following structure:

```
s3://your-bucket/
└── scraped_data/
    └── YYYYMMDD_HHMMSS/
        └── {restaurant_id}_{restaurant_name}.json
```

Each JSON file contains:
- Restaurant metadata (ID, name, URL)
- Location information (city, country)
- Scraped data (rating, reviews, address, etc.)
- JSON-LD structured data (if available)
- Timestamp and success status

## Monitoring

### CloudWatch Logs

Logs are automatically sent to CloudWatch:
- Log group: `/aws/lambda/RestaurantScraperLambda`
- Retention: 30 days

### Metrics

Monitor these CloudWatch metrics:
- Invocations
- Duration
- Errors
- Throttles

## Cost Optimization

- Lambda: ~$0.20 per million requests (512MB, 300s timeout)
- S3: ~$0.023 per GB stored
- Data transfer: First 1GB free, then ~$0.09 per GB

Tips to reduce costs:
1. Adjust Lambda memory size based on actual usage
2. Enable S3 lifecycle policies to delete old data
3. Use reserved concurrency to limit parallel executions
4. Consider using Spot instances for large-scale scraping

## Troubleshooting

### Common Issues

1. **Timeout errors**: Increase Lambda timeout or optimize scraping logic
2. **S3 access denied**: Check IAM role permissions
3. **API rate limiting**: Implement exponential backoff
4. **Memory errors**: Increase Lambda memory allocation

### Testing Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run test event
python test_lambda.py
```

## Security Considerations

- S3 bucket is encrypted with AES-256
- Public access is blocked by default
- Lambda function uses least-privilege IAM role
- API Gateway can be configured with API keys for access control
- Consider using VPC endpoints for private S3 access

## Contributing

1. Test changes locally
2. Update requirements.txt if adding dependencies
3. Update this README with any new features
4. Test deployment with SAM/Serverless before committing

## License

[Your License Here]