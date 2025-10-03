.PHONY: install test package deploy clean

# Variables
FUNCTION_NAME = RestaurantScraperLambda
PACKAGE_DIR = package
DEPLOYMENT_PACKAGE = deployment-package.zip
PYTHON = python3.11

install:
	pip install -r requirements.txt

test:
	python test_lambda.py

package: clean
	mkdir -p $(PACKAGE_DIR)
	pip install -r requirements.txt -t $(PACKAGE_DIR) --platform manylinux2014_x86_64 --only-binary=:all:
	cp AWS_scrape_restaurant_data.py $(PACKAGE_DIR)/
	cd $(PACKAGE_DIR) && zip -r ../$(DEPLOYMENT_PACKAGE) .
	@echo "Deployment package created: $(DEPLOYMENT_PACKAGE)"

deploy-sam:
	sam build
	sam deploy --guided

deploy-serverless:
	serverless deploy --stage prod

invoke:
	aws lambda invoke \
		--function-name $(FUNCTION_NAME) \
		--payload '{}' \
		response.json
	cat response.json

logs:
	aws logs tail /aws/lambda/$(FUNCTION_NAME) --follow

clean:
	rm -rf $(PACKAGE_DIR)
	rm -f $(DEPLOYMENT_PACKAGE)
	rm -f response.json
	rm -rf .aws-sam
	rm -rf .serverless

update-function: package
	aws lambda update-function-code \
		--function-name $(FUNCTION_NAME) \
		--zip-file fileb://$(DEPLOYMENT_PACKAGE)