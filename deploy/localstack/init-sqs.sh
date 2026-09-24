#!/bin/bash
# Provisions the SQS queues (runs when LocalStack is ready).
# Ingress: wager-transactions.fifo -> redrive to wager-transactions-dlq.fifo.
# The application moves poison/permanent failures to the DLQ itself after
# SQS_MAX_RECEIVES (5); the broker redrive (maxReceiveCount=8) is a safety net
# for consumers that crash repeatedly on the same message.
set -euo pipefail
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"

create() { awslocal sqs create-queue --region "$REGION" --queue-name "$1" --attributes "$2" >/dev/null; echo "queue $1 ready"; }

create wager-transactions-dlq.fifo '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}'
create wager-transactions.fifo "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"30\",\"ReceiveMessageWaitTimeSeconds\":\"10\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions-dlq.fifo\\\",\\\"maxReceiveCount\\\":\\\"8\\\"}\"}"
create wallet-events-dlq.fifo '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}'
create wallet-events.fifo "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"arn:aws:sqs:${REGION}:${ACCOUNT}:wallet-events-dlq.fifo\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"

# Broker access policies (least privilege). Enforced by AWS; LocalStack
# Community stores them but does not enforce IAM (see ARCHITECTURE.md).
INGRESS_POLICY=$(cat <<JSON
{"Version":"2012-10-17","Statement":[
 {"Sid":"ProviderGatewaySend","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::${ACCOUNT}:user/provider-gateway"},"Action":["sqs:SendMessage"],"Resource":"arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions.fifo"},
 {"Sid":"WalletServiceConsume","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::${ACCOUNT}:user/wallet-service"},"Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:ChangeMessageVisibility","sqs:GetQueueAttributes","sqs:GetQueueUrl"],"Resource":"arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions.fifo"}
]}
JSON
)
awslocal sqs set-queue-attributes --region "$REGION" \
  --queue-url "http://sqs.${REGION}.localhost.localstack.cloud:4566/${ACCOUNT}/wager-transactions.fifo" \
  --attributes "$(POLICY="$INGRESS_POLICY" python3 -c 'import json,os; print(json.dumps({"Policy": os.environ["POLICY"]}))')" >/dev/null || true
echo "sqs provisioning done"
