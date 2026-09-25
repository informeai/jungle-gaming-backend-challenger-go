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

# Broker access policies: one AWS identity per component, least privilege.
#   provider-gateway     producer: SendMessage on the ingress queue
#   wallet-consumer      consumer: receive/delete/change visibility on ingress,
#                        SendMessage on the ingress DLQ
#   wallet-outbox-relay  relay: SendMessage on the events queue only
#   (the HTTP API has no AWS identity at all)
# Enforced by AWS; LocalStack Community stores them but does not enforce IAM
# (see ARCHITECTURE.md), so the consumer also validates providers itself.
arn() { echo "arn:aws:sqs:${REGION}:${ACCOUNT}:$1"; }
user() { echo "arn:aws:iam::${ACCOUNT}:user/$1"; }
set_policy() { # queue-name policy-json
  awslocal sqs set-queue-attributes --region "$REGION" \
    --queue-url "http://sqs.${REGION}.localhost.localstack.cloud:4566/${ACCOUNT}/$1" \
    --attributes "$(POLICY="$2" python3 -c 'import json,os; print(json.dumps({"Policy": os.environ["POLICY"]}))')" >/dev/null
  echo "policy applied to $1"
}

set_policy wager-transactions.fifo "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Sid\":\"ProviderGatewaySend\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"$(user provider-gateway)\"},\"Action\":[\"sqs:SendMessage\"],\"Resource\":\"$(arn wager-transactions.fifo)\"},
 {\"Sid\":\"ConsumerReceive\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"$(user wallet-consumer)\"},\"Action\":[\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:ChangeMessageVisibility\",\"sqs:GetQueueAttributes\",\"sqs:GetQueueUrl\"],\"Resource\":\"$(arn wager-transactions.fifo)\"}
]}"

set_policy wager-transactions-dlq.fifo "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Sid\":\"ConsumerDeadLetter\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"$(user wallet-consumer)\"},\"Action\":[\"sqs:SendMessage\",\"sqs:GetQueueUrl\"],\"Resource\":\"$(arn wager-transactions-dlq.fifo)\"}
]}"

set_policy wallet-events.fifo "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Sid\":\"RelayPublish\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"$(user wallet-outbox-relay)\"},\"Action\":[\"sqs:SendMessage\",\"sqs:GetQueueAttributes\",\"sqs:GetQueueUrl\"],\"Resource\":\"$(arn wallet-events.fifo)\"}
]}"
echo "sqs provisioning done"
