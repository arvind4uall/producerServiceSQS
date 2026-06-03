# Amazon SQS Practice Project — Spring Boot Producer & Consumer

A hands-on project to understand how AWS SQS works end to end with Spring Boot.
Two services — one sends messages, one listens and logs them — with a full alerting pipeline that notifies Slack when messages land in the dead-letter queue.

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────────────────┐
                        │                   AWS Cloud                          │
                        │                                                       │
  ┌──────────────┐      │   ┌─────────────┐        ┌──────────────────────┐   │
  │   Producer   │      │   │             │        │   Consumer Service   │   │
  │   Service    │─────────▶│ order-queue │───────▶│   @SqsListener       │   │
  │  (port 8080) │      │   │             │        │   logs to console    │   │
  │              │      │   └─────────────┘        └──────────────────────┘   │
  │  POST /send  │      │         │                                            │
  │              │      │         │ (on 3rd failure)                           │
  │  POST        │      │         ▼                                            │
  │  /send-to-dlq│      │   ┌─────────────┐                                   │
  └──────┬───────┘      │   │ order-queue │                                   │
         │              │   │    -dlq     │                                   │
         └─────────────────▶│             │                                   │
                        │   └──────┬──────┘                                   │
                        │          │                                           │
                        │          ▼                                           │
                        │   ┌─────────────┐     ┌────────────┐               │
                        │   │ CloudWatch  │────▶│    SNS     │               │
                        │   │   Alarm     │     │ dlq-alerts │               │
                        │   │ (watches    │     └─────┬──────┘               │
                        │   │  DLQ count) │           │                       │
                        │   └─────────────┘           ▼                       │
                        │                      ┌─────────────┐               │
                        │                      │  Amazon Q   │               │
                        │                      │  Developer  │               │
                        │                      └─────┬───────┘               │
                        └────────────────────────────┼───────────────────────┘
                                                      │
                                                      ▼
                                              ┌──────────────┐
                                              │    Slack     │
                                              │  #dlq-alerts │
                                              │   channel    │
                                              └──────────────┘
```

---

## What We Built

```
Producer Service (port 8080)
    ├── POST /api/send           → sends message to order-queue
    └── POST /api/send-to-dlq   → sends message directly to order-queue-dlq

Consumer Service (port 8081)
    └── @SqsListener on order-queue → logs received message to console
```

---

## Queues Created on AWS

| Queue | Type | Purpose |
|---|---|---|
| `order-queue` | Standard | Main queue where producer sends messages |
| `order-queue-dlq` | Standard | Dead-letter queue — holds failed/unprocessable messages |

The two queues are linked — if a message in `order-queue` fails to be processed **3 times**, AWS automatically moves it to `order-queue-dlq`. This is called the **redrive policy**.

---

## Prerequisites

- Java 21
- Maven
- AWS CLI installed and configured
- An AWS account with an IAM user that has SQS permissions

---

## Core Concepts Explained

### What is Amazon SQS?

SQS stands for Simple Queue Service. It is a fully managed message queue service by AWS. The idea is simple — instead of Service A calling Service B directly (which creates tight coupling and can fail if B is down), Service A drops a message into a queue and Service B picks it up whenever it is ready.

This pattern is called **asynchronous communication** and it is the backbone of modern distributed systems.

Two types of queues:

| Type | Behavior | Use when |
|---|---|---|
| Standard | At-least-once delivery, best-effort ordering | High throughput, order doesn't matter |
| FIFO | Exactly-once, strict ordering | Order matters (e.g. payment steps) |

We used **Standard** because for practice ordering doesn't matter.

---

### What is a Dead-Letter Queue (DLQ)?

A DLQ is just a regular SQS queue that acts as a parking lot for messages that failed to be processed.

Here is how it works in real life:

```
1. Producer sends a message to order-queue
2. Consumer picks it up and tries to process it
3. Consumer throws an exception — processing failed
4. SQS makes the message visible again after visibility timeout
5. Consumer tries again — fails again
6. This repeats until "Maximum receives" limit is hit (we set 3)
7. On the 3rd failure SQS automatically moves it to order-queue-dlq
8. Someone gets alerted and investigates why the message keeps failing
```

The DLQ is not a failure — it is a safety net. Without it, a bad message would keep retrying forever and potentially block your queue.

In this project we also have a `/api/send-to-dlq` endpoint that sends directly to the DLQ — that is just for practice and testing purposes.

---

### What is Visibility Timeout?

When a consumer picks up a message from SQS, the message is not deleted immediately. Instead SQS hides it from other consumers for a period called **visibility timeout** (we set 30 seconds).

This gives the consumer time to process the message and delete it. If the consumer crashes mid-processing, the message becomes visible again after 30 seconds and another consumer can pick it up.

This is how SQS guarantees **at-least-once delivery**.

---

### What is Long Polling?

By default SQS uses short polling — the consumer asks "any messages?" and SQS responds immediately even if there are no messages. This wastes API calls and money.

Long polling (we set 20 seconds) means the consumer asks "any messages?" and SQS waits up to 20 seconds before responding. If a message arrives in those 20 seconds, it is returned immediately. If not, SQS returns an empty response after 20 seconds.

Long polling reduces empty API calls by up to 95% and is always recommended for production.

---

### What is CloudWatch?

CloudWatch is AWS's monitoring and observability service. Think of it as the "eyes" of your AWS infrastructure.

It continuously collects metrics from all AWS services. For SQS it tracks things like:
- How many messages are in the queue right now
- How many messages were sent/received/deleted
- How old the oldest message is

We created a **CloudWatch Alarm** that watches the metric `ApproximateNumberOfMessagesVisible` on `order-queue-dlq`. The moment this number goes above 0 (meaning a message landed in DLQ), the alarm transitions from **OK → In alarm** and triggers SNS.

Important thing to remember — **CloudWatch only notifies on state transition**. So if the alarm is already "In alarm" and you send another message, you won't get another notification until the alarm goes back to OK first. This is why we purge the DLQ after investigating.

---

### What is SNS?

SNS stands for Simple Notification Service. It is a pub/sub messaging service — you publish one message to an SNS topic and it fans out to all subscribers.

CloudWatch cannot talk to Slack directly. It only knows how to trigger SNS. SNS then delivers the alert to whoever is subscribed.

In our setup:
```
CloudWatch → SNS (dlq-alerts topic) → Amazon Q Developer → Slack
```

The real power of SNS is fan-out. Tomorrow if you want the same DLQ alert to also go to email, PagerDuty, or a Lambda function — you just add another subscription to the SNS topic. You don't touch CloudWatch at all.

---

### What is Amazon Q Developer in Chat Applications?

Previously known as AWS Chatbot. It is a service that connects AWS to your chat tools like Slack or Microsoft Teams.

In our setup it subscribes to the `dlq-alerts` SNS topic and posts notifications to the `#dlq-alerts` Slack channel whenever SNS publishes a message.

Without Amazon Q Developer you would have to build a custom Lambda + webhook to get AWS alerts into Slack. Amazon Q Developer makes this zero-code.

---

## AWS Credentials Setup

### Why We Don't Hardcode Credentials

Never put your `accessKey` and `secretKey` directly in `application.yml` or any config file. If you push that to GitHub, your AWS account is compromised within minutes — bots scan public repos constantly for exposed keys.

### The Right Way — AWS CLI Profile

AWS CLI stores your credentials locally in a file on your machine at `~/.aws/credentials`. Spring Boot reads from there automatically — you don't write a single credential in your code.

```bash
aws configure
```

It will ask you four things:

```
AWS Access Key ID:      → your access key
AWS Secret Access Key:  → your secret key
Default region name:    → ap-south-1
Default output format:  → json
```

After this, your `~/.aws/credentials` file looks like:

```ini
[default]
aws_access_key_id = AKIA...
aws_secret_access_key = xxxxxx
```

### How Spring Boot Picks This Up Automatically

Spring Cloud AWS uses something called the **Default Credential Provider Chain**. When your app starts, it checks multiple places for credentials in this order:

```
1. Java system properties           (e.g. -Daws.accessKeyId=...)
2. Environment variables            (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
3. ~/.aws/credentials file      ✅  ← this is what aws configure sets up
4. IAM role on EC2/ECS/Lambda       (used in production, zero config needed)
```

It stops at the first match. Since we ran `aws configure`, option 3 kicks in. Your `application.yml` only needs the region — nothing else:

```yaml
spring:
  cloud:
    aws:
      region:
        static: ap-south-1
```

On production (EC2 or ECS), you attach an IAM role to the instance instead. The SDK picks it up from option 4 automatically — no credentials file, no environment variables, nothing to manage.

---

## IAM Policy Attached to the User

The IAM user `sqs-local-dev` has this policy attached:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowSQSAccess",
      "Effect": "Allow",
      "Action": [
        "sqs:CreateQueue",
        "sqs:DeleteQueue",
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes",
        "sqs:SetQueueAttributes",
        "sqs:ChangeMessageVisibility",
        "sqs:ListQueues"
      ],
      "Resource": "*"
    }
  ]
}
```

`Resource: "*"` is used here because `CreateQueue` and `ListQueues` don't operate on a specific queue ARN — the queue doesn't exist yet when you're creating it. For a stricter setup in production, you'd scope the send/receive/delete actions down to specific queue ARNs.

---

## AWS Alerting Setup

### Services Used

| Service | Role |
|---|---|
| CloudWatch Alarm (DLQ-Alert) | Watches `ApproximateNumberOfMessagesVisible` on `order-queue-dlq` |
| SNS Topic (dlq-alerts) | Receives alarm trigger and fans out to subscribers |
| Amazon Q Developer | Subscribes to SNS and posts to Slack |
| Slack (#dlq-alerts) | Where the alert is received |

### How the Alert Flow Works

```
Message lands in DLQ
        ↓
CloudWatch detects it (checks every 1 minute)
        ↓
Alarm transitions OK → In alarm
        ↓
Triggers dlq-alerts SNS topic
        ↓
Amazon Q Developer picks it up
        ↓
Posts alert to Slack #dlq-alerts ⚠️
```

### Important — CloudWatch Alarm State Transition

CloudWatch only sends a notification when the alarm **transitions from OK → In alarm**. If the alarm is already "In alarm" and you send another message, no new notification is sent.

To test the alert again after it has fired:

```bash
# Step 1 — purge the DLQ so alarm goes back to OK
# Go to SQS → order-queue-dlq → Purge queue

# Step 2 — or manually reset via CLI
aws cloudwatch set-alarm-state \
  --alarm-name "DLQ-Alert" \
  --state-value OK \
  --state-reason "Manually resetting for testing" \
  --region ap-south-1

# Step 3 — send a fresh message to DLQ
curl -X POST http://localhost:8080/api/send-to-dlq \
  -H "Content-Type: application/json" \
  -d '"Fresh DLQ test!"'
```

---

## How SqsTemplate Works

`SqsTemplate` is the Spring Cloud AWS abstraction over the raw AWS SDK. Under the hood it:

1. Calls `sqs:GetQueueUrl` to resolve the queue name to a full URL
2. Serializes your message
3. Calls `sqs:SendMessage`

You just pass the queue name — the library handles everything else.

---

## How @SqsListener Works

`@SqsListener` tells Spring to start a background polling thread when the app boots up. That thread keeps calling `sqs:ReceiveMessage` on the queue. When a message arrives:

1. Spring calls your annotated method with the message body
2. If the method returns normally (no exception) → Spring automatically calls `sqs:DeleteMessage` — the message is gone
3. If the method throws an exception → Spring does NOT delete the message → SQS makes it visible again after visibility timeout → it gets retried

This is why you should never silently swallow exceptions in a listener if you want the retry and DLQ mechanism to work properly.

---

## Running the Services

```bash
# Terminal 1 — start producer on port 8080
cd producerService
mvn spring-boot:run -DskipTests

# Terminal 2 — start consumer on port 8081
cd consumerService
mvn spring-boot:run -DskipTests
```

Both services need to be running at the same time to test the full flow.

---

## API Endpoints

### Send a message to the main queue

```bash
curl -X POST http://localhost:8080/api/send \
  -H "Content-Type: application/json" \
  -d '"Hello from Arvind!"'
```

Expected response:
```
Message sent to main queue: "Hello from Arvind!"
```

Expected log in consumer terminal:
```
=== [MAIN QUEUE] Message received ===
Content: "Hello from Arvind!"
=====================================
```

### Send a message directly to DLQ

```bash
curl -X POST http://localhost:8080/api/send-to-dlq \
  -H "Content-Type: application/json" \
  -d '"This message failed processing"'
```

Expected response:
```
Message sent directly to DLQ: "This message failed processing"
```

Wait 1-2 minutes → CloudWatch detects it → SNS fires → Slack alert arrives in `#dlq-alerts`

---

## Verifying via AWS CLI

```bash
# Check that both queues exist and get their URLs
aws sqs get-queue-url --queue-name order-queue --region ap-south-1
aws sqs get-queue-url --queue-name order-queue-dlq --region ap-south-1

# Check approximate number of messages sitting in DLQ right now
aws sqs get-queue-attributes \
  --queue-url https://sqs.ap-south-1.amazonaws.com/YOUR_ACCOUNT_ID/order-queue-dlq \
  --attribute-names ApproximateNumberOfMessages \
  --region ap-south-1

# Manually reset CloudWatch alarm to OK state (useful for testing)
aws cloudwatch set-alarm-state \
  --alarm-name "DLQ-Alert" \
  --state-value OK \
  --state-reason "Manually resetting for testing" \
  --region ap-south-1
```

---

## Viewing Messages in AWS Console

1. Go to **AWS Console → SQS → Queues → order-queue**
2. Click **"Send and receive messages"** (top right)
3. Scroll down to **"Receive messages"** section
4. Click **"Poll for messages"**

If the consumer service is running, it will pick up messages almost immediately so the queue may look empty by the time you poll from the console. To see a message sitting in the queue, stop the consumer first, send a message via the API, then poll from the console.

---

## Common Issues and Fixes

**`sqs:CreateQueue` not authorized**
Your IAM user is missing the `sqs:CreateQueue` permission. Add it to the policy attached to your user.

**App crashes on startup with AWS error**
Usually means credentials are not configured. Run `aws configure list` to verify they're set, and `aws sts get-caller-identity` to confirm they're valid.

**Consumer not receiving messages**
Check that `Receive message wait time` on your queue is set to 20 seconds (long polling). With 0 seconds (short polling), the listener may miss messages or make too many empty API calls.

**`spring-cloud-aws` version conflict**
`spring-cloud-aws 3.x` works with Spring Boot `3.x` only. If you're on Spring Boot 4.x, use `spring-cloud-aws 4.x` (when available) or downgrade Spring Boot to `3.3.x`.

**SNS subscription stuck on "Pending confirmation"**
Slack webhooks cannot confirm SNS subscriptions automatically. Use Amazon Q Developer (formerly AWS Chatbot) instead of a direct HTTPS subscription — it handles the Slack authorization internally.

**CloudWatch alarm fired but no Slack message received**
Check the channel ID in Amazon Q Developer configuration. The Slack channel ID must match exactly — it is case sensitive and must be the full ID (e.g. `C0B7WJMF4H4` not `C0B7WJMF4`). Also make sure the Amazon Q app is added to your Slack channel via `/invite @Amazon Q`.

---

## Common Interview Questions

**Q: What is the difference between SQS and SNS?**

SQS is a queue — one consumer pulls and processes each message. SNS is pub/sub — one message is pushed to all subscribers simultaneously. In our project SQS handles the actual work (order processing) while SNS handles broadcasting alerts.

**Q: What happens if your consumer crashes mid-processing?**

The message becomes visible again after the visibility timeout (30 seconds in our case) and gets redelivered to another consumer. This is at-least-once delivery.

**Q: Why use a DLQ?**

Without a DLQ a poison message (one that always causes an exception) would retry forever and potentially block your queue. DLQ isolates bad messages so the rest of the queue keeps flowing normally.

**Q: What is the difference between Standard and FIFO queues?**

Standard gives higher throughput but messages may arrive out of order or more than once. FIFO guarantees strict ordering and exactly-once processing but has lower throughput limits. Use FIFO when order matters like payment processing steps.

**Q: How would you handle duplicate messages in Standard SQS?**

By making your consumer **idempotent** — meaning processing the same message twice produces the same result as processing it once. Usually done by tracking message IDs in a database and skipping already-processed ones.

**Q: What is long polling and why is it better than short polling?**

Short polling returns immediately even if no messages exist — wastes API calls. Long polling waits up to 20 seconds for a message to arrive before returning. Reduces empty API calls by up to 95% and reduces cost significantly.

**Q: What is the role of SNS in this architecture when the alert originates from SQS?**

CloudWatch watches the SQS DLQ and detects when messages land there. But CloudWatch cannot talk to Slack directly — it only knows how to trigger SNS. SNS then acts as a delivery pipe and fans out to all subscribers (in our case Amazon Q Developer which posts to Slack). This loose coupling means tomorrow you can add email or PagerDuty alerts by just adding another SNS subscription without touching CloudWatch at all.

---

## Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 21 | Language |
| Spring Boot | 3.3.5 | Application framework |
| Spring Cloud AWS | 3.1.1 | AWS integration for Spring |
| Amazon SQS | — | Message queue service |
| Amazon SNS | — | Notification fanout |
| Amazon CloudWatch | — | Monitoring and alarms |
| Amazon Q Developer | — | Slack integration (formerly AWS Chatbot) |
| Maven | 3.x | Build tool |
