# ConcreteNS (Concrete Notification Service)

ConcreteNS is a FoundationDB-backed implementation of the Amazon SNS (Simple Notification Service) API. It aims to provide high-performance, strictly ordered (FIFO), and reliable messaging infrastructure compatible with the AWS SDKs.

## Parity Status

**ConcreteNS is currently a subset of Amazon SNS.** While it supports core pub/sub workflows, several advanced features and protocols are incomplete or planned for future releases.

## Caveats & TODOs

The following features DO NOT currently work the same as Amazon SNS. We will revisit these in future updates:

### 1. Supported Protocols
*   **[TODO] Lambda**: Direct invocation of AWS Lambda functions is **NOT implemented**.
*   **[TODO] Mobile Push**: Platform Application APIs (APNS, GCM/FCM) and the `application` protocol are **NOT implemented**.
*   **[TODO] Email / SMS**: `email`, `email-json`, and `sms` protocols are **NOT implemented**.
*   **[TODO] Kinesis Firehose**: Data Firehose delivery is **NOT implemented**.
*   **[CAVEAT] HTTP/HTTPS**: Basic POST delivery works, but robust verification, auth headers, and custom timeout configurations are currently limited compared to AWS.

### 2. Message Delivery & Reliability
*   **[TODO] Custom Delivery Policies**: The `DeliveryPolicy` attribute (JSON) used to configure specific retry counts, delays, and backoff functions is **ignored**. ConcreteNS uses a default global retry policy logic.
*   **[TODO] External DLQ Redrive**: `RedrivePolicy` attributes are stored, but failed messages are currently not automatically moved to the specified **external SQS ARN**. They function closer to internal dead-letter handling.
*   **[CAVEAT] Subscription Confirmation**: The `SubscribeURL` provided in confirmation requests is currently hardcoded to `localhost`. This requires manual configuration for production deployments behind load balancers.

### 3. Security & Access Control
*   **[TODO] IAM Policies**: The `Policy` attribute is **ignored**. There is no access control system (IAM) enforcement; all clients with network access to the server have full permissions.
*   **[TODO] Server-Side Encryption**: `KmsMasterKeyId` and encryption at rest are **NOT implemented**.

### 4. API Completeness
*   **[TODO] Platform APIs**: `CreatePlatformApplication`, `CreatePlatformEndpoint`, `DeletePlatformApplication`, etc., are missing.
*   **[TODO] Phone Number APIs**: APIs for checking opt-out status are missing.

### 5. Strict FIFO Ordering
*   **[CAVEAT] Concurrency**: While strict ordering is verified for serial publishers, high-concurrency atomic sequencing relies on FDB's transactional guarantees. Extreme edge cases in clock synchronization across distributed workers (if scaled) may require `Versionstamp` implementation (planned).

## Getting Started

```bash
# Run the server
go run cmd/concretens/main.go
```

The server listens on port 8080 by default. You can use `aws sns --endpoint-url http://localhost:8080 ...` to interact with it.
