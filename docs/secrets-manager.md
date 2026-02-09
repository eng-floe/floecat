# Secrets Manager integration

This document describes how Floecat uses AWS Secrets Manager, the required IAM permissions, and optional hardening with per-account role assumption.

## Overview

Floecat stores secret payloads in AWS Secrets Manager using a key format:

`accounts/<accountId>/<secretType>/<secretId>`

Each secret is tagged with:

- `AccountId` = the account id passed by Floecat

These tags support ABAC-style policies.

Connector AuthCredentials are written to Secrets Manager and removed from connector records before
they are persisted to the pointer store. Responses from the Connectors service mask any sensitive
auth fields so callers never see raw tokens, keys, or client secrets.

## Configuration

Optional role assumption:

- `floecat.secrets.aws.role-arn` (optional)

If set, Floecat assumes this role per account operation and tags the STS session with `AccountId=<accountId>`. If unset, Floecat uses its default AWS credentials for all operations.

Set via system property:

```bash
-Dfloecat.secrets.aws.role-arn=arn:aws:iam::123456789012:role/floecat-secrets
```

Or via environment variable:

```bash
export FLOECAT_SECRETS_AWS_ROLE_ARN=arn:aws:iam::123456789012:role/floecat-secrets
```

## Required IAM permissions (base)

The IAM identity Floecat runs under must be allowed to call Secrets Manager APIs. A minimal base policy looks like:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SecretsManagerBase",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:CreateSecret",
        "secretsmanager:GetSecretValue",
        "secretsmanager:PutSecretValue",
        "secretsmanager:DeleteSecret",
        "secretsmanager:TagResource"
      ],
      "Resource": "*"
    }
  ]
}
```

If you can scope the `Resource` to your Secrets Manager ARNs, do so.

## Optional hardening: per-account ABAC with role assumption

When `floecat.secrets.aws.role-arn` is set, Floecat will assume that role and attach an STS session tag `AccountId=<accountId>` for each operation. This allows you to enforce that secrets can only be accessed when the caller's `AccountId` tag matches the secret's `AccountId` tag.

### Role trust policy

The role referenced by `floecat.secrets.aws.role-arn` must trust the Floecat base identity and allow session tagging:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:role/floecat-base"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {"sts:TagSession": "true"}
      }
    }
  ]
}
```

### Role permissions policy

Attach a policy that enforces tag alignment:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SecretsByAccountTag",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:CreateSecret",
        "secretsmanager:GetSecretValue",
        "secretsmanager:PutSecretValue",
        "secretsmanager:DeleteSecret",
        "secretsmanager:TagResource"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "secretsmanager:ResourceTag/AccountId": "${aws:PrincipalTag/AccountId}"
        }
      }
    }
  ]
}
```

This prevents any caller from reading or writing secrets unless the secret is tagged with the same `AccountId` value as the session.
