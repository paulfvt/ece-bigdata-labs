# Lab 2 – Object storage with S3: answers

## Q1. Why are the credentials in a Secret and not in the ConfigMap?

The credentials are private. A ConfigMap is for normal settings, like the endpoint or the bucket name, and anyone can read it in clear text. A Secret is made for private data: we can give the right to read it to fewer people. So my keys are better protected.

## Q2. What happens if the Job runs again tomorrow? How does a production platform give credentials to a Job?

Tomorrow, the Job will fail. The Onyxia token expires after a few hours, so the upload gets an error like `ExpiredToken`.

In production, nobody copies keys by hand. The Job has its own identity (a service account), and the platform gives it short credentials automatically, for example with an IAM role or a tool like Vault. The credentials are renewed all the time.

## Q3. How can we turn this Job into a daily ingestion?

I can use a `CronJob` instead of a `Job`, with a schedule like `0 2 * * *` (every day at 2 am).

But two things must change:

- The data must not be in a ConfigMap. The Job should read new data from the source system every day.
- The credentials must be renewed automatically, not copied by hand.

I can also put the date in the file name, like `bronze/orders/2026-10-03.csv`, to keep the data of each day.
