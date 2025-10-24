**When violations spike**: check last ingest partition, roll back or quarantine, re-run validation.
**When DAG fails**: inspect logs, re-run specific task with same date.
**When schema changes**: update rules & version configs.
