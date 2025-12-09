# Fix Duplicate Data Issue

## Problem

The pipeline was storing the same data repeatedly in Snowflake because:
1. Kafka consumer wasn't committing offsets, causing it to re-read the same messages
2. `commit_dag` had a fallback that picked the latest file, reprocessing old data
3. No deduplication in Snowflake - same records could be inserted multiple times

## Solutions Implemented

### 1. Fixed Kafka Consumer to Commit Offsets

**File:** `dags/utils/kafka_utils.py`

- Added explicit offset commit after consuming messages
- This ensures Kafka marks messages as consumed, preventing re-reading

**Change:**
```python
# Commit offsets after consuming messages to avoid reprocessing
if messages:
    try:
        consumer.commit(asynchronous=False)
        logger.info(f"Committed offsets for {len(messages)} messages")
    except Exception as e:
        logger.warning(f"Failed to commit offsets: {e}")
```

### 2. Removed Fallback in commit_dag

**File:** `dags/commit_dag.py`

- Removed the fallback that used `get_latest_file()` 
- Now only processes files explicitly passed from `clean_dag`
- Prevents reprocessing old files

**Change:**
- Removed "Try 3: Latest file in cleaned directory (fallback)"
- Added better error message explaining the file must come from clean_dag

### 3. Added Deduplication in Snowflake

**File:** `dags/commit_dag.py`

- Changed from `COPY INTO` to `MERGE` statement
- Uses staging table approach
- Deduplicates based on: `USER_ID + ENTITY_ID + OCCURRED_AT`

**How it works:**
1. Creates temporary staging table
2. Copies data to staging table
3. MERGEs staging into target table
4. Only inserts records that don't already exist (based on unique combination)

**Change:**
```python
MERGE INTO target_table AS target
USING staging_table AS source
ON target.USER_ID = source.USER_ID 
   AND target.ENTITY_ID = source.ENTITY_ID 
   AND target.OCCURRED_AT = source.OCCURRED_AT
WHEN NOT MATCHED THEN
    INSERT (...)
```

### 4. Added Message Check in polls_dag

**File:** `dags/polls_dag.py`

- Added `ShortCircuitOperator` to check if messages were received
- Only triggers `clean_dag` if new messages exist
- Prevents unnecessary processing when no new data

## Result

Now the pipeline will:
- ✅ Only read new messages from Kafka (offsets committed)
- ✅ Only process files from current run (no fallback to old files)
- ✅ Skip duplicate records in Snowflake (MERGE deduplication)
- ✅ Skip downstream processing when no new messages

## Testing

After deploying these changes:

1. **Restart services:**
   ```bash
   docker compose restart
   ```

2. **Monitor logs:**
   ```bash
   docker compose logs -f airflow-scheduler | grep -i "kafka\|merge\|deduplicate"
   ```

3. **Check Snowflake:**
   - Verify no duplicate rows
   - Check that only new data is being inserted
   - Monitor row counts to ensure they're increasing, not repeating

## Notes

- The deduplication key is: `USER_ID + ENTITY_ID + OCCURRED_AT`
- If you need different deduplication logic, modify the MERGE ON clause
- Kafka offsets are committed after each successful poll
- Old files in `data/raw` and `data/cleaned` won't be reprocessed

