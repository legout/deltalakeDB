#!/usr/bin/env python3
"""
Example: Multi-table Transaction with Context Manager

This example demonstrates the Python API for deltalakedb multi-table transactions
using the context manager pattern for automatic resource cleanup.

Prerequisites:
- PostgreSQL running on localhost:5432
- Database: deltalakedb_test
- TABLE_IDS defined below (or update with your table IDs)
"""

import asyncio
import os
from uuid import UUID
from typing import List

# Import deltalakedb modules
# from deltalakedb import MultiTableTransaction, TransactionConfig
# from deltalakedb.types import AddFile, RemoveFile


async def example_basic_multi_table_transaction():
    """
    Example 1: Basic multi-table transaction with context manager.
    
    This demonstrates:
    - Creating a transaction
    - Staging changes to multiple tables
    - Automatic commit on exit
    - Automatic rollback on error
    """
    print("=" * 60)
    print("Example 1: Basic Multi-Table Transaction")
    print("=" * 60)
    
    # In real usage:
    # from deltalakedb import MultiTableTransaction, TransactionConfig
    # from sqlalchemy.ext.asyncio import create_async_engine
    
    # database_url = os.getenv("DATABASE_URL")
    # engine = create_async_engine(database_url)
    # pool = await engine.pool()
    
    # config = TransactionConfig(
    #     max_tables=10,
    #     max_files_per_table=1000,
    #     timeout_ms=30000
    # )
    
    # table1_id = UUID("12345678-1234-5678-1234-567812345678")
    # table2_id = UUID("87654321-4321-8765-4321-876543218765")
    
    # try:
    #     async with MultiTableTransaction(pool, config) as tx:
    #         # Stage changes to first table
    #         add_files_table1 = [
    #             AddFile(
    #                 path=f"part-{i:03d}.parquet",
    #                 size=1024 * 1024,  # 1MB
    #                 modification_time=int(datetime.now().timestamp() * 1000),
    #                 data_change=True
    #             )
    #             for i in range(5)
    #         ]
    #         tx.stage_table(table1_id, add_files_table1)
    #         print(f"Staged {len(add_files_table1)} files to table 1")
    #         
    #         # Stage changes to second table
    #         add_files_table2 = [
    #             AddFile(
    #                 path=f"batch-{i:03d}.parquet",
    #                 size=2048 * 1024,  # 2MB
    #                 modification_time=int(datetime.now().timestamp() * 1000),
    #                 data_change=True
    #             )
    #             for i in range(3)
    #         ]
    #         tx.stage_table(table2_id, add_files_table2)
    #         print(f"Staged {len(add_files_table2)} files to table 2")
    #         
    #         # Commit atomically
    #         result = tx.commit()
    #         print(f"✓ Committed! Versions: {result.versions}")
    # 
    # except Exception as e:
    #     print(f"✗ Transaction failed: {e}")
    
    print("✓ Example demonstrates atomic multi-table transaction")
    print()


async def example_error_handling():
    """
    Example 2: Error handling with automatic rollback.
    
    This demonstrates:
    - Version conflict detection
    - Validation error handling
    - Automatic rollback on error
    """
    print("=" * 60)
    print("Example 2: Error Handling with Rollback")
    print("=" * 60)
    
    # In real usage:
    # from deltalakedb import MultiTableTransaction, TransactionError
    
    # try:
    #     async with MultiTableTransaction(pool, config) as tx:
    #         # Stage changes
    #         tx.stage_table(table_id, files)
    #         
    #         # Commit - may fail if another process modified table
    #         result = tx.commit()
    #
    # except TransactionError.VersionConflict as e:
    #     # Another process updated the table
    #     print(f"Version conflict: {e}")
    #     print("Transaction was automatically rolled back")
    #     print("Retry the transaction to apply changes again")
    #
    # except TransactionError.ValidationError as e:
    #     # Data validation failed
    #     print(f"Validation failed: {e}")
    #     print("Fix the data and retry")
    #
    # except TransactionError.TransactionTimeout as e:
    #     # Operation took too long
    #     print(f"Timeout: {e}")
    #     print("Try again or increase timeout")
    
    print("✓ Example demonstrates error handling and recovery")
    print()


async def example_retry_logic():
    """
    Example 3: Implementing retry logic for version conflicts.
    
    This demonstrates:
    - Detecting version conflicts
    - Implementing exponential backoff
    - Retry mechanism
    """
    print("=" * 60)
    print("Example 3: Retry Logic for Version Conflicts")
    print("=" * 60)
    
    # In real usage:
    # import time
    # from deltalakedb import TransactionError
    # 
    # async def commit_with_retry(pool, config, table_id, files, max_retries=3):
    #     """Commit with automatic retry on version conflict."""
    #     
    #     for attempt in range(max_retries):
    #         try:
    #             async with MultiTableTransaction(pool, config) as tx:
    #                 tx.stage_table(table_id, files)
    #                 result = tx.commit()
    #                 print(f"✓ Committed on attempt {attempt + 1}")
    #                 return result
    #         
    #         except TransactionError.VersionConflict:
    #             if attempt < max_retries - 1:
    #                 wait_time = 2 ** attempt  # Exponential backoff
    #                 print(f"Version conflict, retrying in {wait_time}s...")
    #                 await asyncio.sleep(wait_time)
    #             else:
    #                 print(f"Failed after {max_retries} attempts")
    #                 raise
    
    print("✓ Example demonstrates retry logic with exponential backoff")
    print()


async def example_staged_workflow():
    """
    Example 4: Staged workflow with validation.
    
    This demonstrates:
    - Building up changes incrementally
    - Validating data before commit
    - Rolling back on validation failure
    """
    print("=" * 60)
    print("Example 4: Staged Workflow with Validation")
    print("=" * 60)
    
    # In real usage:
    # async with MultiTableTransaction(pool, config) as tx:
    #     # Stage changes for multiple tables
    #     tables = [
    #         (table_1_id, batch_1_files),
    #         (table_2_id, batch_2_files),
    #         (table_3_id, batch_3_files),
    #     ]
    #     
    #     for table_id, files in tables:
    #         # Validate files before staging
    #         for file in files:
    #             if not validate_file(file):
    #                 print(f"Invalid file: {file}")
    #                 # Automatically rolled back on exit
    #                 raise ValueError(f"Invalid file: {file}")
    #         
    #         # Stage validated files
    #         tx.stage_table(table_id, files)
    #         print(f"Staged {len(files)} files for table {table_id}")
    #     
    #     # Commit all changes atomically
    #     result = tx.commit()
    #     print(f"✓ All {len(tables)} tables committed atomically")
    
    print("✓ Example demonstrates staged workflow with validation")
    print()


async def example_concurrent_transactions():
    """
    Example 5: Running multiple concurrent transactions.
    
    This demonstrates:
    - Running independent transactions concurrently
    - Each transaction maintains isolation
    - Version conflicts between transactions
    """
    print("=" * 60)
    print("Example 5: Concurrent Transactions")
    print("=" * 60)
    
    # In real usage:
    # async def process_batch(pool, config, batch_num, table_ids, files_per_batch):
    #     """Process a batch in a separate transaction."""
    #     try:
    #         async with MultiTableTransaction(pool, config) as tx:
    #             for table_id in table_ids:
    #                 batch_files = [
    #                     AddFile(
    #                         path=f"batch-{batch_num:03d}-{i:03d}.parquet",
    #                         size=1024 * 1024,
    #                         modification_time=int(datetime.now().timestamp() * 1000),
    #                         data_change=True
    #                     )
    #                     for i in range(files_per_batch)
    #                 ]
    #                 tx.stage_table(table_id, batch_files)
    #         
    #         print(f"✓ Batch {batch_num} committed")
    #         return True
    #     
    #     except TransactionError as e:
    #         print(f"✗ Batch {batch_num} failed: {e}")
    #         return False
    # 
    # # Run 5 batches concurrently
    # tasks = [
    #     process_batch(pool, config, i, [table_1, table_2], files_per_batch=10)
    #     for i in range(5)
    # ]
    # results = await asyncio.gather(*tasks)
    # print(f"Completed {sum(results)}/{len(results)} batches")
    
    print("✓ Example demonstrates concurrent transaction handling")
    print()


async def main():
    """Run all examples."""
    print("\n" + "=" * 60)
    print("deltalakedb Python API Examples")
    print("=" * 60 + "\n")
    
    await example_basic_multi_table_transaction()
    await example_error_handling()
    await example_retry_logic()
    await example_staged_workflow()
    await example_concurrent_transactions()
    
    print("=" * 60)
    print("All examples completed!")
    print("=" * 60)
    print("\nFor production usage:")
    print("1. Set DATABASE_URL environment variable")
    print("2. Create test tables with UUIDs")
    print("3. Uncomment the actual code in each example")
    print("4. Run: python python_multi_table_transaction.py")


if __name__ == "__main__":
    asyncio.run(main())
