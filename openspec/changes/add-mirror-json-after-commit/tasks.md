## 1. Implementation
- [ ] 1.1 Add mirror serializer for JSON commit format
- [ ] 1.2 Implement post-commit write to `_delta_log/NNNNNNNNNN.json`
- [ ] 1.3 Add mirror status table and retry loop

## 2. Validation
- [ ] 2.1 JSON matches canonical Delta commit content for a sample commit
- [ ] 2.2 Induced failure is retried until success

## 3. Dependencies
- Depends on `add-sql-schema-core` (source of actions)

