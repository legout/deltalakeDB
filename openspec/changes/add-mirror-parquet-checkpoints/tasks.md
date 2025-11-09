## 1. Implementation
- [ ] 1.1 Implement Parquet checkpoint serializer matching Delta schema
- [ ] 1.2 Emit checkpoint for version `V` when `V % interval == 0` after JSON is confirmed
- [ ] 1.3 Verify checkpoint round-trips via replay tests

## 2. Validation
- [ ] 2.1 External readers can load from checkpoint + subsequent JSON
- [ ] 2.2 Checkpoint size and row group layout meet guidance

## 3. Dependencies
- Depends on `add-mirror-json-after-commit`

