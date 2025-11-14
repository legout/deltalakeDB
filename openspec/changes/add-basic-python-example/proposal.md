# Add Basic Python Example

## Summary

Add a minimal, beginner-friendly Python example that demonstrates the core functionality of DeltaLake DB Python with minimal setup and complexity.

## Problem Statement

While DeltaLake DB Python has comprehensive examples for advanced features (async I/O, multi-table transactions, performance optimization, etc.), it lacks a truly basic "getting started" example that new users can run immediately to understand the core value proposition. Current examples are:

- Too complex for beginners (400+ lines of code)
- Focused on specific advanced features
- Require significant setup and understanding of the architecture
- Don't demonstrate the simplest possible use case

New users need a minimal example that shows:
1. How to install and import the package
2. How to connect to a table
3. How to perform basic read/write operations
4. How to verify it worked

## Proposed Solution

Create a `basic_example.py` that demonstrates the absolute essentials:

```python
#!/usr/bin/env python3
"""
Basic DeltaLake DB Python Example

A minimal example to get started with DeltaLake DB Python.
Shows how to create a table, write data, and read it back.
"""

import pandas as pd
import deltalakedb as dl

def main():
    # Create sample data
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    
    # Create/connect to a table using in-memory SQLite
    table = dl.create("deltasql://sqlite:///:memory:", "users")
    
    # Write data
    table.write(data, mode="overwrite")
    
    # Read data back
    result = table.read().to_pandas()
    print(result)
    
    print("✅ Basic example completed successfully!")

if __name__ == "__main__":
    main()
```

## Scope

### In Scope
- Create a single `basic_example.py` file in `crates/python/examples/`
- Focus on the most common use case: create table → write data → read data
- Use in-memory SQLite for zero setup
- Include clear comments and explanations
- Update the README to reference this example

### Out of Scope
- Advanced features (transactions, async, performance optimization)
- Database setup/configuration
- Production deployment patterns
- Integration with other tools

## Success Criteria

1. New users can run the example with zero setup beyond installing the package
2. Example runs in under 5 seconds
3. Example demonstrates the core value proposition clearly
4. Code is under 50 lines and easily understandable
5. Example is referenced in the main README

## Alternatives Considered

1. **Enhance existing examples**: Would still be too complex for true beginners
2. **Create interactive tutorial**: More effort than needed for this request
3. **Add to documentation**: Users prefer executable examples over docs

## Dependencies

- None (uses existing functionality)
- Requires pandas (already a dependency)

## Impact Assessment

- **Development effort**: Low (1-2 hours)
- **Maintenance effort**: Minimal (simple, stable example)
- **User experience**: High (removes barrier to entry)
- **Documentation**: Improves discoverability

## Implementation Notes

- Use in-memory SQLite to eliminate setup requirements
- Keep dependencies minimal (pandas only)
- Include error handling for common issues
- Add clear success indicators
- Use the simplest API surface possible