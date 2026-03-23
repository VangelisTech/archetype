# Mind Extraction: Multi-Project Theory-of-Mind

This module expands mind extraction to process conversations from all `.claude/projects/*` directories, enabling cross-project memory aggregation and unified theory-of-mind profiling.

## Features

- **Multi-Project Scanning**: Automatically discovers and processes all `.claude/projects/*/` directories
- **Deduplication**: Prevents duplicate memories using content hashing
- **Incremental Processing**: Only processes new conversations since last run
- **Unified Profiling**: Aggregates insights across all projects into a comprehensive profile
- **LanceDB Persistence**: Stores memories with efficient querying capabilities

## Components

### ProjectContext
Tracks which project/repository a conversation belongs to, enabling project-specific insights and cross-project analysis.

### Dedup
Content-based deduplication using SHA256 hashing to ensure the same user message doesn't create duplicate memories across sessions.

### MemoryAggregator
Merges memory extractions across projects and generates unified theory-of-mind profiles with:
- Technical insights
- Behavioral patterns  
- User preferences
- Project-specific breakdowns

## Usage

### Basic Usage
```python
from examples.mind.main import main

# Process all .claude projects and generate unified profile
profile = main()
print(f"Processed {profile['total_memories']} memories across {len(profile['projects_covered'])} projects")
```

### Incremental Processing
```python
# Only process new conversations since last run
profile = main(incremental=True)
```

### Custom Base Path
```python
from pathlib import Path

# Search for .claude directories in custom location
profile = main(base_path=Path("/custom/path"))
```

### Command Line Interface
```bash
# Basic extraction
python -m examples.mind.main

# Incremental processing
python -m examples.mind.main --incremental

# Custom output location
python -m examples.mind.main --output /path/to/profile.json

# Custom search path
python -m examples.mind.main --base-path /custom/path
```

## Example Output

The unified profile includes:
```json
{
  "total_memories": 150,
  "projects_covered": ["project-a", "project-b", "project-c"],
  "memory_types": {
    "technical": 45,
    "preference": 30,
    "behavioral": 25,
    "general": 50
  },
  "project_breakdown": {
    "project-a": 60,
    "project-b": 45,
    "project-c": 45
  },
  "high_confidence_memories": [...],
  "technical_insights": [...],
  "behavioral_patterns": [...],
  "preferences": [...]
}
```

## Storage

- Memories are persisted to LanceDB at `~/.archetype/mind_extraction/memories.lance`
- Profiles are exported to JSON at `~/.archetype/mind_extraction/unified_mind_profile.json`
- Incremental run state is tracked in `~/.archetype/mind_extraction/last_run.json`

## Architecture

```
examples/mind/
├── components.py    # ProjectContext, Dedup, Memory components
├── scanner.py       # Multi-directory conversation scanning
├── extractor.py     # Memory extraction from conversations
├── aggregator.py    # Cross-project memory aggregation  
├── storage.py       # LanceDB persistence layer
└── main.py          # CLI and orchestration
```