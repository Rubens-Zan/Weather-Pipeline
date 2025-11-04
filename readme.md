# Weather Chalenge Pipeline

## Usage
### Syncing
```bash
uv sync
```
### Running the Pipeline

The CLI provides four commands:

```bash
# Run individual stages
uv run cli.py parametrize    # Generate task list from config
uv run cli.py scrape         # Fetch weather data from API
uv run cli.py transform      # Transform and merge data

# Run the complete pipeline
uv run cli.py pipeline       # Execute all stages sequentially
```

## Output
Parquet Files