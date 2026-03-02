# Export Data

Export daily prompt-level performance data from the Omnia API into flat JSON files ready for BI tools (Looker Studio, BigQuery, Tableau, etc.).

The script fetches share of voice, visibility, and citations at prompt granularity — the most detailed level available. Data is queried **per engine** (Google AI Overviews, Google AI Mode, Perplexity, OpenAI) and every output row is fully denormalized — brand info, topic properties, and engine are included in every row so you can load the files directly into any analytics tool without joins.

Since the data is exported at the finest grain, you can roll up metrics by any dimension in your BI tool: group by `topicId` for topic-level aggregates, by `brandId` for brand-level, by `engine` for engine comparisons, or any combination.

## Quick start

```bash
# Install tsx if you haven't already
npm install -g tsx

# Set your API key
export OMNIA_API_KEY="ot_your-token-here"

# Run the export (defaults to today, all topics and prompts, all engines)
tsx export-data.ts --brandId YOUR_BRAND_ID
```

To find your brand ID, call `GET /brands` with your API key:

```bash
curl https://app.useomnia.com/api/v1/brands \
  -H "Authorization: Bearer $OMNIA_API_KEY"
```

## Options

| Flag                       | Description                 | Default      |
| -------------------------- | --------------------------- | ------------ |
| `--brandId <uuid>`         | Brand to export (required)  | --           |
| `--startDate <YYYY-MM-DD>` | Start of date range         | Today        |
| `--endDate <YYYY-MM-DD>`   | End of date range           | Today        |
| `--topicIds <id,id,...>`   | Only export these topics    | All topics   |
| `--promptIds <id,id,...>`  | Only export these prompts   | All prompts  |
| `--engines <e1,e2,...>`    | AI engines to query         | All engines  |
| `--outputDir <path>`       | Where to write output files | `./export`   |
| `--concurrency <1-10>`     | Parallel API requests       | 4            |

### Engines

Aggregates are queried once per engine. By default all four engines are queried:

- `google-ai-overviews`
- `google-ai-mode`
- `perplexity`
- `openai`

Use `--engines` to narrow to specific engines (e.g. `--engines perplexity,openai`).

## Output

The script produces 3 JSON files (one per metric) plus a manifest:

```
export/
├── manifest.json
├── share-of-voice.json
├── visibility.json
└── citations.json
```

### Row structure

Every row is self-contained and fully denormalized. Brand info, topic properties, prompt info, and the engine are included in every row so the files can be loaded directly into BI tools without joins.

**Share of voice** (`share-of-voice.json`):

```json
{
  "brandId": "abc-123",
  "brandName": "My Brand",
  "brandDomain": "mybrand.com",
  "topicId": "topic-1",
  "topicName": "AI Assistants",
  "topicLocation": "us",
  "topicTags": ["core"],
  "topicType": "Non-Branded",
  "promptId": "prompt-1",
  "promptQuery": "best ai assistant for coding",
  "engine": "perplexity",
  "date": "2025-06-15",
  "mentionedBrand": "Competitor A",
  "mentionedDomain": "competitor.com",
  "mentionCount": 28,
  "rank": 2,
  "relationship": "competitor",
  "shareOfVoice": 0.104
}
```

**Citations** (`citations.json`) have a different shape:

```json
{
  "brandId": "abc-123",
  "brandName": "My Brand",
  "brandDomain": "mybrand.com",
  "topicId": "topic-1",
  "topicName": "AI Assistants",
  "topicLocation": "us",
  "topicTags": ["core"],
  "topicType": "Non-Branded",
  "promptId": "prompt-1",
  "promptQuery": "best ai assistant for coding",
  "engine": "openai",
  "date": "2025-06-15",
  "citedDomain": "example.com",
  "citedUrl": "https://example.com/article",
  "citedTitle": "Some Article",
  "totalCitations": 5,
  "shareOfVoice": 0.03,
  "sourceType": "third_party"
}
```

**Visibility** (`visibility.json`) has the same context fields as share of voice, with `visibility` and `rank` instead of `mentionCount` and `shareOfVoice`.

### Manifest

`manifest.json` contains export metadata: date range, engines, brand info, the full list of topics (including location, tags, and topic type) and prompts, and row counts per metric. Use it to verify the export completed correctly or to build a lookup table for topic/prompt IDs.

## How it works

1. **Discovery**: Fetches the brand, then auto-discovers all topics (with their location, tags, and topic type) and prompts under it (unless filtered with `--topicIds` / `--promptIds`).
2. **Daily aggregates**: For each day in the date range, fetches all metric/engine combinations for every prompt. Each engine is queried separately so rows contain per-engine breakdowns. Uses concurrent workers (configurable with `--concurrency`) and handles pagination automatically.
3. **Retries**: Retries on 429 (rate limited) using the `Retry-After` header, and on 5xx (server error) with exponential backoff. Only 5xx errors count toward the circuit breaker, which aborts after 5 consecutive server failures.
4. **Output**: Writes denormalized JSON files — one per metric. Brand info, topic properties, and engine are denormalized into every row.

## Examples

```bash
# Export today's data (all engines, all topics/prompts)
tsx export-data.ts --brandId abc-123

# Export a specific month
tsx export-data.ts --brandId abc-123 \
  --startDate 2025-06-01 \
  --endDate 2025-06-30

# Export only specific topics
tsx export-data.ts --brandId abc-123 \
  --topicIds topic-1,topic-2

# Export only Perplexity and OpenAI data
tsx export-data.ts --brandId abc-123 \
  --engines perplexity,openai

# Increase parallelism (careful with rate limits)
tsx export-data.ts --brandId abc-123 --concurrency 8
```

## Performance and concurrency

The script processes one day at a time. Within each day, it fetches multiple metrics, prompts, and engines in parallel using a worker pool controlled by `--concurrency` (default: 4).

The total number of API calls depends on how many prompts and engines your export includes. For a brand with 15 topics and 75 prompts across all 4 engines, exporting a full month looks roughly like this:

- **75 prompts** x 3 metrics x 4 engines = 900 API calls per day
- **31 days** = ~27,900 API calls total

At the default concurrency of 4, this takes several minutes. Higher concurrency (e.g. `--concurrency 8`) reduces the time but consumes rate limit tokens faster. You can also reduce the number of engines with `--engines` to cut the call count proportionally.

If the API returns repeated 5xx errors, the script aborts after 5 consecutive failures. If this happens, wait a moment and retry with lower concurrency.

To export faster for large date ranges, consider splitting the work into multiple runs with non-overlapping date ranges.

## Requirements

- Node.js 18+ (uses the built-in `fetch` API)
- [tsx](https://github.com/privatenumber/tsx) (`npm install -g tsx`)
- An Omnia API key set as `OMNIA_API_KEY` environment variable

The script has zero npm dependencies. It uses only `node:fs`, `node:path`, and the global `fetch` API.
