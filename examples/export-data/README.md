# Export Data

Export daily brand performance data from the Omnia API into flat JSON files ready for BI tools (Looker Studio, BigQuery, Tableau, etc.).

The script fetches share of voice, visibility, and citations at three levels of granularity: brand, topic, and prompt. Each output row is fully denormalized so you can load the files directly into any analytics tool without joins.

## Quick start

```bash
# Install tsx if you haven't already
npm install -g tsx

# Set your API key
export OMNIA_API_KEY="ot_your-token-here"

# Run the export (defaults to last 30 days, all topics and prompts)
tsx export-data.ts --brandId YOUR_BRAND_ID
```

To find your brand ID, call `GET /brands` with your API key:

```bash
curl https://app.useomnia.com/api/v1/brands \
  -H "Authorization: Bearer $OMNIA_API_KEY"
```

## Options

| Flag                       | Description                 | Default     |
| -------------------------- | --------------------------- | ----------- |
| `--brandId <uuid>`         | Brand to export (required)  | --          |
| `--startDate <YYYY-MM-DD>` | Start of date range         | 30 days ago |
| `--endDate <YYYY-MM-DD>`   | End of date range           | Yesterday   |
| `--topicIds <id,id,...>`   | Only export these topics    | All topics  |
| `--promptIds <id,id,...>`  | Only export these prompts   | All prompts |
| `--outputDir <path>`       | Where to write output files | `./export`  |
| `--concurrency <1-20>`     | Parallel API requests       | 4           |

## Output

The script produces 9 JSON files (3 metrics x 3 levels) plus a manifest:

```
export/
├── manifest.json
├── brand/
│   ├── share-of-voice.json
│   ├── visibility.json
│   └── citations.json
├── topic/
│   ├── share-of-voice.json
│   ├── visibility.json
│   └── citations.json
└── prompt/
    ├── share-of-voice.json
    ├── visibility.json
    └── citations.json
```

### Row structure

Every row is self-contained. Context fields are denormalized into each row so the files can be loaded directly into BI tools.

**Brand-level share of voice** (`brand/share-of-voice.json`):

```json
{
  "brandId": "abc-123",
  "brandName": "My Brand",
  "date": "2025-06-15",
  "mentionedBrand": "Competitor A",
  "mentionedDomain": "competitor.com",
  "mentionCount": 28,
  "rank": 2,
  "relationship": "competitor",
  "shareOfVoice": 0.104
}
```

**Topic-level** rows add `topicId` and `topicName`. **Prompt-level** rows add `promptId` and `promptQuery` on top of that.

**Citations** have a different shape:

```json
{
  "brandId": "abc-123",
  "brandName": "My Brand",
  "date": "2025-06-15",
  "citedDomain": "example.com",
  "citedUrl": "https://example.com/article",
  "citedTitle": "Some Article",
  "totalCitations": 5,
  "shareOfVoice": 0.03,
  "sourceType": "third_party"
}
```

### Manifest

`manifest.json` contains export metadata: date range, brand info, the full list of topics and prompts, and row counts per metric. Use it to verify the export completed correctly or to build a lookup table for topic/prompt IDs.

## How it works

1. **Discovery**: Fetches the brand, then auto-discovers all topics and prompts under it (unless filtered with `--topicIds` / `--promptIds`)
2. **Daily aggregates**: For each day in the date range, fetches all 9 metric/level combinations for every entity. Uses concurrent workers (configurable with `--concurrency`) and handles pagination automatically.
3. **Rate limiting**: Tracks the `X-RateLimit-Remaining` header and pauses when running low. Retries on 429 (rate limited) and 5xx (server error) with exponential backoff. Aborts after 5 consecutive errors to avoid hammering the API.
4. **Output**: Writes denormalized JSON files organized by level (brand/topic/prompt).

## Examples

```bash
# Export last 30 days (default)
tsx export-data.ts --brandId abc-123

# Export a specific month
tsx export-data.ts --brandId abc-123 \
  --startDate 2025-06-01 \
  --endDate 2025-06-30

# Export only specific topics
tsx export-data.ts --brandId abc-123 \
  --topicIds topic-1,topic-2

# Increase parallelism (careful with rate limits)
tsx export-data.ts --brandId abc-123 --concurrency 8
```

## Requirements

- Node.js 18+ (uses the built-in `fetch` API)
- [tsx](https://github.com/privatenumber/tsx) (`npm install -g tsx`)
- An Omnia API key set as `OMNIA_API_KEY` environment variable

The script has zero npm dependencies. It uses only `node:fs`, `node:path`, and the global `fetch` API.
