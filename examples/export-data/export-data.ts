#!/usr/bin/env tsx

/**
 * Omnia Data Export Script
 *
 * Exports daily brand performance data (share of voice, visibility, citations)
 * from the Omnia public API at brand, topic, and prompt granularity.
 *
 * Designed as a reference implementation for integrating Omnia analytics data
 * with BI tools (Looker Studio, BigQuery, Tableau, etc.) or ingesting into
 * external databases.
 *
 * Usage:
 *   OMNIA_API_KEY=ot_xxx npx tsx export-data.ts --brandId <uuid> [options]
 *
 * Documentation: https://docs.useomnia.com
 */

import fs from "node:fs";
import path from "node:path";

// ---------------------------------------------------------------------------
// Types — API Response Shapes
// ---------------------------------------------------------------------------

interface ApiPagination {
  page: number;
  pageSize: number;
  totalItems: number;
}

interface PaginationLinks {
  prev?: string;
  next?: string;
}

interface PaginatedResponse<T> {
  data: T;
  pagination: ApiPagination;
  links: PaginationLinks;
}

interface SingleResponse<T> {
  data: T;
}

interface ApiError {
  error: { code: number; description: string };
}

// ---------------------------------------------------------------------------
// Types — Domain Entities
// ---------------------------------------------------------------------------

interface Brand {
  id: string;
  name: string;
  domain: string;
  mainLocation: string | null;
  createdAt: string;
}

interface Topic {
  id: string;
  name: string;
  location: string;
  status: string;
  tags: string[];
  topicSource: string;
  topicType: string;
  createdAt: string;
}

interface Prompt {
  id: string;
  query: string;
  topicId: string;
  isMonitoringActive: boolean;
  location: string;
  tags: string[];
  createdAt: string;
}

// ---------------------------------------------------------------------------
// Types — Flat Export Rows (Denormalized for BI)
//
// Each level extends the previous with additional context columns.
// This ensures every row is self-contained for BI tools. No joins needed.
// ---------------------------------------------------------------------------

type FlatRow = Record<string, string | number | boolean | null>;

// ---------------------------------------------------------------------------
// Types — Export Manifest
// ---------------------------------------------------------------------------

interface ExportManifest {
  exportedAt: string;
  apiBaseUrl: string;
  dateRange: { startDate: string; endDate: string };
  brand: { id: string; name: string; domain: string };
  topics: Array<{ id: string; name: string }>;
  prompts: Array<{ id: string; query: string; topicId: string }>;
  stats: {
    totalApiCalls: number;
    durationMs: number;
    rowCounts: {
      brand: MetricRowCounts;
      topic: MetricRowCounts;
      prompt: MetricRowCounts;
    };
  };
}

interface MetricRowCounts {
  shareOfVoice: number;
  visibility: number;
  citations: number;
}

// ---------------------------------------------------------------------------
// Types — CLI Configuration
// ---------------------------------------------------------------------------

interface ExportConfig {
  apiKey: string;
  apiBaseUrl: string;
  brandId: string;
  topicIds: string[] | null;
  promptIds: string[] | null;
  startDate: string;
  endDate: string;
  outputDir: string;
  concurrency: number;
}

// ---------------------------------------------------------------------------
// Types — Internal
// ---------------------------------------------------------------------------

interface PromptWithTopic extends Prompt {
  topicName: string;
}

interface EntityContext {
  brand: Brand;
  topics: Topic[];
  prompts: PromptWithTopic[];
}

type MetricType = "share-of-voice" | "visibility" | "citations";
type EntityLevel = "brand" | "topic" | "prompt";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_API_BASE_URL = "https://app.useomnia.com/api/v1";
const DEFAULT_OUTPUT_DIR = "./export";

const DEFAULT_CONCURRENCY = 4;
const MAX_CONCURRENCY = 10;
const RATE_LIMIT_SAFETY_THRESHOLD = 5;
const MAX_PAGE_SIZE = 100;
const MAX_RETRIES = 3;
const MAX_CONSECUTIVE_ERRORS = 5;

const INITIAL_RATE_LIMIT_TOKENS = 50;
const BACKOFF_BASE_SECONDS = 2;
const MAX_BACKOFF_SECONDS = 30;
const DEFAULT_RETRY_AFTER_SECONDS = 5;
const RATE_LIMIT_PAUSE_MS = 1000;

// ---------------------------------------------------------------------------
// CLI Parsing
// ---------------------------------------------------------------------------

function parseCliArgs(): ExportConfig {
  const args = process.argv.slice(2);

  if (args.includes("--help") || args.includes("-h")) {
    printUsage();
    process.exit(0);
  }

  const apiKey = process.env.OMNIA_API_KEY;
  if (!apiKey) {
    console.error("Error: OMNIA_API_KEY environment variable is required.");
    console.error(
      "Generate an API key at https://app.useomnia.com/n/api-access\n",
    );
    process.exit(1);
  }

  const brandId = getArgValue(args, "--brandId");
  if (!brandId) {
    console.error("Error: --brandId is required.\n");
    printUsage();
    process.exit(1);
  }

  const topicIdsRaw = getArgValue(args, "--topicIds");
  const promptIdsRaw = getArgValue(args, "--promptIds");

  const today = formatDateUTC(new Date());
  const startDate = getArgValue(args, "--startDate") ?? today;
  const endDate = getArgValue(args, "--endDate") ?? today;

  if (!isValidDate(startDate) || !isValidDate(endDate)) {
    console.error("Error: dates must be in YYYY-MM-DD format.");
    process.exit(1);
  }

  if (startDate > endDate) {
    console.error("Error: --startDate must be before --endDate.");
    process.exit(1);
  }

  return {
    apiKey,
    apiBaseUrl: process.env.OMNIA_API_BASE_URL ?? DEFAULT_API_BASE_URL,
    brandId,
    topicIds: topicIdsRaw
      ? topicIdsRaw.split(",").map((id) => id.trim())
      : null,
    promptIds: promptIdsRaw
      ? promptIdsRaw.split(",").map((id) => id.trim())
      : null,
    startDate,
    endDate,
    outputDir: getArgValue(args, "--outputDir") ?? DEFAULT_OUTPUT_DIR,
    concurrency: parseConcurrency(getArgValue(args, "--concurrency")),
  };
}

function getArgValue(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag);
  if (index === -1 || index + 1 >= args.length) return undefined;
  return args[index + 1];
}

function parseConcurrency(value: string | undefined): number {
  if (!value) return DEFAULT_CONCURRENCY;
  const n = parseInt(value, 10);
  if (isNaN(n) || n < 1 || n > MAX_CONCURRENCY) {
    console.error(
      `Error: --concurrency must be between 1 and ${MAX_CONCURRENCY}.`,
    );
    process.exit(1);
  }
  return n;
}

function printUsage(): void {
  console.log(`
Omnia Data Export Script
========================

Exports daily brand performance data from the Omnia public API.
Produces flat, denormalized JSON files ready for BI tools.

Usage:
  OMNIA_API_KEY=ot_xxx npx tsx export-data.ts --brandId <uuid> [options]

Required:
  --brandId <uuid>           Brand ID to export data for

Optional:
  --topicIds <id,id,...>     Comma-separated topic IDs (default: auto-discover all)
  --promptIds <id,id,...>    Comma-separated prompt IDs (default: auto-discover all)
  --startDate <YYYY-MM-DD>  Start of date range (default: today)
  --endDate <YYYY-MM-DD>    End of date range (default: today)
  --outputDir <path>         Output directory (default: ./export)
  --concurrency <number>     Parallel requests (1-${MAX_CONCURRENCY}, default: ${DEFAULT_CONCURRENCY})
  --help, -h                 Show this help message

Environment:
  OMNIA_API_KEY              Bearer token for API authentication (required)
  OMNIA_API_BASE_URL         Override the API base URL (default: https://app.useomnia.com/api/v1)

Output:
  export/
  ├── manifest.json           Export metadata and entity inventory
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

Examples:
  # Export today's data for a brand (auto-discovers topics and prompts)
  OMNIA_API_KEY=ot_xxx npx tsx export-data.ts \\
    --brandId 123e4567-e89b-12d3-a456-426614174000

  # Export a specific date range with specific topics
  OMNIA_API_KEY=ot_xxx npx tsx export-data.ts \\
    --brandId 123e4567-e89b-12d3-a456-426614174000 \\
    --topicIds abc123,def456 \\
    --startDate 2025-01-01 \\
    --endDate 2025-01-31

Documentation: https://docs.useomnia.com
`);
}

// ---------------------------------------------------------------------------
// Date Utilities (all UTC to avoid timezone drift)
// ---------------------------------------------------------------------------

function formatDateUTC(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function isValidDate(dateStr: string): boolean {
  return (
    /^\d{4}-\d{2}-\d{2}$/.test(dateStr) &&
    !isNaN(new Date(dateStr + "T00:00:00Z").getTime())
  );
}

function generateDateRange(startDate: string, endDate: string): string[] {
  const dates: string[] = [];
  const current = new Date(startDate + "T00:00:00Z");
  const end = new Date(endDate + "T00:00:00Z");

  while (current <= end) {
    dates.push(formatDateUTC(current));
    current.setUTCDate(current.getUTCDate() + 1);
  }

  return dates;
}

// ---------------------------------------------------------------------------
// Concurrency Utilities
// ---------------------------------------------------------------------------

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let nextIndex = 0;

  async function worker(): Promise<void> {
    while (nextIndex < items.length) {
      const index = nextIndex++;
      results[index] = await fn(items[index]);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    () => worker(),
  );
  await Promise.all(workers);

  return results;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Omnia API Client
// ---------------------------------------------------------------------------

class OmniaApiClient {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private remainingTokens = INITIAL_RATE_LIMIT_TOKENS;
  private totalApiCalls = 0;
  private consecutiveErrors = 0;

  constructor(apiKey: string, baseUrl: string) {
    this.baseUrl = baseUrl;
    this.headers = {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    };
  }

  getTotalApiCalls(): number {
    return this.totalApiCalls;
  }

  async get<T>(
    endpoint: string,
    params: Record<string, string> = {},
  ): Promise<T> {
    const url = this.buildUrl(endpoint, params);
    return this.request<T>(url);
  }

  async fetchAllPages<TItem>(
    endpoint: string,
    dataKey: string,
    params: Record<string, string> = {},
  ): Promise<TItem[]> {
    type Page = PaginatedResponse<Record<string, TItem[]>>;

    const firstPage = await this.get<Page>(endpoint, {
      ...params,
      pageSize: String(MAX_PAGE_SIZE),
    });

    const allItems: TItem[] = [...(firstPage.data[dataKey] ?? [])];
    let nextUrl = firstPage.links.next;

    while (nextUrl) {
      const page = await this.request<Page>(nextUrl);
      allItems.push(...(page.data[dataKey] ?? []));
      nextUrl = page.links.next;
    }

    return allItems;
  }

  private async request<T>(url: string, retryCount = 0): Promise<T> {
    if (this.consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      throw new Error(
        `Aborting: ${MAX_CONSECUTIVE_ERRORS} consecutive API errors. ` +
          `The API may be overloaded. Try again later or reduce --concurrency.`,
      );
    }

    await this.waitForRateLimit();

    const response = await fetch(url, { method: "GET", headers: this.headers });

    this.trackRateLimitHeaders(response);
    this.totalApiCalls++;

    // 429: rate-limited. Wait and retry without counting it as an error.
    if (response.status === 429) {
      if (retryCount >= MAX_RETRIES) {
        throw new Error(
          `Failed after ${MAX_RETRIES} retries: ${response.status} (${url})`,
        );
      }
      const waitSeconds = this.parseRetryAfter(response);
      console.warn(
        `  429 on ${url}. Waiting ${waitSeconds}s ` +
          `(attempt ${retryCount + 1}/${MAX_RETRIES})...`,
      );
      await sleep(waitSeconds * 1000);
      return this.request<T>(url, retryCount + 1);
    }

    // 5xx: server error. Count towards circuit breaker, then retry.
    if (response.status >= 500) {
      this.consecutiveErrors++;
      if (retryCount >= MAX_RETRIES) {
        throw new Error(
          `Failed after ${MAX_RETRIES} retries: ${response.status} (${url})`,
        );
      }
      const waitSeconds = Math.min(
        BACKOFF_BASE_SECONDS ** retryCount,
        MAX_BACKOFF_SECONDS,
      );
      console.warn(
        `  ${response.status} on ${url}. Waiting ${waitSeconds}s ` +
          `(attempt ${retryCount + 1}/${MAX_RETRIES})...`,
      );
      await sleep(waitSeconds * 1000);
      return this.request<T>(url, retryCount + 1);
    }

    if (!response.ok) {
      this.consecutiveErrors++;
      const body = (await response.json().catch(() => null)) as ApiError | null;
      const description = body?.error?.description ?? response.statusText;
      throw new Error(`API ${response.status}: ${description} (${url})`);
    }

    this.consecutiveErrors = 0;
    return response.json() as Promise<T>;
  }

  private buildUrl(endpoint: string, params: Record<string, string>): string {
    const url = new URL(`${this.baseUrl}${endpoint}`);
    for (const [key, value] of Object.entries(params)) {
      url.searchParams.set(key, value);
    }
    return url.toString();
  }

  private trackRateLimitHeaders(response: Response): void {
    const remaining = response.headers.get("X-RateLimit-Remaining");
    if (remaining !== null) {
      this.remainingTokens = parseInt(remaining, 10);
    }
  }

  private parseRetryAfter(response: Response): number {
    const retryAfter = response.headers.get("Retry-After");
    if (retryAfter !== null) {
      const seconds = parseInt(retryAfter, 10);
      if (!isNaN(seconds)) return seconds;
    }
    return DEFAULT_RETRY_AFTER_SECONDS;
  }

  private async waitForRateLimit(): Promise<void> {
    if (this.remainingTokens < RATE_LIMIT_SAFETY_THRESHOLD) {
      await sleep(RATE_LIMIT_PAUSE_MS);
    }
  }
}

// ---------------------------------------------------------------------------
// Entity Discovery
// ---------------------------------------------------------------------------

async function discoverEntities(
  client: OmniaApiClient,
  config: ExportConfig,
): Promise<EntityContext> {
  console.log("Fetching brand details...");
  const { data: brand } = await client.get<SingleResponse<Brand>>(
    `/brands/${config.brandId}`,
  );
  console.log(`  Brand: ${brand.name} (${brand.domain})`);

  console.log("Discovering topics...");
  const allTopics = await client.fetchAllPages<Topic>(
    `/brands/${config.brandId}/topics`,
    "topics",
  );

  const topics = config.topicIds
    ? allTopics.filter((t) => config.topicIds!.includes(t.id))
    : allTopics;

  console.log(`  Found ${topics.length} topics`);

  console.log("Discovering prompts...");
  const topicPromptPairs = await mapWithConcurrency(
    topics,
    config.concurrency,
    async (topic) => {
      const prompts = await client.fetchAllPages<Prompt>(
        `/topics/${topic.id}/prompts`,
        "prompts",
      );
      return prompts.map(
        (p): PromptWithTopic => ({ ...p, topicName: topic.name }),
      );
    },
  );

  const allPrompts = topicPromptPairs.flat();
  const prompts = config.promptIds
    ? allPrompts.filter((p) => config.promptIds!.includes(p.id))
    : allPrompts;

  console.log(`  Found ${prompts.length} prompts`);

  return { brand, topics, prompts };
}

// ---------------------------------------------------------------------------
// Aggregate Fetching — Generic fetcher for any entity level and metric
// ---------------------------------------------------------------------------

function fetchAggregates<T>(
  client: OmniaApiClient,
  basePath: string,
  metric: MetricType,
  date: string,
): Promise<T[]> {
  return client.fetchAllPages<T>(
    `${basePath}/${metric}/aggregates`,
    "aggregates",
    {
      startDate: date,
      endDate: date,
    },
  );
}

// ---------------------------------------------------------------------------
// Row Flattening — Transform API aggregates into denormalized export rows
//
// Each metric has a rename map that translates API field names into
// BI-friendly column names (e.g. "brand" -> "mentionedBrand" to avoid
// ambiguity with the exported brand). Fields not in the map are kept as-is.
// ---------------------------------------------------------------------------

const FIELD_RENAMES: Record<MetricType, Record<string, string>> = {
  "share-of-voice": { brand: "mentionedBrand", domain: "mentionedDomain" },
  visibility: { brand: "mentionedBrand", domain: "mentionedDomain" },
  citations: {
    domain: "citedDomain",
    url: "citedUrl",
    title: "citedTitle",
    type: "sourceType",
  },
};

function flattenAggregates(
  aggregates: Record<string, unknown>[],
  date: string,
  context: Record<string, unknown>,
  metric: MetricType,
): Array<FlatRow & { date: string }> {
  const renames = FIELD_RENAMES[metric];
  return aggregates.map((agg) => {
    const row: FlatRow = {};
    for (const [key, value] of Object.entries(agg)) {
      const renamedKey = renames[key] ?? key;
      row[renamedKey] = value as string | number | boolean | null;
    }
    return { ...context, date, ...row } as FlatRow & { date: string };
  });
}

// ---------------------------------------------------------------------------
// Context Builders — Create denormalization context for each entity level
// ---------------------------------------------------------------------------

function buildBrandContext(brand: Brand): Record<string, unknown> {
  return { brandId: brand.id, brandName: brand.name };
}

function buildTopicContext(
  brand: Brand,
  topic: Topic,
): Record<string, unknown> {
  return {
    ...buildBrandContext(brand),
    topicId: topic.id,
    topicName: topic.name,
  };
}

function buildPromptContext(
  brand: Brand,
  prompt: PromptWithTopic,
): Record<string, unknown> {
  return {
    ...buildBrandContext(brand),
    topicId: prompt.topicId,
    topicName: prompt.topicName,
    promptId: prompt.id,
    promptQuery: prompt.query,
  };
}

// ---------------------------------------------------------------------------
// Data Collection — Orchestrate fetching and flattening for a single day
// ---------------------------------------------------------------------------

interface DailyCollectionTask {
  level: EntityLevel;
  metric: MetricType;
  entityId: string;
  execute: () => Promise<void>;
}

const METRICS: MetricType[] = ["share-of-voice", "visibility", "citations"];
const METRIC_TO_BUCKET_KEY: Record<MetricType, keyof MetricBucket> = {
  "share-of-voice": "shareOfVoice",
  visibility: "visibility",
  citations: "citations",
};

/**
 * Build all metric-fetch tasks for a single entity (brand, topic, or prompt).
 * Each task fetches one metric's aggregates for one day and appends flat rows.
 */
function buildEntityMetricTasks(
  client: OmniaApiClient,
  level: EntityLevel,
  entityId: string,
  apiPath: string,
  date: string,
  context: Record<string, unknown>,
  bucket: MetricBucket,
): DailyCollectionTask[] {
  return METRICS.map((metric) => ({
    level,
    metric,
    entityId,
    execute: async () => {
      const aggs = await fetchAggregates<Record<string, unknown>>(
        client,
        apiPath,
        metric,
        date,
      );
      bucket[METRIC_TO_BUCKET_KEY[metric]].push(
        ...flattenAggregates(aggs, date, context, metric),
      );
    },
  }));
}

function buildDailyTasks(
  client: OmniaApiClient,
  entities: EntityContext,
  date: string,
  results: ExportResults,
): DailyCollectionTask[] {
  const { brand, topics, prompts } = entities;
  const tasks: DailyCollectionTask[] = [];

  tasks.push(
    ...buildEntityMetricTasks(
      client,
      "brand",
      brand.id,
      `/brands/${brand.id}`,
      date,
      buildBrandContext(brand),
      results.brand,
    ),
  );

  for (const topic of topics) {
    tasks.push(
      ...buildEntityMetricTasks(
        client,
        "topic",
        topic.id,
        `/topics/${topic.id}`,
        date,
        buildTopicContext(brand, topic),
        results.topic,
      ),
    );
  }

  for (const prompt of prompts) {
    tasks.push(
      ...buildEntityMetricTasks(
        client,
        "prompt",
        prompt.id,
        `/prompts/${prompt.id}`,
        date,
        buildPromptContext(brand, prompt),
        results.prompt,
      ),
    );
  }

  return tasks;
}

// ---------------------------------------------------------------------------
// Accumulator — Collects rows across all days
// ---------------------------------------------------------------------------

interface MetricBucket {
  shareOfVoice: Array<FlatRow & { date: string }>;
  visibility: Array<FlatRow & { date: string }>;
  citations: Array<FlatRow & { date: string }>;
}

interface ExportResults {
  brand: MetricBucket;
  topic: MetricBucket;
  prompt: MetricBucket;
  errors: Array<{
    date: string;
    level: EntityLevel;
    metric: MetricType;
    entityId: string;
    error: string;
  }>;
}

function createExportResults(): ExportResults {
  return {
    brand: { shareOfVoice: [], visibility: [], citations: [] },
    topic: { shareOfVoice: [], visibility: [], citations: [] },
    prompt: { shareOfVoice: [], visibility: [], citations: [] },
    errors: [],
  };
}

function getRowCounts(bucket: MetricBucket): MetricRowCounts {
  return {
    shareOfVoice: bucket.shareOfVoice.length,
    visibility: bucket.visibility.length,
    citations: bucket.citations.length,
  };
}

// ---------------------------------------------------------------------------
// File Output
// ---------------------------------------------------------------------------

function writeExportFiles(
  results: ExportResults,
  manifest: ExportManifest,
  outputDir: string,
): void {
  const levels: EntityLevel[] = ["brand", "topic", "prompt"];

  for (const level of levels) {
    fs.mkdirSync(path.join(outputDir, level), { recursive: true });
  }

  writeJsonFile(path.join(outputDir, "manifest.json"), manifest);

  for (const level of levels) {
    writeJsonFile(
      path.join(outputDir, level, "share-of-voice.json"),
      results[level].shareOfVoice,
    );
    writeJsonFile(
      path.join(outputDir, level, "visibility.json"),
      results[level].visibility,
    );
    writeJsonFile(
      path.join(outputDir, level, "citations.json"),
      results[level].citations,
    );
  }
}

function writeJsonFile(filePath: string, data: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf-8");
  const sizeKb = (fs.statSync(filePath).size / 1024).toFixed(1);
  console.log(`  ${filePath} (${sizeKb} KB)`);
}

// ---------------------------------------------------------------------------
// Logging Helpers
// ---------------------------------------------------------------------------

function formatDuration(ms: number): string {
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${minutes}m ${secs}s`;
}

function logConfig(config: ExportConfig): void {
  console.log("\n=== Omnia Data Export ===\n");
  console.log(`API:        ${config.apiBaseUrl}`);
  console.log(`Brand ID:   ${config.brandId}`);
  console.log(`Date range: ${config.startDate} to ${config.endDate}`);
  console.log(`Output:     ${config.outputDir}`);
  console.log(`Concurrency: ${config.concurrency}\n`);
}

function logExportPlan(entities: EntityContext, days: number): void {
  const entitiesCount = 1 + entities.topics.length + entities.prompts.length;
  const tasksPerDay = entitiesCount * 3;

  console.log(`\nExport plan:`);
  console.log(
    `  ${entitiesCount} entities (1 brand + ${entities.topics.length} topics + ${entities.prompts.length} prompts)`,
  );
  console.log(
    `  ${days} days x ${tasksPerDay} tasks/day = ${days * tasksPerDay} total API task groups`,
  );
  console.log(`  (each task may require multiple pages)\n`);
}

function logSummary(
  results: ExportResults,
  manifest: ExportManifest,
  outputDir: string,
): void {
  const { stats } = manifest;

  console.log(`\n=== Export Complete ===\n`);
  console.log(`Duration:   ${formatDuration(stats.durationMs)}`);
  console.log(`API calls:  ${stats.totalApiCalls}`);
  console.log(`Rows exported:`);

  const levels: EntityLevel[] = ["brand", "topic", "prompt"];
  for (const level of levels) {
    const counts = stats.rowCounts[level];
    const label = level.charAt(0).toUpperCase() + level.slice(1);
    console.log(
      `  ${label.padEnd(8)} SOV: ${counts.shareOfVoice}, Visibility: ${counts.visibility}, Citations: ${counts.citations}`,
    );
  }

  if (results.errors.length > 0) {
    console.log(`\nErrors (${results.errors.length}):`);
    const displayErrors = results.errors.slice(0, 10);
    for (const err of displayErrors) {
      console.log(
        `  ${err.date} ${err.level}/${err.metric} [${err.entityId}]: ${err.error}`,
      );
    }
    if (results.errors.length > 10) {
      console.log(`  ... and ${results.errors.length - 10} more`);
    }
  }

  console.log(`\nOutput: ${path.resolve(outputDir)}`);
}

// ---------------------------------------------------------------------------
// Daily Aggregates — Fetch all metrics for all entities across all days
// ---------------------------------------------------------------------------

async function fetchAllDailyAggregates(
  client: OmniaApiClient,
  entities: EntityContext,
  dates: string[],
  results: ExportResults,
  concurrency: number,
): Promise<void> {
  console.log("Fetching daily aggregates...");

  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    console.log(`  [${i + 1}/${dates.length}] ${date}`);

    const tasks = buildDailyTasks(client, entities, date, results);

    await mapWithConcurrency(tasks, concurrency, async (task) => {
      try {
        await task.execute();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        results.errors.push({
          date,
          level: task.level,
          metric: task.metric,
          entityId: task.entityId,
          error: message,
        });
      }
    });
  }
}

// ---------------------------------------------------------------------------
// Manifest Builder
// ---------------------------------------------------------------------------

function buildManifest(
  config: ExportConfig,
  entities: EntityContext,
  results: ExportResults,
  totalApiCalls: number,
  durationMs: number,
): ExportManifest {
  return {
    exportedAt: new Date().toISOString(),
    apiBaseUrl: config.apiBaseUrl,
    dateRange: { startDate: config.startDate, endDate: config.endDate },
    brand: {
      id: entities.brand.id,
      name: entities.brand.name,
      domain: entities.brand.domain,
    },
    topics: entities.topics.map((t) => ({ id: t.id, name: t.name })),
    prompts: entities.prompts.map((p) => ({
      id: p.id,
      query: p.query,
      topicId: p.topicId,
    })),
    stats: {
      totalApiCalls,
      durationMs,
      rowCounts: {
        brand: getRowCounts(results.brand),
        topic: getRowCounts(results.topic),
        prompt: getRowCounts(results.prompt),
      },
    },
  };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const config = parseCliArgs();
  const client = new OmniaApiClient(config.apiKey, config.apiBaseUrl);

  logConfig(config);

  const entities = await discoverEntities(client, config);
  const dates = generateDateRange(config.startDate, config.endDate);

  logExportPlan(entities, dates.length);

  const results = createExportResults();
  const startTime = Date.now();

  await fetchAllDailyAggregates(
    client,
    entities,
    dates,
    results,
    config.concurrency,
  );

  const manifest = buildManifest(
    config,
    entities,
    results,
    client.getTotalApiCalls(),
    Date.now() - startTime,
  );

  console.log("\nWriting output files...");
  writeExportFiles(results, manifest, config.outputDir);

  logSummary(results, manifest, config.outputDir);
}

main().catch((error) => {
  console.error(
    "\nFatal error:",
    error instanceof Error ? error.message : error,
  );
  process.exit(1);
});
