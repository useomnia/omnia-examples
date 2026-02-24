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
 *   OMNIA_API_KEY=ot_xxx tsx export-data.ts --brandId <uuid> [options]
 *
 * Documentation: https://docs.useomnia.com
 */

import fs from "node:fs";
import path from "node:path";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface PaginatedResponse<T> {
  data: T;
  pagination: { page: number; pageSize: number; totalItems: number };
  links: { prev?: string; next?: string };
}

interface SingleResponse<T> {
  data: T;
}

interface ApiError {
  error: { code: number; description: string };
}

interface Brand {
  id: string;
  name: string;
  domain: string;
}

interface Topic {
  id: string;
  name: string;
}

interface Prompt {
  id: string;
  query: string;
  topicId: string;
}

type FlatRow = Record<string, string | number | boolean | null>;
type MetricType = "share-of-voice" | "visibility" | "citations";
type EntityLevel = "brand" | "topic" | "prompt";

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

interface EntityContext {
  brand: Brand;
  topics: Topic[];
  prompts: Array<Prompt & { topicName: string }>;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_API_BASE_URL = "https://app.useomnia.com/api/v1";
const DEFAULT_OUTPUT_DIR = "./export";

const DEFAULT_CONCURRENCY = 4;
const MAX_CONCURRENCY = 10;
const MAX_PAGE_SIZE = 100;
const MAX_RETRIES = 3;
const MAX_CONSECUTIVE_ERRORS = 5;

const BACKOFF_BASE_SECONDS = 2;
const MAX_BACKOFF_SECONDS = 30;
const DEFAULT_RETRY_AFTER_SECONDS = 5;

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
  OMNIA_API_KEY=ot_xxx tsx export-data.ts --brandId <uuid> [options]

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
  OMNIA_API_KEY=ot_xxx tsx export-data.ts \\
    --brandId 123e4567-e89b-12d3-a456-426614174000

  # Export a specific date range with specific topics
  OMNIA_API_KEY=ot_xxx tsx export-data.ts \\
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
    return this.fetch<T>(this.buildUrl(endpoint, params));
  }

  async fetchAllPages<TItem>(
    endpoint: string,
    dataKey: string,
    params: Record<string, string> = {},
  ): Promise<TItem[]> {
    type Page = PaginatedResponse<Record<string, TItem[]>>;
    const allItems: TItem[] = [];

    let nextUrl: string | undefined = this.buildUrl(endpoint, {
      ...params,
      pageSize: String(MAX_PAGE_SIZE),
    });

    while (nextUrl) {
      const page = await this.fetch<Page>(nextUrl);
      allItems.push(...(page.data[dataKey] ?? []));
      nextUrl = page.links.next;
    }

    return allItems;
  }

  // -- Core fetch with retry logic ----------------------------------------

  private async fetch<T>(url: string, retries = 0): Promise<T> {
    this.checkCircuitBreaker();

    const response = await globalThis.fetch(url, {
      method: "GET",
      headers: this.headers,
    });
    this.totalApiCalls++;

    if (response.status === 429) {
      return this.retryAfter<T>(url, retries, response);
    }

    if (response.status >= 500) {
      this.consecutiveErrors++;
      return this.retryWithBackoff<T>(url, retries, response.status);
    }

    if (!response.ok) {
      this.consecutiveErrors++;
      await this.throwApiError(response, url);
    }

    this.consecutiveErrors = 0;
    return response.json() as Promise<T>;
  }

  // -- Retry strategies ---------------------------------------------------

  private async retryAfter<T>(
    url: string,
    retries: number,
    response: Response,
  ): Promise<T> {
    if (retries >= MAX_RETRIES) {
      throw new Error(`Rate limited after ${MAX_RETRIES} retries (${url})`);
    }

    const seconds = this.parseRetryAfterHeader(response);
    console.warn(
      `  429 on ${url}. Retry-After: ${seconds}s (attempt ${retries + 1}/${MAX_RETRIES})`,
    );
    await sleep(seconds * 1000);
    return this.fetch<T>(url, retries + 1);
  }

  private async retryWithBackoff<T>(
    url: string,
    retries: number,
    status: number,
  ): Promise<T> {
    if (retries >= MAX_RETRIES) {
      throw new Error(
        `Failed after ${MAX_RETRIES} retries: ${status} (${url})`,
      );
    }

    const seconds = Math.min(
      BACKOFF_BASE_SECONDS ** retries,
      MAX_BACKOFF_SECONDS,
    );
    console.warn(
      `  ${status} on ${url}. Backing off ${seconds}s (attempt ${retries + 1}/${MAX_RETRIES})`,
    );
    await sleep(seconds * 1000);
    return this.fetch<T>(url, retries + 1);
  }

  // -- Helpers ------------------------------------------------------------

  private checkCircuitBreaker(): void {
    if (this.consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      throw new Error(
        `Aborting: ${MAX_CONSECUTIVE_ERRORS} consecutive server errors. ` +
          `Try again later or reduce --concurrency.`,
      );
    }
  }

  private parseRetryAfterHeader(response: Response): number {
    const header = response.headers.get("Retry-After");
    if (header !== null) {
      const seconds = parseInt(header, 10);
      if (!isNaN(seconds)) return seconds;
    }
    return DEFAULT_RETRY_AFTER_SECONDS;
  }

  private async throwApiError(response: Response, url: string): Promise<never> {
    let description = response.statusText;
    try {
      const body = (await response.json()) as ApiError;
      description = body.error.description;
    } catch {}
    throw new Error(`API ${response.status}: ${description} (${url})`);
  }

  private buildUrl(endpoint: string, params: Record<string, string>): string {
    const url = new URL(`${this.baseUrl}${endpoint}`);
    for (const [key, value] of Object.entries(params)) {
      url.searchParams.set(key, value);
    }
    return url.toString();
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
      return prompts.map((p): Prompt & { topicName: string } => ({
        ...p,
        topicName: topic.name,
      }));
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
// Row Flattening
//
// Rename API fields to avoid ambiguity (e.g. "brand" -> "mentionedBrand"
// so it doesn't clash with the exported brand). Fields not renamed pass through.
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
): FlatRow[] {
  const renames = FIELD_RENAMES[metric];
  return aggregates.map((agg) => {
    const row: FlatRow = {};
    for (const [key, value] of Object.entries(agg)) {
      row[renames[key] ?? key] = value as string | number | boolean | null;
    }
    return { ...context, date, ...row };
  });
}

// ---------------------------------------------------------------------------
// Export Results
// ---------------------------------------------------------------------------

const METRICS: MetricType[] = ["share-of-voice", "visibility", "citations"];
const LEVELS: EntityLevel[] = ["brand", "topic", "prompt"];

type MetricRows = Record<MetricType, FlatRow[]>;
type ExportRows = Record<EntityLevel, MetricRows>;

interface ExportError {
  date: string;
  level: EntityLevel;
  metric: MetricType;
  entityId: string;
  error: string;
}

function createEmptyMetricRows(): MetricRows {
  return { "share-of-voice": [], visibility: [], citations: [] };
}

function createExportRows(): ExportRows {
  return {
    brand: createEmptyMetricRows(),
    topic: createEmptyMetricRows(),
    prompt: createEmptyMetricRows(),
  };
}

// ---------------------------------------------------------------------------
// Task Descriptors
//
// A flat list of { level, metric, entityId, apiPath, context } objects.
// No closures, no execute methods. Just data describing what to fetch.
// ---------------------------------------------------------------------------

interface FetchTask {
  level: EntityLevel;
  metric: MetricType;
  entityId: string;
  apiPath: string;
  context: Record<string, unknown>;
}

function buildDailyTasks(entities: EntityContext, date: string): FetchTask[] {
  const { brand, topics, prompts } = entities;
  const tasks: FetchTask[] = [];

  const brandCtx = { brandId: brand.id, brandName: brand.name };

  for (const metric of METRICS) {
    tasks.push({
      level: "brand",
      metric,
      entityId: brand.id,
      apiPath: `/brands/${brand.id}/${metric}/aggregates`,
      context: brandCtx,
    });
  }

  for (const topic of topics) {
    const topicCtx = { ...brandCtx, topicId: topic.id, topicName: topic.name };
    for (const metric of METRICS) {
      tasks.push({
        level: "topic",
        metric,
        entityId: topic.id,
        apiPath: `/topics/${topic.id}/${metric}/aggregates`,
        context: topicCtx,
      });
    }
  }

  for (const prompt of prompts) {
    const promptCtx = {
      ...brandCtx,
      topicId: prompt.topicId,
      topicName: prompt.topicName,
      promptId: prompt.id,
      promptQuery: prompt.query,
    };
    for (const metric of METRICS) {
      tasks.push({
        level: "prompt",
        metric,
        entityId: prompt.id,
        apiPath: `/prompts/${prompt.id}/${metric}/aggregates`,
        context: promptCtx,
      });
    }
  }

  return tasks;
}

// ---------------------------------------------------------------------------
// Daily Aggregates
// ---------------------------------------------------------------------------

async function fetchAllDailyAggregates(
  client: OmniaApiClient,
  entities: EntityContext,
  dates: string[],
  concurrency: number,
): Promise<{ rows: ExportRows; errors: ExportError[] }> {
  const rows = createExportRows();
  const errors: ExportError[] = [];

  console.log("Fetching daily aggregates...");

  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    console.log(`  [${i + 1}/${dates.length}] ${date}`);

    const tasks = buildDailyTasks(entities, date);

    await mapWithConcurrency(tasks, concurrency, async (task) => {
      try {
        const aggregates = await client.fetchAllPages<Record<string, unknown>>(
          task.apiPath,
          "aggregates",
          { startDate: date, endDate: date },
        );
        const flatRows = flattenAggregates(
          aggregates,
          date,
          task.context,
          task.metric,
        );
        rows[task.level][task.metric].push(...flatRows);
      } catch (error) {
        errors.push({
          date,
          level: task.level,
          metric: task.metric,
          entityId: task.entityId,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });
  }

  return { rows, errors };
}

// ---------------------------------------------------------------------------
// File Output
// ---------------------------------------------------------------------------

function writeExportFiles(
  rows: ExportRows,
  manifest: Record<string, unknown>,
  outputDir: string,
): void {
  for (const level of LEVELS) {
    fs.mkdirSync(path.join(outputDir, level), { recursive: true });
  }

  writeJsonFile(path.join(outputDir, "manifest.json"), manifest);

  for (const level of LEVELS) {
    for (const metric of METRICS) {
      writeJsonFile(
        path.join(outputDir, level, `${metric}.json`),
        rows[level][metric],
      );
    }
  }
}

function writeJsonFile(filePath: string, data: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf-8");
  const sizeKb = (fs.statSync(filePath).size / 1024).toFixed(1);
  console.log(`  ${filePath} (${sizeKb} KB)`);
}

// ---------------------------------------------------------------------------
// Logging
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
  const entityCount = 1 + entities.topics.length + entities.prompts.length;
  const tasksPerDay = entityCount * METRICS.length;

  console.log(`\nExport plan:`);
  console.log(
    `  ${entityCount} entities (1 brand + ${entities.topics.length} topics + ${entities.prompts.length} prompts)`,
  );
  console.log(
    `  ${days} days x ${tasksPerDay} tasks/day = ${days * tasksPerDay} total API calls`,
  );
  console.log(`  (paginated endpoints may require additional requests)\n`);
}

function logSummary(
  rows: ExportRows,
  errors: ExportError[],
  totalApiCalls: number,
  durationMs: number,
  outputDir: string,
): void {
  console.log(`\n=== Export Complete ===\n`);
  console.log(`Duration:   ${formatDuration(durationMs)}`);
  console.log(`API calls:  ${totalApiCalls}`);
  console.log(`Rows exported:`);

  for (const level of LEVELS) {
    const label = level.charAt(0).toUpperCase() + level.slice(1);
    const sov = rows[level]["share-of-voice"].length;
    const vis = rows[level]["visibility"].length;
    const cit = rows[level]["citations"].length;
    console.log(
      `  ${label.padEnd(8)} SOV: ${sov}, Visibility: ${vis}, Citations: ${cit}`,
    );
  }

  if (errors.length > 0) {
    console.log(`\nErrors (${errors.length}):`);
    for (const err of errors.slice(0, 10)) {
      console.log(
        `  ${err.date} ${err.level}/${err.metric} [${err.entityId}]: ${err.error}`,
      );
    }
    if (errors.length > 10) {
      console.log(`  ... and ${errors.length - 10} more`);
    }
  }

  console.log(`\nOutput: ${path.resolve(outputDir)}`);
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

function buildManifest(
  config: ExportConfig,
  entities: EntityContext,
  rows: ExportRows,
  totalApiCalls: number,
  durationMs: number,
): Record<string, unknown> {
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
      rowCounts: Object.fromEntries(
        LEVELS.map((level) => [
          level,
          Object.fromEntries(METRICS.map((m) => [m, rows[level][m].length])),
        ]),
      ),
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

  const startTime = Date.now();
  const { rows, errors } = await fetchAllDailyAggregates(
    client,
    entities,
    dates,
    config.concurrency,
  );
  const durationMs = Date.now() - startTime;

  const manifest = buildManifest(
    config,
    entities,
    rows,
    client.getTotalApiCalls(),
    durationMs,
  );

  console.log("\nWriting output files...");
  writeExportFiles(rows, manifest, config.outputDir);

  logSummary(
    rows,
    errors,
    client.getTotalApiCalls(),
    durationMs,
    config.outputDir,
  );
}

main().catch((error) => {
  console.error(
    "\nFatal error:",
    error instanceof Error ? error.message : error,
  );
  process.exit(1);
});
