#!/usr/bin/env tsx

/**
 * Omnia Data Export Script
 *
 * Exports daily brand performance data (share of voice, visibility, citations,
 * sentiment) from the Omnia public API at brand, topic, and prompt granularity.
 *
 * Designed as a reference implementation for integrating Omnia analytics data
 * with BI tools (Looker Studio, BigQuery, Tableau, etc.) or ingesting into
 * external databases.
 *
 * Usage:
 *   OMNIA_API_KEY=ot_xxx tsx export-data.ts [options]
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

interface ApiError {
  error: { code: number; description: string };
}

interface Brand {
  id: string;
  name: string;
  domain: string;
}

interface ApiTopic {
  id: string;
  name: string;
  location: string | null;
  tags: string[];
  topicType: TopicType | null;
}

interface Topic extends ApiTopic {
  brandId: string;
}

interface Prompt {
  id: string;
  query: string;
  topicId: string;
}

type FlatRow = Record<string, string | number | boolean | string[] | null>;
type MetricType = "share-of-voice" | "visibility" | "citations" | "sentiment";
type EntityLevel = "brand" | "topic" | "prompt";
type Engine = (typeof ENGINES)[number];
type TopicType = (typeof TOPIC_TYPES)[number];

interface EnrichedPrompt extends Prompt {
  brandId: string;
  topicName: string;
  topicLocation: string | null;
  topicTags: string[];
  topicType: TopicType | null;
}

interface ExportConfig {
  apiKey: string;
  apiBaseUrl: string;
  brandIds: string[] | null;
  topicIds: string[] | null;
  promptIds: string[] | null;
  startDate: string;
  endDate: string;
  outputDir: string;
  concurrency: number;
  engines: Engine[];
}

interface EntityContext {
  brands: Brand[];
  topics: Topic[];
  prompts: EnrichedPrompt[];
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

const ENGINES = [
  "google-ai-overviews",
  "google-ai-mode",
  "perplexity",
  "openai",
] as const;

const TOPIC_TYPES = ["Branded", "Non-Branded"] as const;

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

  const brandIdsRaw = getArgValue(args, "--brandIds");

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

  const enginesRaw = getArgValue(args, "--engines");
  let engines: Engine[] = [...ENGINES];
  if (enginesRaw && enginesRaw !== "all") {
    const requested = enginesRaw.split(",").map((e) => e.trim());
    const invalid = requested.filter(
      (e) => !(ENGINES as readonly string[]).includes(e),
    );
    if (invalid.length > 0) {
      console.error(`Error: invalid engine(s): ${invalid.join(", ")}`);
      console.error(`Valid engines: ${ENGINES.join(", ")}`);
      process.exit(1);
    }
    engines = requested as Engine[];
  }

  return {
    apiKey,
    apiBaseUrl: process.env.OMNIA_API_BASE_URL ?? DEFAULT_API_BASE_URL,
    brandIds: brandIdsRaw
      ? brandIdsRaw.split(",").map((id) => id.trim())
      : null,
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
    engines,
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
Produces flat, denormalized JSON files at brand, topic, and prompt levels.
Metrics: share of voice, visibility, citations, and sentiment.

Usage:
  OMNIA_API_KEY=ot_xxx tsx export-data.ts [options]

Optional:
  --brandIds <id,id,...>     Comma-separated brand IDs (default: all account brands)
  --topicIds <id,id,...>     Comma-separated topic IDs (default: auto-discover all)
  --promptIds <id,id,...>    Comma-separated prompt IDs (default: auto-discover all)
  --startDate <YYYY-MM-DD>  Start of date range (default: today)
  --endDate <YYYY-MM-DD>    End of date range (default: today)
  --outputDir <path>         Output directory (default: ./export)
  --concurrency <number>     Parallel requests (1-${MAX_CONCURRENCY}, default: ${DEFAULT_CONCURRENCY})
  --engines <e1,e2,...>      AI engines to query (default: all)
                             Valid: ${ENGINES.join(", ")}
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
  │   ├── citations.json
  │   └── sentiment.json
  ├── topic/
  │   ├── share-of-voice.json
  │   ├── visibility.json
  │   ├── citations.json
  │   └── sentiment.json
  └── prompt/
      ├── share-of-voice.json
      ├── visibility.json
      ├── citations.json
      └── sentiment.json

Examples:
  # Export today's data for all account brands
  OMNIA_API_KEY=ot_xxx tsx export-data.ts

  # Export specific brands only
  OMNIA_API_KEY=ot_xxx tsx export-data.ts \\
    --brandIds 123e4567-e89b-12d3-a456-426614174000,abcd1234-e89b-12d3-a456-426614174000

  # Export a specific date range with specific topics
  OMNIA_API_KEY=ot_xxx tsx export-data.ts \\
    --topicIds abc123,def456 \\
    --startDate 2025-01-01 \\
    --endDate 2025-01-31

  # Export only specific engines
  OMNIA_API_KEY=ot_xxx tsx export-data.ts \\
    --engines perplexity,openai

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
    return this.request<T>(this.buildUrl(endpoint, params));
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
      const page = await this.request<Page>(nextUrl);
      allItems.push(...(page.data[dataKey] ?? []));
      nextUrl = page.links.next;
    }

    return allItems;
  }

  // -- Core fetch with retry logic ----------------------------------------

  private async request<T>(url: string, retries = 0): Promise<T> {
    this.checkCircuitBreaker();

    const response = await fetch(url, {
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
    return this.request<T>(url, retries + 1);
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
    return this.request<T>(url, retries + 1);
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
  console.log("Fetching brands...");
  const allBrands = await client.fetchAllPages<Brand>("/brands", "brands");

  const brands = config.brandIds
    ? allBrands.filter((b) => config.brandIds!.includes(b.id))
    : allBrands;

  if (brands.length === 0) {
    console.error("Error: no brands found matching the given IDs.");
    process.exit(1);
  }

  for (const brand of brands) {
    console.log(`  Brand: ${brand.name} (${brand.domain})`);
  }

  const allTopics: Topic[] = [];
  const allPrompts: EnrichedPrompt[] = [];

  for (const brand of brands) {
    console.log(`Discovering topics for ${brand.name}...`);
    const rawTopics = await client.fetchAllPages<ApiTopic>(
      `/brands/${brand.id}/topics`,
      "topics",
    );
    const brandTopics: Topic[] = rawTopics.map((t) => ({ ...t, brandId: brand.id }));

    const topics = config.topicIds
      ? brandTopics.filter((t) => config.topicIds!.includes(t.id))
      : brandTopics;

    console.log(`  Found ${topics.length} topics`);
    allTopics.push(...topics);

    console.log(`Discovering prompts for ${brand.name}...`);
    const topicPromptPairs = await mapWithConcurrency(
      topics,
      config.concurrency,
      async (topic) => {
        const prompts = await client.fetchAllPages<Prompt>(
          `/topics/${topic.id}/prompts`,
          "prompts",
        );
        return prompts.map((p): EnrichedPrompt => ({
          ...p,
          brandId: brand.id,
          topicName: topic.name,
          topicLocation: topic.location,
          topicTags: topic.tags,
          topicType: topic.topicType,
        }));
      },
    );

    const brandPrompts = topicPromptPairs.flat();
    const prompts = config.promptIds
      ? brandPrompts.filter((p) => config.promptIds!.includes(p.id))
      : brandPrompts;

    console.log(`  Found ${prompts.length} prompts`);
    allPrompts.push(...prompts);
  }

  return { brands, topics: allTopics, prompts: allPrompts };
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
  sentiment: { brand: "mentionedBrand", domain: "mentionedDomain", rank: "mentionedBrandRank" },
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

const METRICS: MetricType[] = ["share-of-voice", "visibility", "citations", "sentiment"];
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
  return { "share-of-voice": [], visibility: [], citations: [], sentiment: [] };
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
  extraParams: Record<string, string>;
}

function buildDailyTasks(entities: EntityContext, config: ExportConfig): FetchTask[] {
  const { brands, topics, prompts } = entities;
  const tasks: FetchTask[] = [];

  for (const brand of brands) {
    const brandCtx = {
      brandId: brand.id,
      brandName: brand.name,
      brandDomain: brand.domain,
    };

    const brandTopics = topics.filter((t) => t.brandId === brand.id);
    const brandPrompts = prompts.filter((p) => p.brandId === brand.id);

    for (const engine of config.engines) {
      const engineParam = { engine };

      // Brand-level
      for (const metric of METRICS) {
        tasks.push({
          level: "brand",
          metric,
          entityId: brand.id,
          apiPath: `/brands/${brand.id}/${metric}/aggregates`,
          context: { ...brandCtx, engine },
          extraParams: engineParam,
        });
      }

      // Topic-level
      for (const topic of brandTopics) {
        const topicCtx = {
          ...brandCtx,
          topicId: topic.id,
          topicName: topic.name,
          topicLocation: topic.location,
          topicTags: topic.tags,
          topicType: topic.topicType,
          engine,
        };
        for (const metric of METRICS) {
          tasks.push({
            level: "topic",
            metric,
            entityId: topic.id,
            apiPath: `/topics/${topic.id}/${metric}/aggregates`,
            context: topicCtx,
            extraParams: engineParam,
          });
        }
      }

      // Prompt-level
      for (const prompt of brandPrompts) {
        const promptCtx = {
          ...brandCtx,
          topicId: prompt.topicId,
          topicName: prompt.topicName,
          topicLocation: prompt.topicLocation,
          topicTags: prompt.topicTags,
          topicType: prompt.topicType,
          promptId: prompt.id,
          promptQuery: prompt.query,
          engine,
        };
        for (const metric of METRICS) {
          tasks.push({
            level: "prompt",
            metric,
            entityId: prompt.id,
            apiPath: `/prompts/${prompt.id}/${metric}/aggregates`,
            context: promptCtx,
            extraParams: engineParam,
          });
        }
      }
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
  config: ExportConfig,
  dates: string[],
): Promise<{ rows: ExportRows; errors: ExportError[] }> {
  const rows = createExportRows();
  const errors: ExportError[] = [];

  const tasks = buildDailyTasks(entities, config);

  console.log("Fetching daily aggregates...");

  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    console.log(`  [${i + 1}/${dates.length}] ${date}`);

    await mapWithConcurrency(tasks, config.concurrency, async (task) => {
      try {
        const aggregates = await client.fetchAllPages<Record<string, unknown>>(
          task.apiPath,
          "aggregates",
          { startDate: date, endDate: date, ...task.extraParams },
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
          error: String(error),
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
  console.log(`API:         ${config.apiBaseUrl}`);
  console.log(`Brands:      ${config.brandIds ? config.brandIds.join(", ") : "all"}`);
  console.log(`Date range:  ${config.startDate} to ${config.endDate}`);
  console.log(`Engines:     ${config.engines.join(", ")}`);
  console.log(`Output:      ${config.outputDir}`);
  console.log(`Concurrency: ${config.concurrency}\n`);
}

function logExportPlan(
  entities: EntityContext,
  days: number,
  config: ExportConfig,
): void {
  const engineCount = config.engines.length;
  const brandCount = entities.brands.length;
  const entityCount = brandCount + entities.topics.length + entities.prompts.length;
  const tasksPerDay = entityCount * METRICS.length * engineCount;

  console.log(`\nExport plan:`);
  console.log(
    `  ${entityCount} entities (${brandCount} brands + ${entities.topics.length} topics + ${entities.prompts.length} prompts)`,
  );
  console.log(`  ${engineCount} engine(s): ${config.engines.join(", ")}`);
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
    const sent = rows[level]["sentiment"].length;
    console.log(
      `  ${label.padEnd(8)} SOV: ${sov}, Visibility: ${vis}, Citations: ${cit}, Sentiment: ${sent}`,
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
    engines: config.engines,
    brands: entities.brands.map((b) => ({
      id: b.id,
      name: b.name,
      domain: b.domain,
    })),
    topics: entities.topics.map((t) => ({
      id: t.id,
      name: t.name,
      location: t.location,
      tags: t.tags,
      topicType: t.topicType,
    })),
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

  logExportPlan(entities, dates.length, config);

  const startTime = Date.now();
  const { rows, errors } = await fetchAllDailyAggregates(
    client,
    entities,
    config,
    dates,
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
  console.error("\nFatal error:", String(error));
  process.exit(1);
});
