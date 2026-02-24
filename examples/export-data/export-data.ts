#!/usr/bin/env tsx

/**
 * Omnia Data Export Script
 *
 * Exports daily brand performance data (share of voice, visibility, citations)
 * from the Omnia public API at brand, topic, and prompt granularity.
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

interface ExportError {
  date: string;
  level: EntityLevel;
  metric: MetricType;
  entityId: string;
  error: string;
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

const METRICS: MetricType[] = ["share-of-voice", "visibility", "citations"];
const LEVELS: EntityLevel[] = ["brand", "topic", "prompt"];

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

  const today = new Date().toISOString().slice(0, 10);
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
// Date Utilities
// ---------------------------------------------------------------------------

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
    dates.push(current.toISOString().slice(0, 10));
    current.setUTCDate(current.getUTCDate() + 1);
  }

  return dates;
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let nextIndex = 0;

  // Each worker pulls the next item from the queue until none remain
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
// API Client (module-level state, plain functions)
// ---------------------------------------------------------------------------

let apiBaseUrl = "";
let apiHeaders: Record<string, string> = {};
let totalApiCalls = 0;
let consecutiveErrors = 0;

function initClient(apiKey: string, baseUrl: string): void {
  apiBaseUrl = baseUrl;
  apiHeaders = { Authorization: `Bearer ${apiKey}` };
}

function buildUrl(
  endpoint: string,
  params: Record<string, string> = {},
): string {
  const url = new URL(`${apiBaseUrl}${endpoint}`);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }
  return url.toString();
}

async function apiFetch<T>(url: string, retries = 0): Promise<T> {
  if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
    throw new Error(
      `Aborting: ${MAX_CONSECUTIVE_ERRORS} consecutive server errors. ` +
        `Try again later or reduce --concurrency.`,
    );
  }

  const response = await fetch(url, { headers: apiHeaders });
  totalApiCalls++;

  if (response.status === 429) {
    if (retries >= MAX_RETRIES) {
      throw new Error(`Rate limited after ${MAX_RETRIES} retries (${url})`);
    }
    const header = response.headers.get("Retry-After");
    const seconds =
      header !== null && !isNaN(parseInt(header, 10))
        ? parseInt(header, 10)
        : DEFAULT_RETRY_AFTER_SECONDS;
    console.warn(
      `  429 rate limited. Retry-After: ${seconds}s (attempt ${retries + 1}/${MAX_RETRIES})`,
    );
    await sleep(seconds * 1000);
    return apiFetch<T>(url, retries + 1);
  }

  if (response.status >= 500) {
    consecutiveErrors++;
    if (retries >= MAX_RETRIES) {
      throw new Error(
        `Failed after ${MAX_RETRIES} retries: ${response.status} (${url})`,
      );
    }
    const seconds = Math.min(
      BACKOFF_BASE_SECONDS ** retries,
      MAX_BACKOFF_SECONDS,
    );
    console.warn(
      `  ${response.status} server error. Backing off ${seconds}s (attempt ${retries + 1}/${MAX_RETRIES})`,
    );
    await sleep(seconds * 1000);
    return apiFetch<T>(url, retries + 1);
  }

  if (!response.ok) {
    let description = response.statusText;
    try {
      const body = await response.json();
      description = body.error.description;
    } catch {}
    throw new Error(`API ${response.status}: ${description} (${url})`);
  }

  consecutiveErrors = 0;
  return response.json() as Promise<T>;
}

async function fetchAllPages<TItem>(
  endpoint: string,
  dataKey: string,
  params: Record<string, string> = {},
): Promise<TItem[]> {
  type Page = PaginatedResponse<Record<string, TItem[]>>;
  const allItems: TItem[] = [];

  let nextUrl: string | undefined = buildUrl(endpoint, {
    ...params,
    pageSize: String(MAX_PAGE_SIZE),
  });

  while (nextUrl) {
    const page = await apiFetch<Page>(nextUrl);
    allItems.push(...(page.data[dataKey] ?? []));
    nextUrl = page.links.next;
  }

  return allItems;
}

// ---------------------------------------------------------------------------
// Row Flattening
//
// Rename API fields to avoid ambiguity (e.g. "brand" -> "mentionedBrand"
// so it doesn't clash with the exported brand context fields).
// Remaining fields pass through as-is since SOV and visibility have
// different metric fields (shareOfVoice/mentionCount vs visibility).
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

function flatten(
  metric: MetricType,
  aggregates: Record<string, unknown>[],
  date: string,
  context: Record<string, unknown>,
): FlatRow[] {
  const renames = FIELD_RENAMES[metric];
  return aggregates.map((agg) => {
    const renamed: FlatRow = {};
    for (const [key, value] of Object.entries(agg)) {
      renamed[renames[key] ?? key] = value as string | number | boolean | null;
    }
    return { ...context, date, ...renamed };
  });
}

// ---------------------------------------------------------------------------
// Entity Discovery
// ---------------------------------------------------------------------------

interface EntityContext {
  brand: Brand;
  topics: Topic[];
  prompts: Array<Prompt & { topicName: string }>;
}

async function discoverEntities(config: ExportConfig): Promise<EntityContext> {
  console.log("Fetching brand details...");
  const { data: brand } = await apiFetch<{ data: Brand }>(
    buildUrl(`/brands/${config.brandId}`),
  );
  console.log(`  Brand: ${brand.name} (${brand.domain})`);

  console.log("Discovering topics...");
  const allTopics = await fetchAllPages<Topic>(
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
      const prompts = await fetchAllPages<Prompt>(
        `/topics/${topic.id}/prompts`,
        "prompts",
      );
      return prompts.map((p) => ({ ...p, topicName: topic.name }));
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
// Daily Aggregates
// ---------------------------------------------------------------------------

type Results = Record<EntityLevel, Record<MetricType, FlatRow[]>>;

async function fetchMetric(
  metric: MetricType,
  endpoint: string,
  date: string,
  context: Record<string, unknown>,
): Promise<FlatRow[]> {
  const aggregates = await fetchAllPages<Record<string, unknown>>(
    endpoint,
    "aggregates",
    { startDate: date, endDate: date },
  );
  return flatten(metric, aggregates, date, context);
}

async function fetchAllDailyAggregates(
  entities: EntityContext,
  dates: string[],
  concurrency: number,
): Promise<{ results: Results; errors: ExportError[] }> {
  const results: Results = {
    brand: { "share-of-voice": [], visibility: [], citations: [] },
    topic: { "share-of-voice": [], visibility: [], citations: [] },
    prompt: { "share-of-voice": [], visibility: [], citations: [] },
  };
  const errors: ExportError[] = [];
  const { brand, topics, prompts } = entities;
  const brandCtx = { brandId: brand.id, brandName: brand.name };

  console.log("\nFetching daily aggregates...");

  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    console.log(`  [${i + 1}/${dates.length}] ${date}`);

    // Build a flat list of work items for this day
    const work: Array<{
      level: EntityLevel;
      metric: MetricType;
      entityId: string;
      endpoint: string;
      context: Record<string, unknown>;
    }> = [];

    for (const metric of METRICS) {
      work.push({
        level: "brand",
        metric,
        entityId: brand.id,
        endpoint: `/brands/${brand.id}/${metric}/aggregates`,
        context: brandCtx,
      });
    }

    for (const topic of topics) {
      const ctx = { ...brandCtx, topicId: topic.id, topicName: topic.name };
      for (const metric of METRICS) {
        work.push({
          level: "topic",
          metric,
          entityId: topic.id,
          endpoint: `/topics/${topic.id}/${metric}/aggregates`,
          context: ctx,
        });
      }
    }

    for (const prompt of prompts) {
      const ctx = {
        ...brandCtx,
        topicId: prompt.topicId,
        topicName: prompt.topicName,
        promptId: prompt.id,
        promptQuery: prompt.query,
      };
      for (const metric of METRICS) {
        work.push({
          level: "prompt",
          metric,
          entityId: prompt.id,
          endpoint: `/prompts/${prompt.id}/${metric}/aggregates`,
          context: ctx,
        });
      }
    }

    await mapWithConcurrency(work, concurrency, async (item) => {
      try {
        const rows = await fetchMetric(
          item.metric,
          item.endpoint,
          date,
          item.context,
        );
        results[item.level][item.metric].push(...rows);
      } catch (error) {
        errors.push({
          date,
          level: item.level,
          metric: item.metric,
          entityId: item.entityId,
          error: String(error),
        });
      }
    });
  }

  return { results, errors };
}

// ---------------------------------------------------------------------------
// File Output
// ---------------------------------------------------------------------------

function writeJsonFile(filePath: string, data: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf-8");
  const sizeKb = (fs.statSync(filePath).size / 1024).toFixed(1);
  console.log(`  ${filePath} (${sizeKb} KB)`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const config = parseCliArgs();
  initClient(config.apiKey, config.apiBaseUrl);

  console.log("\n=== Omnia Data Export ===\n");
  console.log(`API:        ${config.apiBaseUrl}`);
  console.log(`Brand ID:   ${config.brandId}`);
  console.log(`Date range: ${config.startDate} to ${config.endDate}`);
  console.log(`Output:     ${config.outputDir}`);
  console.log(`Concurrency: ${config.concurrency}\n`);

  const entities = await discoverEntities(config);
  const dates = generateDateRange(config.startDate, config.endDate);

  const entityCount = 1 + entities.topics.length + entities.prompts.length;
  const tasksPerDay = entityCount * METRICS.length;
  console.log(`\nExport plan:`);
  console.log(
    `  ${entityCount} entities (1 brand + ${entities.topics.length} topics + ${entities.prompts.length} prompts)`,
  );
  console.log(
    `  ${dates.length} days x ${tasksPerDay} tasks/day = ${dates.length * tasksPerDay} total API calls`,
  );
  console.log(`  (paginated endpoints may require additional requests)`);

  const startTime = Date.now();
  const { results, errors } = await fetchAllDailyAggregates(
    entities,
    dates,
    config.concurrency,
  );
  const durationMs = Date.now() - startTime;

  // Write output files
  console.log("\nWriting output files...");
  for (const level of LEVELS) {
    fs.mkdirSync(path.join(config.outputDir, level), { recursive: true });
  }

  const manifest = {
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
          Object.fromEntries(METRICS.map((m) => [m, results[level][m].length])),
        ]),
      ),
    },
  };

  writeJsonFile(path.join(config.outputDir, "manifest.json"), manifest);
  for (const level of LEVELS) {
    for (const metric of METRICS) {
      writeJsonFile(
        path.join(config.outputDir, level, `${metric}.json`),
        results[level][metric],
      );
    }
  }

  // Summary
  const seconds = Math.round(durationMs / 1000);
  const duration =
    seconds < 60
      ? `${seconds}s`
      : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;

  console.log(`\n=== Export Complete ===\n`);
  console.log(`Duration:   ${duration}`);
  console.log(`API calls:  ${totalApiCalls}`);
  console.log(`Rows exported:`);
  for (const level of LEVELS) {
    const label = (level.charAt(0).toUpperCase() + level.slice(1)).padEnd(8);
    const sov = results[level]["share-of-voice"].length;
    const vis = results[level]["visibility"].length;
    const cit = results[level]["citations"].length;
    console.log(
      `  ${label} SOV: ${sov}, Visibility: ${vis}, Citations: ${cit}`,
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

  console.log(`\nOutput: ${path.resolve(config.outputDir)}`);
}

main().catch((error) => {
  console.error("\nFatal error:", String(error));
  process.exit(1);
});
