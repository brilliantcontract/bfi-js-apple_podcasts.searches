import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { Pool } from "pg";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const HEADERS_FILE = path.join(DATA_DIR, "headers.json");

const API_URL = "https://amp-api.podcasts.apple.com/v1/catalog/us/search/groups";

const SCRAPE_NINJA_ENDPOINT = "https://scrapeninja.p.rapidapi.com/scrape";
const SCRAPE_NINJA_HOST = "scrapeninja.p.rapidapi.com";
const DEFAULT_SCRAPE_NINJA_API_KEY =
  "455e2a6556msheffc310f7420b51p102ea0jsn1c531be1e299";
const SCRAPE_NINJA_API_KEY =
  process.env.SCRAPE_NINJA_API_KEY || DEFAULT_SCRAPE_NINJA_API_KEY;
const USE_SCRAPE_NINJA = process.env.USE_SCRAPE_NINJA === "true";

const DB_CONFIG = {
  host: process.env.DB_HOST || "3.140.167.34",
  port: Number.parseInt(process.env.DB_PORT, 10) || 5432,
  user: process.env.DB_USER || "redash",
  password: process.env.DB_PASSWORD || "te83NECug38ueP",
  database: process.env.DB_NAME || "scrapers",
};

const FETCH_QUERIES_SQL =
  "SELECT query FROM apple_podcasts.not_scraped_queries_vw";
const INSERT_SEARCH_SQL =
  "INSERT INTO apple_podcasts.searches(author_name, profile_title, query, url) VALUES ($1, $2, $3, $4)";

function buildAuthorizationHeader(value) {
  if (!value || typeof value !== "string") {
    return "";
  }

  const trimmed = value.trim();
  return trimmed.toLowerCase().startsWith("bearer")
    ? trimmed
    : `Bearer ${trimmed}`;
}

const DEFAULT_HEADERS = {
  accept: "*/*",
  "accept-language": "en-US,en;q=0.9,ru;q=0.8,uk;q=0.7",
  authorization: buildAuthorizationHeader(process.env.APPLE_AUTHORIZATION),
  cookie: "geo=UA",
  origin: "https://podcasts.apple.com",
  priority: "u=1, i",
  referer: "https://podcasts.apple.com/",
  "sec-ch-ua":
    '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"Windows"',
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-site",
  "user-agent":
    process.env.USER_AGENT ||
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
};

async function ensureDataDir() {
  await fs.mkdir(DATA_DIR, { recursive: true });
}

async function loadHeaderOverrides() {
  try {
    const raw = await fs.readFile(HEADERS_FILE, "utf-8");
    const parsed = JSON.parse(raw);

    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    return parsed;
  } catch (error) {
    return {};
  }
}

function buildRequestHeaders(overrides) {
  const headers = { ...DEFAULT_HEADERS };

  Object.entries(overrides || {}).forEach(([key, value]) => {
    if (typeof value === "string" && value.trim() !== "") {
      headers[key.toLowerCase()] = value.trim();
    }
  });

  return Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [key, value]).filter(([, value]) => value !== "")
  );
}

function validateAuthHeaders(headers) {
  if (!headers.authorization) {
    throw new Error(
      "Missing required Apple Podcasts authorization header. Supply APPLE_AUTHORIZATION env var or data/headers.json"
    );
  }
}

function buildSearchUrl(query) {
  const url = new URL(API_URL);
  const params = url.searchParams;

  params.set("platform", "web");
  params.set("extend", "editorialArtwork,feedUrl");
  params.append("extend[podcast-channels]", "availableShowCount");
  params.append("extend[podcasts]", "editorialArtwork");
  params.append("include[podcast-episodes]", "channel,podcast");
  params.append("include[podcasts]", "channel");
  params.set("limit", "25");
  params.set("groups", "category,channel,episode,show,top");
  params.set("with", "entitlements,transcripts");
  params.set(
    "types",
    "podcasts,podcast-channels,podcast-episodes,categories,editorial-items"
  );
  params.set("term", query);
  params.set("l", "en-US");

  return url.toString();
}

function parseProfiles(responseJson, query) {
  if (Array.isArray(responseJson?.errors) && responseJson.errors.length) {
    const message = responseJson.errors
      .map((error) =>
        typeof error?.detail === "string"
          ? error.detail.trim()
          : typeof error?.title === "string"
            ? error.title.trim()
            : typeof error?.message === "string"
              ? error.message.trim()
              : ""
      )
      .filter(Boolean)
      .join("; ");

    throw new Error(message || "Apple Podcasts API returned an error response.");
  }

  const candidates = [];

  if (Array.isArray(responseJson?.data)) {
    candidates.push(...responseJson.data);
  }

  if (Array.isArray(responseJson?.included)) {
    candidates.push(...responseJson.included);
  }

  return candidates
    .filter((item) => item && typeof item === "object")
    .map((item) => {
      if (item.type && item.type !== "podcasts") {
        return null;
      }

      const attributes = item.attributes;

      if (!attributes || typeof attributes !== "object") {
        return null;
      }

      const authorName =
        typeof attributes.artistName === "string" ? attributes.artistName : "";
      const profileTitle =
        typeof attributes.name === "string" ? attributes.name : "";
      const url = typeof attributes.url === "string" ? attributes.url : "";

      if (!url) {
        return null;
      }

      return { authorName, profileTitle, query, url };
    })
    .filter(Boolean);
}

async function fetchSearchResults(headers, query) {
  if (USE_SCRAPE_NINJA) {
    const targetUrl = buildSearchUrl(query);
    const scrapeResponse = await fetch(SCRAPE_NINJA_ENDPOINT, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-rapidapi-host": SCRAPE_NINJA_HOST,
        "x-rapidapi-key": SCRAPE_NINJA_API_KEY,
      },
      body: JSON.stringify({
        url: targetUrl,
        method: "GET",
        headers,
      }),
    });

    if (!scrapeResponse.ok) {
      const text = await scrapeResponse.text();
      throw new Error(
        `Scrape Ninja request failed with status ${scrapeResponse.status}: ${text.slice(0, 200)}`
      );
    }

    const result = await scrapeResponse.json();
    const parsedBody = result?.body ? JSON.parse(result.body) : null;

    if (!parsedBody) {
      throw new Error("Scrape Ninja response did not include a parsable body.");
    }

    return parsedBody;
  }

  const response = await fetch(buildSearchUrl(query), {
    method: "GET",
    headers,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Request failed with status ${response.status}: ${text.slice(0, 200)}`
    );
  }

  return response.json();
}

async function loadQueries(pool) {
  const { rows } = await pool.query(FETCH_QUERIES_SQL);

  return rows
    .map((row) => (row && typeof row.query === "string" ? row.query.trim() : ""))
    .filter((value) => value !== "");
}

async function saveProfiles(pool, profiles) {
  if (!profiles.length) {
    return 0;
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const profile of profiles) {
      await client.query(INSERT_SEARCH_SQL, [
        profile.authorName,
        profile.profileTitle,
        profile.query,
        profile.url,
      ]);
    }

    await client.query("COMMIT");
    return profiles.length;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function main() {
  await ensureDataDir();

  const headerOverrides = await loadHeaderOverrides();
  const headers = buildRequestHeaders(headerOverrides);

  validateAuthHeaders(headers);

  const pool = new Pool(DB_CONFIG);

  try {
    const queries = await loadQueries(pool);

    if (!queries.length) {
      console.warn("No queries found to process.");
      return;
    }

    console.log(`Processing ${queries.length} quer${queries.length === 1 ? "y" : "ies"}.`);

    for (const query of queries) {
      try {
        const responseJson = await fetchSearchResults(headers, query);
        const profiles = parseProfiles(responseJson, query);
        
        if (!profiles.length) {
          console.warn(`No profiles returned for query: ${query}`);
          continue;
        }

        const inserted = await saveProfiles(pool, profiles);
        console.log(`Saved ${inserted} profile${inserted === 1 ? "" : "s"} for query "${query}".`);
      } catch (error) {
        console.error(`Failed to process query "${query}": ${error.message}`);
      }
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("Fatal error while running scraper:", error);
  process.exitCode = 1;
});
