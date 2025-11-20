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

const API_URL =
  "https://amp-api.podcasts.apple.com/v1/catalog/us/search/groups";

const SCRAPE_NINJA_ENDPOINT = "https://scrapeninja.p.rapidapi.com/scrape";
const SCRAPE_NINJA_HOST = "scrapeninja.p.rapidapi.com";
const DEFAULT_SCRAPE_NINJA_API_KEY =
  "455e2a6556msheffc310f7420b51p102ea0jsn1c531be1e299";

const DB_CONFIG = {
  host: process.env.DB_HOST || "3.140.167.34",
  port: Number.parseInt(process.env.DB_PORT, 10) || 5432,
  user: process.env.DB_USER || "redash",
  password: process.env.DB_PASSWORD || "te83NECug38ueP",
  database: process.env.DB_NAME || "scrapers",
};

const FETCH_QUERIES_SQL =
  "select query from apple_podcasts.not_scraped_queries_vw";
const INSERT_SEARCH_SQL =
  "insert into apple_podcasts.searches(author_name, profile_title, query, url) values ($1, $2, $3, $4)";

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

function shouldUseScrapeNinja() {
  return String(process.env.SCRAPE_NINJA_ENABLED || "").toLowerCase() === "true";
}

function getScrapeNinjaApiKey() {
  return process.env.SCRAPE_NINJA_API_KEY || DEFAULT_SCRAPE_NINJA_API_KEY;
}

async function fetchViaScrapeNinja(url, headers) {
  const apiKey = getScrapeNinjaApiKey();

  if (!apiKey) {
    throw new Error(
      "SCRAPE_NINJA_API_KEY is required when SCRAPE_NINJA_ENABLED is true."
    );
  }

  const response = await fetch(SCRAPE_NINJA_ENDPOINT, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-rapidapi-key": apiKey,
      "x-rapidapi-host": SCRAPE_NINJA_HOST,
    },
    body: JSON.stringify({
      url,
      method: "GET",
      headers,
      autoparse: true,
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `ScrapeNinja request failed with status ${response.status}: ${text.slice(0, 200)}`
    );
  }

  const payload = await response.json();
  let parsedBody = payload?.body;

  if (typeof parsedBody === "string") {
    try {
      parsedBody = JSON.parse(parsedBody);
    } catch (error) {
      throw new Error(
        `ScrapeNinja returned a non-JSON body: ${parsedBody.slice(0, 200)}`
      );
    }
  }

  if (!parsedBody) {
    throw new Error("ScrapeNinja response did not include a body.");
  }

  return parsedBody;
}

function validateAuthHeaders(headers) {
  if (!headers.authorization) {
    throw new Error(
      "authorization (Bearer token) is required. Supply APPLE_AUTHORIZATION env var or data/headers.json"
    );
  }
}

function buildSearchUrl(query) {
  const url = new URL(API_URL);
  const params = {
    platform: "web",
    "extend": "editorialArtwork,feedUrl",
    "extend[podcast-channels]": "availableShowCount",
    "extend[podcasts]": "editorialArtwork",
    "include[podcast-episodes]": "channel,podcast",
    "include[podcasts]": "channel",
    limit: "25",
    groups: "category,channel,episode,show,top",
    with: "entitlements,transcripts",
    types: "podcasts,podcast-channels,podcast-episodes,categories,editorial-items",
    term: query,
    l: "en-US",
  };

  Object.entries(params).forEach(([key, value]) => {
    if (typeof value === "string") {
      url.searchParams.set(key, value);
    }
  });

  return url.toString();
}

function parseProfiles(responseJson, query) {
  if (Array.isArray(responseJson?.errors) && responseJson.errors.length) {
    const message = responseJson.errors
      .map((error) =>
        typeof error?.message === "string" ? error.message.trim() : ""
      )
      .filter(Boolean)
      .join("; ");

    throw new Error(message || "Apple Podcasts API returned an error response.");
  }

  const seen = new Set();

  const extractProfile = (attributes) => {
    if (!attributes || typeof attributes !== "object") {
      return null;
    }

    const url = typeof attributes.url === "string" ? attributes.url : "";

    if (!url || seen.has(url)) {
      return null;
    }

    const authorName =
      typeof attributes.artistName === "string" ? attributes.artistName : "";
    const profileTitle =
      typeof attributes.name === "string" ? attributes.name : "";

    seen.add(url);
    return { authorName, profileTitle, query, url };
  };

  const candidateProfiles = [];

  const collectAttributes = (value) => {
    if (!value || typeof value !== "object") {
      return;
    }

    if (Array.isArray(value)) {
      value.forEach((item) => collectAttributes(item));
      return;
    }

    if (value.attributes) {
      const profile = extractProfile(value.attributes);
      if (profile) {
        candidateProfiles.push(profile);
      }
    }

    Object.values(value).forEach((child) => {
      if (typeof child === "object") {
        collectAttributes(child);
      }
    });
  };

  collectAttributes(responseJson);

  return candidateProfiles;
}

async function fetchSearchResults(headers, query) {
  const url = buildSearchUrl(query);
  if (shouldUseScrapeNinja()) {
    return fetchViaScrapeNinja(url, headers);
  }

  const response = await fetch(url, { method: "GET", headers });

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
