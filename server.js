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

const API_URL = "https://api-partner.spotify.com/pathfinder/v2/query";

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

const FETCH_QUERIES_SQL = "select query from spotify.not_scraped_queries_vw";
const INSERT_SEARCH_SQL =
  "insert into spotify.searches(author_name, profile_title, query, url) values ($1, $2, $3, $4)";

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
  accept: "application/json",
  "accept-language": "ru",
  "app-platform": "WebPlayer",
  authorization: buildAuthorizationHeader(process.env.SPOTIFY_AUTHORIZATION),
  "client-token": process.env.SPOTIFY_CLIENT_TOKEN?.trim() || "",
  "content-type": "application/json;charset=UTF-8",
  origin: "https://open.spotify.com",
  priority: "u=1, i",
  referer: "https://open.spotify.com/",
  "sec-ch-ua":
    '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"Windows"',
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-site",
  "spotify-app-version": "1.2.78.120.g186ece09",
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
  const missing = [];

  if (!headers.authorization) {
    missing.push(
      "authorization (Bearer token). Supply SPOTIFY_AUTHORIZATION env var or data/headers.json"
    );
  }

  if (!headers["client-token"]) {
    missing.push(
      "client-token. Supply SPOTIFY_CLIENT_TOKEN env var or data/headers.json"
    );
  }

  if (missing.length) {
    throw new Error(
      `Missing required Spotify auth headers: ${missing.join("; ")}. Requests will fail with 401 until these are provided.`
    );
  }
}

function buildRequestBody(query) {
  return {
    variables: {
      includePreReleases: false,
      numberOfTopResults: 20,
      searchTerm: query,
      offset: 0,
      limit: 30,
      includeAudiobooks: true,
      includeAuthors: false,
    },
    operationName: "searchPodcasts",
    extensions: {
      persistedQuery: {
        version: 1,
        sha256Hash: "f4d1e6ff2422dd998e26ba696e853e4372811843361e91105f736d128d3d64e0",
      },
    },
  };
}

function buildUrlFromUri(uri) {
  if (typeof uri !== "string") {
    return null;
  }

  const parts = uri.split(":");

  if (parts.length < 3 || parts[0] !== "spotify") {
    return null;
  }

  const type = parts[1];
  const id = parts.slice(2).join(":");

  if (!type || !id) {
    return null;
  }

  return `https://open.spotify.com/${type}/${id}`;
}

function extractPodcastItems(responseJson) {
  const searchPodcasts = responseJson?.data?.searchPodcasts;
  const searchPodcastsV2 = responseJson?.data?.searchPodcastsV2;
  const searchV2 = responseJson?.data?.searchV2;
  const candidateArrays = [
    searchPodcasts?.items,
    searchPodcasts?.itemsV2,
    searchPodcasts?.podcasts?.items,
    searchPodcasts?.podcastUnionV2?.items,
    searchPodcastsV2?.items,
    searchPodcastsV2?.podcasts?.items,
    searchPodcastsV2?.podcastUnionV2?.items,
    searchV2?.podcasts?.items,
  ];

  return candidateArrays.find(Array.isArray) || [];
}
 
function parseProfiles(responseJson, query) {
  if (Array.isArray(responseJson?.errors) && responseJson.errors.length) {
    const message = responseJson.errors
      .map((error) =>
        typeof error?.message === "string" ? error.message.trim() : ""
      )
      .filter(Boolean)
      .join("; ");

    throw new Error(message || "Spotify API returned an error response.");
  }

  const items = extractPodcastItems(responseJson);

  if (!Array.isArray(items) || !items.length) {
    return [];
  }

  return items
    .map((item) => {
      const data =
        item && typeof item === "object" && item.data && typeof item.data === "object"
          ? item.data
          : item;

      if (!data || typeof data !== "object") {
        return null;
      }

      const authorName =
        typeof data?.publisher?.name === "string" ? data.publisher.name : "";
      const profileTitle = typeof data?.name === "string" ? data.name : "";
      const uri = typeof data?.uri === "string" ? data.uri : "";
      const url = buildUrlFromUri(uri);

      if (!url) {
        return null;
      }

      return { authorName, profileTitle, query, url };
    })
    .filter(Boolean);
}

async function fetchSearchResults(headers, query) {
  if (USE_SCRAPE_NINJA) {
    const scrapeResponse = await fetch(SCRAPE_NINJA_ENDPOINT, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-rapidapi-host": SCRAPE_NINJA_HOST,
        "x-rapidapi-key": SCRAPE_NINJA_API_KEY,
      },
      body: JSON.stringify({
        url: API_URL,
        method: "POST",
        headers,
        body: JSON.stringify(buildRequestBody(query)),
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

  const response = await fetch(API_URL, {
    method: "POST",
    headers,
    body: JSON.stringify(buildRequestBody(query)),
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
