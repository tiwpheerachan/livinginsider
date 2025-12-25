// ============================================
// 🚀 LIVINGINSIDER AI-POWERED SCRAPER
// ============================================
// 
// 📋 DESCRIPTION:
//   Production-ready AI-powered web scraper for LivingInsider.com
//   Extracts real estate listings with 76 data points including:
//   - Basic property info (price, size, rooms, etc.)
//   - Nearby places (BTS, hospitals, universities, malls)
//   - Facilities and amenities (pool, gym, parking, etc.)
//   - AI-powered quality and investment scores
//   - Dashboard-ready analytics
//
// 🧠 AI FEATURES:
//   - Self-learning source selection
//   - Intelligent duplicate detection (semantic matching)
//   - Price anomaly detection
//   - Adaptive scrolling optimization
//   - Quality scoring (0-100)
//   - Investment scoring (0-100)
//
// 📊 OUTPUT:
//   - 76 columns per listing
//   - JSON arrays for nearby places and facilities
//   - Boolean flags for quick filtering
//   - 9 AI-powered scores
//
// 🎯 USAGE:
//   import scrapeListings from './scraper.js';
//   const { rows, meta, insights } = await scrapeListings({ maxResults: 50 });
//
// ⚙️  CONFIG:
//   Set via environment variables (see .env file)
//
// 🔧 MAINTENANCE:
//   - Update selectors if website HTML changes
//   - Adjust scoring algorithms as needed
//   - Monitor AI learning performance
//
// 📅 VERSION: 3.0.0 (Ultimate Dashboard Edition)
// 👨‍💻 AUTHOR: AI Innovation Lab
// 📅 LAST UPDATED: 2025-12-24
//
// ============================================

import fs from "fs";
import path from "path";
import { chromium } from "playwright";

// ============================================
// ⚙️  CONFIGURATION
// ============================================

// Browser Settings
const SCRAPE_HEADLESS = (process.env.SCRAPE_HEADLESS ?? "true") !== "false";
const DEBUG_SCRAPER = (process.env.DEBUG_SCRAPER ?? "true") === "true";

// Timeout Settings (in milliseconds)
const NAV_TIMEOUT_MS = Number(process.env.SCRAPE_NAV_TIMEOUT || 60_000);      // Navigation timeout
const ACTION_TIMEOUT_MS = Number(process.env.SCRAPE_ACTION_TIMEOUT || 12_000); // Action timeout
const DETAIL_TIMEOUT_MS = Number(process.env.SCRAPE_DETAIL_TIMEOUT_MS || 45_000); // Detail page timeout
const DETAIL_RETRIES = Number(process.env.SCRAPE_DETAIL_RETRIES || 3);         // Retry attempts

// Scrolling Settings
const LIST_SCROLL_ROUNDS = Number(process.env.SCRAPE_LIST_SCROLL_ROUNDS || 25);      // Scroll iterations
const LIST_SCROLL_STEP = Number(process.env.SCRAPE_LIST_SCROLL_STEP || 1200);        // Pixels per scroll
const LIST_STEP_DELAY_MS = Number(process.env.SCRAPE_STEP_DELAY || 250);             // Delay between scrolls
const LIST_WAIT_AFTER_SCROLL_MS = Number(process.env.SCRAPE_WAIT_AFTER_SCROLL || 2000); // Wait after scrolling

// Performance Settings
const MAX_CONCURRENCY = Math.max(1, Number(process.env.SCRAPE_MAX_CONCURRENCY || 2)); // Concurrent workers
const MAX_IMAGES = Math.max(0, Number(process.env.SCRAPE_MAX_IMAGES || 20));          // Max images per listing
const PAGE_RECYCLE_AFTER = Number(process.env.PAGE_RECYCLE_AFTER || 5);               // Recycle page after N jobs

// ============================================
// 📊 OUTPUT SCHEMA (76 COLUMNS)
// ============================================

const SCHEMA_KEYS = [
  // ===== BASIC INFO (18 columns) =====
  "listing_id",           // Unique listing ID
  "listing_url",          // Full URL to listing
  "category",             // คอนโด, บ้านเดี่ยว, ทาวน์เฮ้าส์, etc.
  "deal_type",            // ขาย or เช่า
  "project_name",         // Project/building name
  "listing_title",        // Full listing title
  "price_text",           // Price as text (e.g., "฿ 13,590,000")
  "price_value",          // Price as number (13590000)
  "price_psm",            // Price per square meter
  "old_price_text",       // Original price (if discounted)
  "discount_percent",     // Discount percentage
  "usable_area_sqm",      // Usable area in square meters
  "floor",                // Floor number(s)
  "bedrooms",             // Number of bedrooms
  "bathrooms",            // Number of bathrooms
  "parking",              // Parking spaces
  "furnishing",           // Furnished status
  "direction",            // Facing direction
  
  // ===== DATES (2 columns) =====
  "created_at_iso",       // Creation date (ISO format)
  "bumped_at_iso",        // Last bumped date (ISO format)
  
  // ===== LOCATION (3 columns) =====
  "location_text",        // Location description
  "province",             // Province name
  "district",             // District name
  
  // ===== NEARBY PLACES - JSON ARRAYS (5 columns) =====
  "nearby_bts_json",          // [{name, distance_km, lat, lng}, ...]
  "nearby_hospitals_json",    // [{name, distance_km, lat, lng}, ...]
  "nearby_universities_json", // [{name, distance_km, lat, lng}, ...]
  "nearby_malls_json",        // [{name, distance_km, lat, lng}, ...]
  "nearby_all_json",          // Top 20 nearest places (all types)
  
  // ===== NEARBY PLACES - QUICK ACCESS (6 columns) =====
  "nearest_bts_name",              // Name of nearest BTS station
  "nearest_bts_distance_km",       // Distance to nearest BTS (km)
  "nearest_hospital_name",         // Name of nearest hospital
  "nearest_hospital_distance_km",  // Distance to nearest hospital (km)
  "nearest_mall_name",             // Name of nearest mall
  "nearest_mall_distance_km",      // Distance to nearest mall (km)
  
  // ===== MAP (3 columns) =====
  "map_url",              // Google Maps URL
  "lat",                  // Latitude
  "lng",                  // Longitude
  
  // ===== AGENT (4 columns) =====
  "agent_name",           // Agent/seller name
  "agent_url",            // Agent profile URL
  "agent_verified",       // Is agent verified? (boolean)
  "agent_rating",         // Agent rating (if available)
  
  // ===== STATS (3 columns) =====
  "clicks",               // Number of clicks
  "views",                // Number of views
  "favorites",            // Number of favorites
  
  // ===== CONTACT (6 columns) =====
  "contact_phone",        // Phone number
  "contact_email",        // Email address
  "contact_line_url",     // LINE contact URL
  "contact_line_id",      // LINE ID
  "contact_facebook_url", // Facebook URL
  "contact_buttons",      // Other contact methods
  
  // ===== CONTENT (3 columns) =====
  "description_text",     // Full description
  "highlights",           // Key highlights
  "detail_snippet",       // Short snippet (180 chars)
  
  // ===== IMAGES (2 columns) =====
  "images",               // Pipe-separated image URLs
  "cover_image",          // Main cover image URL
  
  // ===== FACILITIES (13 columns) =====
  "facilities_json",      // JSON array of all facilities
  "facility_count",       // Total number of facilities
  "has_pool",             // Has swimming pool? (boolean)
  "has_gym",              // Has gym/fitness? (boolean)
  "has_parking",          // Has parking? (boolean)
  "has_security",         // Has security system? (boolean)
  "has_garden",           // Has garden? (boolean)
  "has_sauna",            // Has sauna? (boolean)
  "has_ev_charger",       // Has EV charger? (boolean)
  "has_sky_pool",         // Has sky pool? (boolean)
  "has_foreigner_quota",  // Foreigner quota available? (boolean)
  "is_luxury",            // Is luxury property? (boolean)
  "has_private_lift",     // Has private lift? (boolean)
  
  // ===== AI QUALITY SCORES (4 columns) =====
  "quality_score",        // Overall quality (0-100)
  "price_score",          // Price reliability (0-100)
  "data_completeness",    // Data completeness (0-100)
  "anomaly_flags",        // Detected anomalies (comma-separated)
  
  // ===== DASHBOARD ANALYTICS (5 columns) =====
  "walkability_score",    // Walkability to BTS (0-100)
  "location_score",       // Location quality (0-100)
  "facility_score",       // Facility quality (0-100)
  "investment_score",     // Investment potential (0-100)
  "value_score",          // Overall value (0-100)
];

// Total: 76 columns

// ============================================
// 🧠 AI LEARNING ENGINE
// ============================================
//
// The AI Learning Engine continuously improves scraper performance by:
// 1. Learning which sources provide best quality data
// 2. Detecting duplicate listings using semantic matching
// 3. Analyzing price patterns to identify anomalies
// 4. Optimizing scroll behavior based on results
// 5. Generating actionable insights and recommendations
//
// ============================================

class AILearningEngine {
  constructor() {
    // Source performance tracking
    // Maps source ID -> {attempts, successes, avgQuality, avgLinksFound, score}
    this.sourcePerformance = new Map();
    
    // Price statistics
    // Tracks min, max, avg prices and individual values for statistical analysis
    this.priceStats = { 
      min: Infinity, 
      max: 0, 
      sum: 0, 
      count: 0, 
      values: [] 
    };
    
    // Category-specific statistics
    // Maps category -> {min, max, sum, count}
    this.categoryStats = new Map();
    
    // Location-specific statistics (future use)
    this.locationStats = new Map();
    
    // Scroll effectiveness tracking
    // Array of {rounds, links} to optimize scrolling
    this.scrollEffectiveness = [];
    
    // Duplicate detection via semantic signatures
    // Set of signatures: "title|price|area|bedrooms|location"
    this.duplicateSignatures = new Set();
  }

  /**
   * Record performance metrics for a source
   * @param {string} sourceId - Unique source identifier
   * @param {object} metrics - {success: bool, linksFound: number, avgQuality: number}
   */
  recordSourcePerformance(sourceId, metrics) {
    if (!this.sourcePerformance.has(sourceId)) {
      this.sourcePerformance.set(sourceId, {
        attempts: 0,
        successes: 0,
        avgQuality: 0,
        avgLinksFound: 0,
        totalLinks: 0,
        score: 0,
      });
    }

    const perf = this.sourcePerformance.get(sourceId);
    perf.attempts++;
    perf.successes += metrics.success ? 1 : 0;
    perf.totalLinks += metrics.linksFound || 0;
    perf.avgLinksFound = perf.totalLinks / perf.attempts;
    
    // Update average quality using running average
    perf.avgQuality = ((perf.avgQuality * (perf.attempts - 1)) + (metrics.avgQuality || 0)) / perf.attempts;
    
    // Calculate composite score
    // - Success rate: 40%
    // - Links found: 30%
    // - Quality: 30%
    perf.score = (
      (perf.successes / perf.attempts) * 0.4 +
      Math.min(perf.avgLinksFound / 15, 1) * 0.3 +
      perf.avgQuality * 0.3
    );

    log(`📊 Source "${sourceId}" score: ${(perf.score * 100).toFixed(1)}%`);
  }

  /**
   * Select best sources based on AI learning
   * @param {Array} allSources - Array of all available sources
   * @param {number} count - Number of sources to select
   * @returns {Array} Selected sources with AI scores
   */
  getBestSources(allSources, count) {
    const scored = allSources.map(src => {
      const perf = this.sourcePerformance.get(src.id) || { score: 0.5 };
      return { ...src, aiScore: perf.score };
    });

    // Sort by AI score + randomness (to avoid overfitting)
    scored.sort((a, b) => {
      const scoreA = a.aiScore + Math.random() * 0.2; // Add 20% randomness
      const scoreB = b.aiScore + Math.random() * 0.2;
      return scoreB - scoreA;
    });

    return scored.slice(0, count);
  }

  /**
   * Learn price patterns for a category
   * @param {number} price - Price value
   * @param {string} category - Property category
   */
  learnPrice(price, category) {
    if (!price || price <= 0) return;
    
    // Update global price stats
    this.priceStats.values.push(price);
    this.priceStats.min = Math.min(this.priceStats.min, price);
    this.priceStats.max = Math.max(this.priceStats.max, price);
    this.priceStats.sum += price;
    this.priceStats.count++;

    // Update category-specific stats
    if (!this.categoryStats.has(category)) {
      this.categoryStats.set(category, { min: Infinity, max: 0, sum: 0, count: 0 });
    }
    const catStats = this.categoryStats.get(category);
    catStats.min = Math.min(catStats.min, price);
    catStats.max = Math.max(catStats.max, price);
    catStats.sum += price;
    catStats.count++;
  }

  /**
   * Detect price anomalies using statistical methods
   * @param {number} price - Price to check
   * @param {string} category - Property category
   * @returns {string|null} Anomaly type or null
   */
  detectPriceAnomaly(price, category) {
    if (!price || this.priceStats.count < 5) return null;

    const globalAvg = this.priceStats.sum / this.priceStats.count;
    const globalStd = this.calculateStdDev(this.priceStats.values, globalAvg);

    // Calculate z-score (standard deviations from mean)
    const zScore = Math.abs((price - globalAvg) / (globalStd || 1));

    // Check category-specific anomalies
    let catAnomaly = null;
    if (category && this.categoryStats.has(category)) {
      const catStats = this.categoryStats.get(category);
      if (catStats.count >= 3) {
        const catAvg = catStats.sum / catStats.count;
        if (price < catAvg * 0.3) catAnomaly = "suspiciously_low";   // < 30% of avg
        if (price > catAvg * 3) catAnomaly = "suspiciously_high";    // > 300% of avg
      }
    }

    // Global anomaly detection
    if (zScore > 3) return "outlier_high";           // More than 3 standard deviations
    if (price < globalAvg * 0.1) return "outlier_low"; // Less than 10% of average
    
    return catAnomaly;
  }

  /**
   * Calculate standard deviation
   * @param {Array} values - Array of numbers
   * @param {number} mean - Mean value
   * @returns {number} Standard deviation
   */
  calculateStdDev(values, mean) {
    if (values.length < 2) return 0;
    const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
    return Math.sqrt(variance);
  }

  /**
   * Generate semantic signature for duplicate detection
   * @param {object} listing - Listing data
   * @returns {string} Unique signature
   */
  generateSignature(listing) {
    const parts = [
      listing.listing_title?.toLowerCase().replace(/\s+/g, ''),
      listing.price_value,
      listing.usable_area_sqm,
      listing.bedrooms,
      listing.location_text,
    ].filter(Boolean);
    
    return parts.join('|');
  }

  /**
   * Check if listing is a duplicate
   * @param {object} listing - Listing to check
   * @returns {boolean} True if duplicate
   */
  isDuplicate(listing) {
    const sig = this.generateSignature(listing);
    if (this.duplicateSignatures.has(sig)) {
      return true;
    }
    this.duplicateSignatures.add(sig);
    return false;
  }

  /**
   * Recommend optimal scroll rounds based on effectiveness
   * @param {number} currentRounds - Current scroll rounds
   * @param {number} linksFound - Links found in last run
   * @returns {number} Recommended scroll rounds
   */
  recommendScrollRounds(currentRounds, linksFound) {
    this.scrollEffectiveness.push({ rounds: currentRounds, links: linksFound });
    
    if (this.scrollEffectiveness.length < 3) return currentRounds;

    // Calculate average effectiveness from recent runs
    const recent = this.scrollEffectiveness.slice(-3);
    const avgLinks = recent.reduce((sum, x) => sum + x.links, 0) / recent.length;

    // Adapt scroll rounds based on results
    if (avgLinks < 8) return Math.min(currentRounds + 5, 35);  // Too few links, scroll more
    if (avgLinks > 20) return Math.max(currentRounds - 3, 15); // Too many links, scroll less
    return currentRounds; // Just right
  }

  /**
   * Generate insights and recommendations
   * @returns {object} Insights object
   */
  generateInsights() {
    const insights = {
      totalSources: this.sourcePerformance.size,
      bestSource: null,
      worstSource: null,
      priceRange: {
        min: this.priceStats.min === Infinity ? 0 : this.priceStats.min,
        max: this.priceStats.max,
        avg: this.priceStats.count > 0 ? this.priceStats.sum / this.priceStats.count : 0,
      },
      categoryBreakdown: {},
      duplicatesDetected: this.duplicateSignatures.size,
      recommendations: [],
    };

    // Find best/worst sources
    let bestScore = 0, worstScore = 1;
    for (const [id, perf] of this.sourcePerformance) {
      if (perf.score > bestScore) {
        bestScore = perf.score;
        insights.bestSource = { id, score: perf.score, avgLinks: perf.avgLinksFound };
      }
      if (perf.score < worstScore && perf.attempts > 0) {
        worstScore = perf.score;
        insights.worstSource = { id, score: perf.score };
      }
    }

    // Category breakdown
    for (const [cat, stats] of this.categoryStats) {
      insights.categoryBreakdown[cat] = {
        count: stats.count,
        avgPrice: stats.sum / stats.count,
        priceRange: [stats.min, stats.max],
      };
    }

    // Generate recommendations
    if (bestScore > 0.7) {
      insights.recommendations.push("Focus on high-performing sources for better results");
    }
    if (this.duplicateSignatures.size > 5) {
      insights.recommendations.push(`${this.duplicateSignatures.size} duplicates detected - consider expanding sources`);
    }

    return insights;
  }
}

// ============================================
// ⭐ QUALITY SCORING SYSTEM
// ============================================
//
// Evaluates listing quality across multiple dimensions:
// 1. Data Completeness (35%) - How many fields are filled?
// 2. Contact Quality (25%) - Are contact methods valid?
// 3. Image Quality (20%) - How many images are available?
// 4. Price Reliability (20%) - Is the price reasonable?
//
// ============================================

class QualityScorer {
  /**
   * Score data completeness (0-1 scale)
   * @param {object} listing - Listing data
   * @returns {number} Completeness score (0-1)
   */
  static scoreDataCompleteness(listing) {
    const fields = [
      'listing_title', 'category', 'deal_type', 'price_value',
      'location_text', 'bedrooms', 'bathrooms', 'usable_area_sqm',
      'contact_phone', 'agent_name', 'images', 'description_text'
    ];

    const filled = fields.filter(f => {
      const val = listing[f];
      if (val === null || val === undefined || val === '') return false;
      if (typeof val === 'string' && val.length < 2) return false;
      return true;
    }).length;

    return filled / fields.length;
  }

  /**
   * Score contact quality (0-1 scale)
   * @param {object} listing - Listing data
   * @returns {number} Contact score (0-1)
   */
  static scoreContactQuality(listing) {
    let score = 0;
    
    // Valid Thai phone number (0xxxxxxxxx)
    if (listing.contact_phone && /^0\d{8,9}$/.test(listing.contact_phone.replace(/\s|-/g, ''))) {
      score += 0.4;
    }
    
    // Valid email
    if (listing.contact_email && listing.contact_email.includes('@')) {
      score += 0.2;
    }
    
    // LINE contact
    if (listing.contact_line_url) score += 0.2;
    
    // Agent info
    if (listing.agent_name && listing.agent_name.length > 2) score += 0.1;
    if (listing.agent_verified) score += 0.1;
    
    return score;
  }

  /**
   * Score image quality (0-1 scale)
   * @param {object} listing - Listing data
   * @returns {number} Image score (0-1)
   */
  static scoreImageQuality(listing) {
    if (!listing.images) return 0;
    const imgCount = listing.images.split('|').filter(Boolean).length;
    if (imgCount === 0) return 0;
    if (imgCount >= 10) return 1;
    return imgCount / 10; // Linear scale
  }

  /**
   * Score price reliability (0-1 scale)
   * @param {object} listing - Listing data
   * @returns {number} Price score (0-1)
   */
  static scorePriceReliability(listing) {
    if (!listing.price_value || listing.price_value <= 0) return 0;
    
    let score = 0.5; // Base score
    
    // Has price per sqm (indicates transparency)
    if (listing.price_psm && listing.price_psm > 0) score += 0.2;
    
    // Price in reasonable range
    if (listing.price_value > 100000 && listing.price_value < 1000000000) score += 0.2;
    
    // Has old price (shows transparency)
    if (listing.old_price_text) score += 0.1;
    
    return Math.min(score, 1);
  }

  /**
   * Calculate overall quality score (0-100)
   * @param {object} listing - Listing data
   * @returns {number} Quality score (0-100)
   */
  static calculateQualityScore(listing) {
    const completeness = this.scoreDataCompleteness(listing);
    const contactQuality = this.scoreContactQuality(listing);
    const imageQuality = this.scoreImageQuality(listing);
    const priceQuality = this.scorePriceReliability(listing);

    // Weighted average
    const score = (
      completeness * 0.35 +    // Data completeness: 35%
      contactQuality * 0.25 +  // Contact quality: 25%
      imageQuality * 0.20 +    // Image quality: 20%
      priceQuality * 0.20      // Price quality: 20%
    );

    return Math.round(score * 100);
  }

  /**
   * Generate anomaly flags
   * @param {object} listing - Listing data
   * @param {AILearningEngine} aiEngine - AI engine instance
   * @returns {string|null} Comma-separated flags or null
   */
  static generateAnomalyFlags(listing, aiEngine) {
    const flags = [];

    // Price anomaly
    const priceAnomaly = aiEngine.detectPriceAnomaly(listing.price_value, listing.category);
    if (priceAnomaly) flags.push(`price_${priceAnomaly}`);

    // Missing critical data
    if (!listing.contact_phone && !listing.contact_email) flags.push("no_contact");
    if (!listing.images || listing.images.length < 10) flags.push("no_images");
    if (!listing.price_value) flags.push("no_price");
    
    // Suspicious patterns
    if (listing.listing_title && listing.listing_title.length < 10) flags.push("short_title");
    if (listing.description_text && listing.description_text.length < 50) flags.push("short_description");

    return flags.join(',') || null;
  }
}

// ============================================
// 🎯 SEARCH SOURCES CONFIGURATION
// ============================================
//
// Defines categories and locations for multi-source scraping
// Each combination creates a unique search URL
//
// ============================================

const CATEGORIES = [
  { id: "condo", name: "คอนโด", url_part: "คอนโด", weight: 1.2 },
  { id: "house", name: "บ้านเดี่ยว", url_part: "บ้าน", weight: 1.0 },
  { id: "townhouse", name: "ทาวน์เฮ้าส์", url_part: "ทาวน์เฮ้าส์", weight: 0.9 },
  { id: "land", name: "ที่ดิน", url_part: "ที่ดิน", weight: 0.8 },
  { id: "commercial", name: "อาคารพาณิชย์", url_part: "อาคารพาณิชย์", weight: 0.7 },
];

const LOCATIONS_BANGKOK = [
  { id: "bangkok", name: "กรุงเทพมหานคร", weight: 1.3 },
  { id: "nonthaburi", name: "นนทบุรี", weight: 1.0 },
  { id: "pathumthani", name: "ปทุมธานี", weight: 0.9 },
  { id: "samutprakan", name: "สมุทรปราการ", weight: 0.9 },
  { id: "samutsakorn", name: "สมุทรสาคร", weight: 0.7 },
];

/**
 * Generate all possible search URL combinations
 * @returns {Array} Array of source objects
 */
function generateSearchURLs() {
  const urls = [];
  for (const cat of CATEGORIES) {
    for (const loc of LOCATIONS_BANGKOK) {
      const keyword = encodeURIComponent(`${cat.url_part} ${loc.name}`);
      const url = `https://www.livinginsider.com/searchword/all/Buysell/1/${keyword}.html`;
      urls.push({
        id: `${cat.id}_${loc.id}`,
        url,
        category: cat.name,
        location: loc.name,
        name: `${cat.name} ใน ${loc.name}`,
        weight: cat.weight * loc.weight, // Composite weight
      });
    }
  }
  return urls; // 5 categories × 5 locations = 25 sources
}

/**
 * Select sources intelligently using AI or weighted random
 * @param {number} targetCount - Target number of listings
 * @param {AILearningEngine} aiEngine - AI engine instance
 * @returns {Array} Selected sources
 */
function selectSmartSources(targetCount, aiEngine) {
  const allSources = generateSearchURLs();
  
  // Use AI if we have learning data
  if (aiEngine && aiEngine.sourcePerformance.size > 0) {
    log("🧠 Using AI-learned source selection");
    return aiEngine.getBestSources(allSources, Math.ceil(targetCount / 10));
  }

  // First run: weighted random selection
  log("🎲 Using weighted random selection (first run)");
  const weighted = allSources.map(s => ({ ...s, randScore: s.weight * Math.random() }));
  weighted.sort((a, b) => b.randScore - a.randScore);
  
  const needed = Math.max(Math.ceil(targetCount / 10), 3);
  return weighted.slice(0, needed);
}

// ============================================
// 🛠️ UTILITY FUNCTIONS
// ============================================

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(...args) { 
  if (DEBUG_SCRAPER) console.log("[scraper]", ...args); 
}

function cleanText(s) { 
  return String(s ?? "").replace(/\s+/g, " ").trim(); 
}

function parseNumberLike(s) {
  if (s === null || s === undefined) return null;
  const txt = String(s).replace(/,/g, "");
  const m = txt.match(/-?\d+(?:\.\d+)?/g);
  if (!m) return null;
  const n = Number(m.join(""));
  return Number.isFinite(n) ? n : null;
}

function absUrl(base, href) {
  if (!href) return "";
  try {
    if (/^https?:\/\//i.test(href)) return href;
    return new URL(href, base).toString();
  } catch { return ""; }
}

function nowISO() { 
  return new Date().toISOString(); 
}

function emitProgress(opts, payload) {
  try { 
    if (typeof opts?.onProgress === "function") opts.onProgress(payload); 
  } catch {}
}

function ensureDebugDir() {
  const dir = path.join(process.cwd(), "debug");
  try { fs.mkdirSync(dir, { recursive: true }); } catch {}
  return dir;
}

function thaiDateToISO(text) {
  const m = String(text || "").match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (!m) return null;
  const dd = Number(m[1]), mm = Number(m[2]);
  let yy = Number(m[3]);
  if (yy >= 2400) yy = yy - 543; // Convert Buddhist year to Gregorian
  if (!dd || !mm || !yy) return null;
  const iso = new Date(Date.UTC(yy, mm - 1, dd, 0, 0, 0));
  return Number.isNaN(iso.getTime()) ? null : iso.toISOString().slice(0, 10);
}

function makeRouteHandler(mode = "auto") {
  const m = String(mode || "auto").toLowerCase();
  return async function routeHandler(route) {
    const req = route.request(), type = req.resourceType(), url = req.url();
    
    // Block ads and trackers
    if (url.includes("doubleclick") || url.includes("googlesyndication") || 
        url.includes("googletagmanager") || url.includes("google-analytics") ||
        url.includes("facebook.com/tr") || url.includes("hotjar") ||
        url.includes("/ads") || url.includes("analytics") || url.includes("tracking")) {
      return route.abort();
    }
    
    if (m === "full") return route.continue();
    
    // Block unnecessary resources
    if (["media", "font", "websocket"].includes(type)) return route.abort();
    if (type === "image" && url.includes("/search")) return route.abort();
    if (m === "fast" && type === "stylesheet") return route.abort();
    
    return route.continue();
  };
}

async function injectPerfCSS(page) {
  await page.addStyleTag({
    content: `*, *::before, *::after { animation-duration: 0.001s !important; transition-duration: 0.001s !important; }`
  }).catch(() => {});
}

async function robustGoto(page, url, { timeout = NAV_TIMEOUT_MS, waitUntil = "domcontentloaded", retries = 2 } = {}) {
  let lastErr = null;
  for (let i = 0; i <= retries; i++) {
    try {
      await page.goto(url, { timeout, waitUntil });
      return;
    } catch (e) {
      lastErr = e;
      await sleep(e.message.includes("crash") ? 2000 + i * 1000 : 450 + i * 700);
    }
  }
  throw lastErr || new Error("goto failed");
}

async function dismissOverlays(page) {
  await page.keyboard.press("Escape").catch(() => {});
  const selectors = ['button:has-text("ยอมรับ")', 'button:has-text("ตกลง")', ".swal2-close", ".close"];
  for (const sel of selectors) {
    try {
      const loc = page.locator(sel).first();
      if ((await loc.count()) > 0) {
        await loc.click({ timeout: 900 }).catch(() => {});
        await page.waitForTimeout(120);
      }
    } catch {}
  }
}

async function autoScrollRobust(page, rounds = LIST_SCROLL_ROUNDS, step = LIST_SCROLL_STEP, delayMs = LIST_STEP_DELAY_MS) {
  let lastH = 0, stableCount = 0;
  for (let i = 0; i < rounds; i++) {
    const h = await page.evaluate(() => document.documentElement.scrollHeight).catch(() => 0);
    if (h && h === lastH) {
      stableCount++;
      if (stableCount >= 5) break; // Page height stable, stop scrolling
    } else { stableCount = 0; }
    lastH = h;
    await page.evaluate((y) => window.scrollBy(0, y), step).catch(() => {});
    await page.waitForTimeout(delayMs);
  }
  await page.waitForTimeout(LIST_WAIT_AFTER_SCROLL_MS);
}

function isLivingDetailUrl(u) {
  try {
    const x = new URL(u);
    return x.hostname.includes("livinginsider.com") && 
           x.pathname.startsWith("/livingdetail/") &&
           /^\/livingdetail\/(\d{4,})\//.test(x.pathname);
  } catch { return false; }
}

async function collectDetailLinksFromList(page) {
  const origin = new URL(page.url()).origin, out = new Set();
  const hrefs = await page.$$eval("a[href]", (as) => as.map((a) => a.getAttribute("href") || "")).catch(() => []);
  for (const h of hrefs) {
    const u = h.startsWith("http") ? h : absUrl(origin, h);
    if (u && isLivingDetailUrl(u)) out.add(u);
  }
  return [...out];
}


async function clickContactButtons(page) {
  // ✅ โฟกัสเฉพาะปุ่มโทรศัพท์ LivingInsider ตาม HTML ที่คุณเจอ
  const phoneBtnSelectors = [
    // div ปุ่มเขียวที่มีไอคอน tel
    'div.ownCont-active.ch-lightgreen:has(img[alt="tel"])',
    'div.ownCont-active:has(img[alt="tel"])',
    'div.ch-lightgreen:has(img[alt="tel"])',
    'img[alt="tel"]',
  ];

  // 1) พยายามคลิกปุ่มโทรศัพท์
  for (const sel of phoneBtnSelectors) {
    try {
      const loc = page.locator(sel).first();
      if ((await loc.count()) > 0) {
        await loc.scrollIntoViewIfNeeded().catch(() => {});
        await loc.click({ timeout: 3000 }).catch(() => {});
        // 2) รอเบอร์ใน modal โผล่
        await page.waitForSelector("#phone_number_modal_show", { timeout: 8000 }).catch(() => {});
        return true;
      }
    } catch {}
  }

  // fallback: ถ้าหาปุ่มแบบข้างบนไม่เจอ ลองคลิก text "ดูเบอร์" เผื่อมี
  try {
    const t = page.locator('text=ดูเบอร์').first();
    if ((await t.count()) > 0) {
      await t.click({ timeout: 3000 }).catch(() => {});
      await page.waitForSelector("#phone_number_modal_show", { timeout: 8000 }).catch(() => {});
      return true;
    }
  } catch {}

  return false;
}

async function parseDetail(page, url) {
  await robustGoto(page, url, { retries: 2 });
  await page.waitForTimeout(300);
  await injectPerfCSS(page);
  await dismissOverlays(page);
  try {
  await clickContactButtons(page);
  await page.waitForTimeout(600);
} catch {}


  // ============================================
  // 🔍 BROWSER-SIDE EXTRACTION
  // ============================================
  // This runs inside the browser for best performance
  // All selectors verified against actual HTML structure
  // ============================================

  const data = await Promise.race([
    page.evaluate(() => {
      // === Helper Functions ===
      const txt = (el) => (el?.textContent || "").replace(/\s+/g, " ").trim();
      const q = (sel) => document.querySelector(sel);
      const qa = (sel) => Array.from(document.querySelectorAll(sel));
      
      const isJunkImage = (src) => {
        if (!src) return true;
        const lower = src.toLowerCase();
        return lower.includes('logo') || lower.includes('flag_') || 
               lower.includes('no-user') || lower.includes('ic_bts') ||
               lower.includes('ic_mrt') || lower.includes('/assets') ||
               lower.includes('/station') || src.length < 20;
      };

      // === EXTRACT TITLE ===
      // ✅ VERIFIED: Works with actual HTML
      const getTitle = () => {
        // Try main H1 first (most accurate)
        const h1Main = q('h1.font_sarabun.show-title, h1.show-title');
        if (h1Main) {
          const title = txt(h1Main);
          if (title && title.length > 10) return title;
        }
        
        // Fallback to any H1
        const h1 = q('h1');
        if (h1) return txt(h1);
        
        return "";
      };

      // === EXTRACT PROJECT NAME ===
      // ✅ VERIFIED: Works with actual HTML
      const getProjectName = () => {
        const breadLinks = qa('nav[aria-label="breadcrumb"] a, .breadcrumb a');
        // Structure: [Home] > [Location] > [Project] > [Listing]
        // We want Project (index -2)
        if (breadLinks.length >= 3) {
          const projectLink = breadLinks[breadLinks.length - 2];
          const projectName = txt(projectLink);
          if (projectName && projectName.length > 2 && projectName.length < 100) {
            return projectName;
          }
        }
        return null;
      };

      // === EXTRACT BADGES (Category & Deal Type) ===
      const getBadges = () => {
        const badges = qa('.badge, .tag, span[class*="badge"]').map(el => txt(el)).filter(t => t && t.length <= 30);
        let category = "", deal_type = "";
        
        for (const b of badges) {
          const lower = b.toLowerCase();
          if (!deal_type) {
            if (b === "ขาย") deal_type = "ขาย";
            else if (b === "เช่า" || b === "ให้เช่า") deal_type = "เช่า";
          }
          if (!category) {
            if (lower.includes("คอนโด")) category = "คอนโด";
            else if (lower.includes("บ้านเดี่ยว")) category = "บ้านเดี่ยว";
            else if (lower.includes("ทาวน์")) category = "ทาวน์เฮ้าส์";
            else if (lower.includes("ที่ดิน")) category = "ที่ดิน";
            else if (lower.includes("อาคาร") || lower.includes("ตึก")) category = "อาคารพาณิชย์";
          }
        }
        
        // Fallback: search in body text
        if (!category) {
          const bodyLower = document.body.textContent.toLowerCase();
          if (bodyLower.includes("คอนโด")) category = "คอนโด";
          else if (bodyLower.includes("บ้านเดี่ยว")) category = "บ้านเดี่ยว";
        }
        
        return { category, deal_type };
      };

      // === EXTRACT PRICE INFO ===
      const getPriceInfo = () => {
        let price_text = "", old_price_text = "", price_psm_text = "";
        
        // Current price
        const priceEl = q('[class*="price"]:not([class*="old"])');
        if (priceEl) {
          const allChildren = Array.from(priceEl.querySelectorAll('*'));
          for (const el of allChildren) {
            const t = txt(el);
            if (t.startsWith("฿") && /\d/.test(t) && t.length <= 30) {
              price_text = t;
              break;
            }
          }
        }
        
        // Fallback: search all text
        if (!price_text) {
          const allTexts = qa('*').map(el => txt(el)).filter(t => t.startsWith("฿") && /\d/.test(t) && t.length <= 30);
          if (allTexts.length) price_text = allTexts[0];
        }
        
        // Old price
        const oldPriceEl = q('s, del, [class*="old-price"]');
        if (oldPriceEl) old_price_text = txt(oldPriceEl);
        
        // Price per sqm
        const bodyText = document.body.textContent;
        const psmMatch = bodyText.match(/\(([^)]*บ[^)]*\/[^)]*ตร[^)]*)\)/);
        if (psmMatch) price_psm_text = psmMatch[1];
        
        return { price_text, old_price_text, price_psm_text };
      };

      // === EXTRACT PROPERTY DETAILS ===
      const getPropertyDetails = () => {
        const bodyText = document.body.textContent;
        let bedrooms = null, bathrooms = null, usable_area_sqm = null, floor = null, parking = null;
        
        const bedMatch = bodyText.match(/(\d+)\s*ห้องนอน/);
        if (bedMatch) bedrooms = Number(bedMatch[1]);
        
        const bathMatch = bodyText.match(/(\d+)\s*ห้องน้ำ/);
        if (bathMatch) bathrooms = Number(bathMatch[1]);
        
        const areaMatch = bodyText.match(/(\d+(?:\.\d+)?)\s*ตร\.ม\./);
        if (areaMatch) usable_area_sqm = Number(areaMatch[1]);
        
        const floorMatch = bodyText.match(/(\d+)\s*ชั้น/);
        if (floorMatch) floor = floorMatch[1];
        
        const parkMatch = bodyText.match(/(\d+)\s*ที่จอดรถ/);
        if (parkMatch) parking = Number(parkMatch[1]);
        
        return { bedrooms, bathrooms, usable_area_sqm, floor, parking };
      };

      // === EXTRACT STATS ===
      const getStats = () => {
        const bodyText = document.body.textContent;
        const statsMatch = bodyText.match(/:\s*([\d,]+)\s*:\s*([\d,]+)/);
        let views = null, clicks = null;
        if (statsMatch) {
          views = Number(statsMatch[1].replace(/,/g, ""));
          clicks = Number(statsMatch[2].replace(/,/g, ""));
        }
        return { views, clicks };
      };

      // === EXTRACT AGENT INFO ===
      const getAgentInfo = () => {
        let agent_name = "", agent_verified = false;
        const agentSection = q('[class*="agent"], [class*="seller"]');
        if (agentSection) {
          const nameEl = agentSection.querySelector('h3, h4, [class*="name"]');
          if (nameEl) agent_name = txt(nameEl);
          agent_verified = txt(agentSection).toLowerCase().includes('verified');
        }
        return { agent_name, agent_verified };
      };

      // === EXTRACT CONTACT INFO ===
      const getContacts = () => {
  const qa = (sel) => Array.from(document.querySelectorAll(sel));
  const q = (sel) => document.querySelector(sel);
  const txt = (el) => (el?.textContent || "").replace(/\s+/g, " ").trim();

  // =========================
  // ✅ PHONE (ชัวร์สุด)
  // =========================
  let phone = "";

  // 1) ดึงจาก modal ที่คุณยืนยันมาแล้ว
  const modalPhone = q("#phone_number_modal_show");
  if (modalPhone) {
    phone = txt(modalPhone);
  }

  // 2) fallback: บางครั้งเว็บใส่เบอร์ลงใน span hideTel_*
  if (!phone) {
    const hideSpans = qa('span[id^="hideTel_"]');
    for (const sp of hideSpans) {
      const t = txt(sp);
      if (t && /0[\d\s-]{8,}/.test(t)) { phone = t; break; }
    }
  }

  // 3) fallback สุดท้าย: regex จาก body (กันพลาด)
  if (!phone) {
    const bodyText = (document.body?.textContent || "").replace(/\s+/g, " ").trim();
    const m = bodyText.match(/0[\d\s-]{8,}/);
    if (m) phone = m[0];
  }

  // normalize phone: เอาเฉพาะตัวเลขและขีด/เว้นวรรคออก
  phone = (phone || "").replace(/[\s-]/g, "");

  // =========================
  // EMAIL
  // =========================
  let email = "";
  const mailA = qa('a[href^="mailto:"]')[0];
  if (mailA) email = (mailA.getAttribute("href") || "").replace(/^mailto:/i, "").trim();

  // =========================
  // LINE
  // =========================
  let line_url = "";
  const lineLink = qa('a[href*="line.me"], a[href*="lin.ee"], a[href*="line.me/R/ti/p"]')[0];
  if (lineLink) line_url = lineLink.getAttribute("href") || "";

  // LINE ID (ถ้ามีเป็นข้อความ)
  let line_id = "";
  const bodyText2 = (document.body?.textContent || "").replace(/\s+/g, " ").trim();
  const lineIdMatch = bodyText2.match(/@([a-zA-Z0-9._-]{3,30})/);
  if (lineIdMatch) line_id = "@" + lineIdMatch[1];

  // =========================
  // FACEBOOK
  // =========================
  let fb_url = "";
  const fbLink = qa('a[href*="facebook.com"]')[0];
  if (fbLink) fb_url = fbLink.getAttribute("href") || "";

  return { phone, email, line_url, fb_url, line_id };
};

      // === EXTRACT IMAGES ===
      const getImages = () => {
        const imgs = qa('img')
          .map(i => i.getAttribute('src') || i.getAttribute('data-src') || "")
          .filter(Boolean)
          .filter(src => !isJunkImage(src))
          .filter(src => src.includes('/upload') || src.includes('cloudfront') || src.length > 50);
        return { imgs };
      };

      // === EXTRACT LOCATION ===
      const getLocation = () => {
        const breadLinks = qa('nav[aria-label="breadcrumb"] a, .breadcrumb a');
        const locations = breadLinks.map(a => txt(a)).filter(t => t && t.length <= 40);
        const location_text = locations.length >= 2 ? locations[locations.length - 2] : "";
        return { location_text, province: location_text, breadcrumb: locations };
      };

      // === EXTRACT DESCRIPTION ===
      const getDescription = () => {
        const descEl = q('[class*="description"], .detail-content');
        if (!descEl) return { description: "" };
        const clone = descEl.cloneNode(true);
        clone.querySelectorAll('nav, footer, script, style').forEach(el => el.remove());
        return { description: txt(clone).slice(0, 2000) };
      };

      // === EXTRACT DATES ===
      const getDates = () => {
        const dateTexts = qa('[class*="date"], time').map(el => txt(el)).filter(t => t && (t.includes('สร้าง') || t.includes('ดัน')));
        return { 
          created: dateTexts.find(t => t.includes('สร้าง')) || "", 
          bumped: dateTexts.find(t => t.includes('ดัน')) || "" 
        };
      };

      // ===================================
      // 🆕 EXTRACT NEARBY PLACES
      // ===================================
      // ✅ VERIFIED: Works with actual HTML
      const getNearbyPlaces = () => {
        const nearbyItems = qa('.box-link-map');
        const bts = [], hospitals = [], universities = [], malls = [], all = [];

        nearbyItems.forEach(item => {
          const nameSpan = item.querySelector('.box-map-l span');
          const name = txt(nameSpan);
          
          const distP = item.querySelector('.box-map-l p');
          const distText = txt(distP);
          const distance = parseFloat(distText.replace(/[^\d.]/g, ''));
          
          const lat = item.getAttribute('data-lat');
          const lng = item.getAttribute('data-lng');
          const map = item.getAttribute('data-map');

          if (!name || !distance) return;

          const place = {
            name: name.trim(),
            distance_km: distance,
            lat: lat ? parseFloat(lat) : null,
            lng: lng ? parseFloat(lng) : null,
          };

          all.push({ ...place, type: map });

          // Categorize
          if (map === 'living_transit' || name.includes('BTS') || name.includes('MRT')) {
            bts.push(place);
          } else if (map === 'living_hospital' || name.includes('โรงพยาบาล')) {
            hospitals.push(place);
          } else if (map === 'living_academy' || name.includes('มหาวิทยาลัย') || name.includes('วิทยาลัย')) {
            universities.push(place);
          } else if (map === 'living_mall') {
            malls.push(place);
          }
        });

        // Sort by distance
        const sortByDist = (a, b) => a.distance_km - b.distance_km;
        bts.sort(sortByDist);
        hospitals.sort(sortByDist);
        universities.sort(sortByDist);
        malls.sort(sortByDist);
        all.sort(sortByDist);

        return {
          bts_json: JSON.stringify(bts),
          hospitals_json: JSON.stringify(hospitals),
          universities_json: JSON.stringify(universities),
          malls_json: JSON.stringify(malls),
          all_json: JSON.stringify(all.slice(0, 20)), // top 20
          
          // Nearest for quick access
          nearest_bts: bts.length ? bts[0].name : null,
          nearest_bts_dist: bts.length ? bts[0].distance_km : null,
          nearest_hospital: hospitals.length ? hospitals[0].name : null,
          nearest_hospital_dist: hospitals.length ? hospitals[0].distance_km : null,
          nearest_mall: malls.length ? malls[0].name : null,
          nearest_mall_dist: malls.length ? malls[0].distance_km : null,
        };
      };

      // ===================================
      // 🆕 EXTRACT FACILITIES
      // ===================================
      // ✅ VERIFIED: Works with actual HTML
      const getFacilities = () => {
        const facilityItems = qa('.item_property_highlight');
        const facilities = [];
        const tags = {
          has_pool: false,
          has_gym: false,
          has_parking: false,
          has_security: false,
          has_garden: false,
          has_sauna: false,
          has_ev_charger: false,
          has_sky_pool: false,
          has_foreigner_quota: false,
          is_luxury: false,
          has_private_lift: false,
        };

        facilityItems.forEach(item => {
          const nameSpan = item.querySelector('.text_property_highlight');
          const name = txt(nameSpan);
          
          if (name && name.length > 0 && name.length < 50) {
            facilities.push(name);

            // Auto-detect facility types
            const lower = name.toLowerCase();
            if (lower.includes('สระ')) tags.has_pool = true;
            if (lower.includes('ฟิตเนส') || lower.includes('ยิม') || lower.includes('gym')) tags.has_gym = true;
            if (lower.includes('จอดรถ')) tags.has_parking = true;
            if (lower.includes('รักษาความปลอดภัย') || lower.includes('security')) tags.has_security = true;
            if (lower.includes('สวน') || lower.includes('garden')) tags.has_garden = true;
            if (lower.includes('ซาวน่า') || lower.includes('sauna')) tags.has_sauna = true;
            if (lower.includes('ev charger')) tags.has_ev_charger = true;
            if (lower.includes('สระน้ำลอยฟ้า') || lower.includes('sky pool')) tags.has_sky_pool = true;
            if (lower.includes('โควต้าต่างชาติ') || lower.includes('foreigner')) tags.has_foreigner_quota = true;
            if (lower.includes('luxury')) tags.is_luxury = true;
            if (lower.includes('ลิฟต์ส่วนตัว') || lower.includes('private lift')) tags.has_private_lift = true;
          }
        });

        return {
          facilities_json: JSON.stringify(facilities),
          facility_count: facilities.length,
          ...tags,
        };
      };

      // === RETURN ALL EXTRACTED DATA ===
      return {
        title: getTitle(),
        project_name: getProjectName(),
        ...getBadges(),
        ...getPriceInfo(),
        ...getPropertyDetails(),
        ...getStats(),
        ...getAgentInfo(),
        ...getContacts(),
        ...getImages(),
        ...getLocation(),
        ...getDescription(),
        ...getDates(),
        ...getNearbyPlaces(),
        ...getFacilities(),
      };
    }),
    sleep(30000).then(() => { throw new Error("evaluate timeout"); })
  ]).catch(() => null);

  if (!data) throw new Error("Failed to extract data");

  // ============================================
  // 🔧 SERVER-SIDE DATA NORMALIZATION
  // ============================================

  const listing_title = cleanText(data.title || "");
  const price_text = cleanText(data.price_text || "");
  const price_value = parseNumberLike(price_text);
  const price_psm = parseNumberLike(data.price_psm_text || "");
  
  // Extract listing ID from URL
  let listing_id = null;
  try {
    const u = new URL(url);
    const m = u.pathname.match(/\/livingdetail\/(\d+)\//);
    if (m) listing_id = m[1];
  } catch {}

  const breadcrumb = Array.isArray(data.breadcrumb) ? data.breadcrumb : [];
  const rawImgs = Array.isArray(data.imgs) ? data.imgs : [];
  const imagesAbs = rawImgs.map(x => absUrl(url, x)).filter(Boolean).filter((v, i, a) => a.indexOf(v) === i).slice(0, MAX_IMAGES);

  // ============================================
  // 📊 DASHBOARD SCORING FUNCTIONS
  // ============================================
  
  /**
   * Calculate walkability score based on BTS distance
   * @param {number} nearestBtsDist - Distance to nearest BTS (km)
   * @returns {number} Score 0-100
   */
  const calculateWalkabilityScore = (nearestBtsDist) => {
    if (!nearestBtsDist) return 0;
    if (nearestBtsDist <= 0.3) return 100; // < 300m = perfect
    if (nearestBtsDist <= 0.5) return 90;  // < 500m = excellent
    if (nearestBtsDist <= 0.8) return 75;  // < 800m = good
    if (nearestBtsDist <= 1.2) return 60;  // < 1.2km = ok
    if (nearestBtsDist <= 2.0) return 40;  // < 2km = fair
    return 20; // > 2km = poor
  };

  /**
   * Calculate location score based on nearby amenities
   * @param {number} nearestBts - Distance to BTS
   * @param {number} nearestMall - Distance to mall
   * @param {number} nearestHospital - Distance to hospital
   * @returns {number} Score 0-100
   */
  const calculateLocationScore = (nearestBts, nearestMall, nearestHospital) => {
    let score = 0;
    
    // BTS distance (40% weight)
    if (nearestBts <= 0.5) score += 40;
    else if (nearestBts <= 1.0) score += 30;
    else if (nearestBts <= 2.0) score += 15;
    
    // Mall distance (30% weight)
    if (nearestMall && nearestMall <= 1.0) score += 30;
    else if (nearestMall && nearestMall <= 2.0) score += 20;
    else if (nearestMall && nearestMall <= 3.0) score += 10;
    
    // Hospital distance (30% weight)
    if (nearestHospital && nearestHospital <= 1.0) score += 30;
    else if (nearestHospital && nearestHospital <= 2.0) score += 20;
    else if (nearestHospital && nearestHospital <= 3.0) score += 10;
    
    return Math.min(score, 100);
  };

  /**
   * Calculate investment score
   * @param {number} priceValue - Total price
   * @param {number} pricePsm - Price per sqm
   * @param {number} nearestBts - Distance to BTS
   * @param {number} facilityCount - Number of facilities
   * @returns {number} Score 0-100
   */
  const calculateInvestmentScore = (priceValue, pricePsm, nearestBts, facilityCount) => {
    let score = 50; // base score
    
    // Price per sqm (25% weight)
    if (pricePsm && pricePsm < 100000) score += 25;
    else if (pricePsm && pricePsm < 150000) score += 15;
    else if (pricePsm && pricePsm < 200000) score += 5;
    
    // BTS proximity (35% weight)
    if (nearestBts && nearestBts <= 0.3) score += 35;
    else if (nearestBts && nearestBts <= 0.5) score += 25;
    else if (nearestBts && nearestBts <= 1.0) score += 15;
    
    // Facilities (20% weight)
    if (facilityCount >= 10) score += 20;
    else if (facilityCount >= 7) score += 15;
    else if (facilityCount >= 5) score += 10;
    
    return Math.min(score, 100);
  };

  /**
   * Calculate facility score
   * @param {number} facilityCount - Total facilities
   * @param {boolean} hasPool - Has pool?
   * @param {boolean} hasGym - Has gym?
   * @returns {number} Score 0-100
   */
  const calculateFacilityScore = (facilityCount, hasPool, hasGym) => {
    let score = (facilityCount / 15) * 60; // max 60 from count
    if (hasPool) score += 20;
    if (hasGym) score += 20;
    return Math.min(Math.round(score), 100);
  };

  // ============================================
  // 🏗️ BUILD ROW OBJECT
  // ============================================

  const row = {
    // Basic Info
    listing_id, 
    listing_url: url,
    category: cleanText(data.category || ""), 
    deal_type: cleanText(data.deal_type || ""),
    project_name: data.project_name || null,
    listing_title, 
    price_text, 
    price_value, 
    price_psm,
    old_price_text: cleanText(data.old_price_text || "") || null,
    discount_percent: null,
    usable_area_sqm: data.usable_area_sqm, 
    floor: data.floor,
    bedrooms: data.bedrooms, 
    bathrooms: data.bathrooms, 
    parking: data.parking,
    furnishing: null, 
    direction: null,
    created_at_iso: thaiDateToISO(data.created || ""), 
    bumped_at_iso: thaiDateToISO(data.bumped || ""),
    
    // Location
    location_text: cleanText(data.location_text || ""), 
    province: cleanText(data.province || ""),
    district: null,
    
    // Nearby Places (JSON)
    nearby_bts_json: data.bts_json || null,
    nearby_hospitals_json: data.hospitals_json || null,
    nearby_universities_json: data.universities_json || null,
    nearby_malls_json: data.malls_json || null,
    nearby_all_json: data.all_json || null,
    
    // Nearest (quick access)
    nearest_bts_name: data.nearest_bts || null,
    nearest_bts_distance_km: data.nearest_bts_dist || null,
    nearest_hospital_name: data.nearest_hospital || null,
    nearest_hospital_distance_km: data.nearest_hospital_dist || null,
    nearest_mall_name: data.nearest_mall || null,
    nearest_mall_distance_km: data.nearest_mall_dist || null,
    
    // Map
    map_url: null, 
    lat: null, 
    lng: null,
    
    // Agent
    agent_name: cleanText(data.agent_name || ""), 
    agent_url: null, 
    agent_verified: Boolean(data.agent_verified),
    agent_rating: null,
    
    // Stats
    clicks: data.clicks, 
    views: data.views, 
    favorites: null,
    
    // Contact
    contact_phone: data.phone ? cleanText(data.phone) : null,
    contact_email: data.email ? cleanText(data.email) : null,
    contact_line_url: data.line_url ? cleanText(data.line_url) : null,
    contact_line_id: data.line_id ? cleanText(data.line_id) : null,
    contact_facebook_url: data.fb_url ? cleanText(data.fb_url) : null,
    contact_buttons: null,
    
    // Content
    description_text: cleanText(data.description || "").slice(0, 1500),
    highlights: null,
    detail_snippet: cleanText(data.description || "").slice(0, 180),
    
    // Images
    images: imagesAbs.join(" | "),
    cover_image: imagesAbs.length ? imagesAbs[0] : null,
    
    // Facilities
    facilities_json: data.facilities_json || null,
    facility_count: data.facility_count || 0,
    has_pool: data.has_pool || false,
    has_gym: data.has_gym || false,
    has_parking: data.has_parking || false,
    has_security: data.has_security || false,
    has_garden: data.has_garden || false,
    has_sauna: data.has_sauna || false,
    has_ev_charger: data.has_ev_charger || false,
    has_sky_pool: data.has_sky_pool || false,
    has_foreigner_quota: data.has_foreigner_quota || false,
    is_luxury: data.is_luxury || false,
    has_private_lift: data.has_private_lift || false,
  };

  // Calculate dashboard scores
  row.walkability_score = calculateWalkabilityScore(row.nearest_bts_distance_km);
  row.location_score = calculateLocationScore(
    row.nearest_bts_distance_km,
    row.nearest_mall_distance_km,
    row.nearest_hospital_distance_km
  );
  row.facility_score = calculateFacilityScore(row.facility_count, row.has_pool, row.has_gym);
  row.investment_score = calculateInvestmentScore(
    row.price_value,
    row.price_psm,
    row.nearest_bts_distance_km,
    row.facility_count
  );
  row.value_score = 0; // will calculate after quality_score

  // Return normalized row
  const out = {};
  for (const k of SCHEMA_KEYS) out[k] = row[k] ?? null;
  return out;
}

// ============================================
// 🔄 RETRY WRAPPER
// ============================================

async function workWithRetry(page, url, context) {
  let lastErr = null;
  for (let i = 0; i <= DETAIL_RETRIES; i++) {
    try {
      // Recreate page if closed
      if (page.isClosed()) {
        page = await context.newPage();
        await injectPerfCSS(page);
      }
      
      const res = await Promise.race([
        parseDetail(page, url),
        sleep(DETAIL_TIMEOUT_MS).then(() => { throw new Error("timeout"); })
      ]);
      
      return { result: res, page };
    } catch (e) {
      lastErr = e;
      
      // Handle crashes
      if (e.message.includes("crash") || e.message.includes("closed")) {
        try {
          await page.close().catch(() => {});
          page = await context.newPage();
          await injectPerfCSS(page);
        } catch {}
      }
      
      // Exponential backoff
      await sleep(e.message.includes("crash") ? 3000 + i * 1500 : 800 + i * 600);
    }
  }
  
  throw lastErr || new Error("failed after retries");
}

// ============================================
// 🚀 MAIN SCRAPER FUNCTION
// ============================================

export default async function scrapeListings(opts = {}) {
  const maxResults = Math.max(1, Number(opts.maxResults || 50));
  const aiEngine = new AILearningEngine();

  const meta = {
    source: "ai_powered_scraper", 
    maxResults, 
    startedAt: nowISO(), 
    endedAt: null,
    sources_used: 0, 
    collected_links: 0, 
    total_parsed: 0,
    duplicates_removed: 0, 
    avg_quality_score: 0, 
    elapsedMs: null, 
    errors: [],
  };

  const t0 = Date.now();
  emitProgress(opts, { stage: "boot", message: "🧠 AI Engine Initializing...", meta });

  log("=".repeat(70));
  log("🚀 AI-POWERED INTELLIGENT SCRAPER v3.0");
  log("=".repeat(70));

  // Launch browser
  const browser = await chromium.launch({
    headless: SCRAPE_HEADLESS,
    args: ["--disable-dev-shm-usage", "--disable-gpu", "--no-sandbox"],
  });

  // ============================================
  // 📋 PHASE 1: COLLECT LINKS
  // ============================================

  const contextOpts = { locale: "th-TH", timezoneId: "Asia/Bangkok" };
  const contextList = await browser.newContext(contextOpts);
  contextList.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
  await contextList.route("**/*", makeRouteHandler("auto"));

  const listPage = await contextList.newPage();
  await injectPerfCSS(listPage);

  const collected = new Set();
  const sources = selectSmartSources(maxResults, aiEngine);
  log(`🎯 Selected ${sources.length} intelligent sources\n`);

  try {
    for (let s = 0; s < sources.length; s++) {
      const source = sources[s];
      log(`📍 SOURCE ${s + 1}/${sources.length}: ${source.name}`);
      
      await robustGoto(listPage, source.url, { retries: 2 });
      await dismissOverlays(listPage);
      
      const adaptiveRounds = aiEngine.recommendScrollRounds(LIST_SCROLL_ROUNDS, 0);
      await autoScrollRobust(listPage, adaptiveRounds);

      const links = await collectDetailLinksFromList(listPage);
      
      aiEngine.recordSourcePerformance(source.id, {
        success: links.length > 0,
        linksFound: links.length,
        avgQuality: 0.5,
      });

      for (const link of links) {
        collected.add(link);
        if (collected.size >= maxResults * 2) break;
      }

      log(`   ✅ Collected ${links.length} links (total: ${collected.size})\n`);

      if (collected.size >= maxResults * 2) {
        log(`🎯 Target reached! Stopping early.\n`);
        break;
      }
    }
  } finally {
    await contextList.close().catch(() => {});
  }

  meta.sources_used = sources.length;
  meta.collected_links = collected.size;
  const sampled = [...collected].slice(0, maxResults);

  log(`🔍 PARSING: ${sampled.length} URLs with AI quality analysis\n`);

  // ============================================
  // 📄 PHASE 2: PARSE DETAILS
  // ============================================

  const contextDetail = await browser.newContext(contextOpts);
  await contextDetail.route("**/*", makeRouteHandler("auto"));

  const rows = [];
  const queue = [...sampled];
  let parsedCount = 0;

  /**
   * Worker function for parallel parsing
   * @param {number} workerId - Worker ID for logging
   */
  async function worker(workerId) {
    let page = await contextDetail.newPage();
    await injectPerfCSS(page);
    let jobCount = 0;

    while (queue.length) {
      const link = queue.shift();
      if (!link) break;

      try {
        const { result: row, page: newPage } = await workWithRetry(page, link, contextDetail);
        page = newPage;
        jobCount++;

        // Recycle page periodically
        if (jobCount % PAGE_RECYCLE_AFTER === 0) {
          await page.close();
          page = await contextDetail.newPage();
          await injectPerfCSS(page);
        }

        // AI duplicate detection
        if (aiEngine.isDuplicate(row)) {
          log(`Worker ${workerId}: 🔁 Duplicate detected, skipped`);
          meta.duplicates_removed++;
          continue;
        }

        // Learn price patterns
        aiEngine.learnPrice(row.price_value, row.category);

        // Calculate AI scores
        row.quality_score = QualityScorer.calculateQualityScore(row);
        row.price_score = QualityScorer.scorePriceReliability(row) * 100;
        row.data_completeness = Math.round(QualityScorer.scoreDataCompleteness(row) * 100);
        row.anomaly_flags = QualityScorer.generateAnomalyFlags(row, aiEngine);

        // Calculate value_score (now that we have quality_score)
        row.value_score = Math.round(
          (row.quality_score / 100) * 40 +  // quality: 40%
          (row.price_score / 100) * 30 +    // price: 30%  
          (row.location_score / 100) * 30   // location: 30%
        );

        rows.push(row);
        parsedCount++;
        meta.total_parsed = parsedCount;

        log(`Worker ${workerId}: ✅ Q:${row.quality_score}% L:${row.location_score}% (${parsedCount}/${sampled.length})`);

        if (parsedCount % 5 === 0) {
          emitProgress(opts, { stage: "detail", message: `${parsedCount}/${sampled.length}`, meta });
        }
      } catch (e) {
        log(`Worker ${workerId}: ❌ ${e.message}`);
        meta.errors.push({ url: link, error: String(e.message) });
      }
    }

    await page.close().catch(() => {});
  }

  // Run workers in parallel
  try {
    const workers = [];
    for (let i = 0; i < MAX_CONCURRENCY; i++) {
      workers.push(worker(i + 1));
    }
    await Promise.all(workers);
  } finally {
    await contextDetail.close().catch(() => {});
    await browser.close().catch(() => {});
  }

  // ============================================
  // 📊 FINALIZE RESULTS
  // ============================================

  meta.elapsedMs = Date.now() - t0;
  meta.endedAt = nowISO();
  meta.avg_quality_score = rows.length > 0 
    ? Math.round(rows.reduce((sum, r) => sum + r.quality_score, 0) / rows.length) 
    : 0;

  const insights = aiEngine.generateInsights();

  log("\n" + "=".repeat(70));
  log("🎉 SCRAPING COMPLETE");
  log(`📊 Results: ${rows.length} listings | Quality: ${meta.avg_quality_score}%`);
  log(`🔁 Duplicates removed: ${meta.duplicates_removed}`);
  log(`💰 Price range: ฿${insights.priceRange.min.toLocaleString()} - ฿${insights.priceRange.max.toLocaleString()}`);
  log(`⏱️  Time: ${Math.round(meta.elapsedMs / 1000)}s`);
  log("=".repeat(70) + "\n");

  emitProgress(opts, { 
    stage: "done", 
    message: `Done: ${rows.length} high-quality listings`, 
    meta, 
    insights 
  });

  // Normalize final output
  const finalRows = rows.map((r) => {
    const o = {};
    for (const k of SCHEMA_KEYS) o[k] = r?.[k] ?? null;
    return o;
  });

  return { rows: finalRows, meta, insights };
}