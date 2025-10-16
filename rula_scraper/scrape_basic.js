import { firefox } from 'playwright';
import { MongoClient } from 'mongodb';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// MongoDB configuration
const MONGO_HOST = process.env.MONGO_HOST || 'localhost';
const MONGO_PORT = process.env.MONGO_PORT || 27017;
const MONGO_DB = process.env.MONGO_DB || 'rula_scraper_final';
const MONGO_USER = process.env.MONGO_USER || 'scraper';
const MONGO_PASSWORD = process.env.MONGO_PASSWORD || 'scraper';
const MONGO_URI = `mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${MONGO_DB}?authSource=admin`;

const STATES = [
    "https://www.rula.com/therapists/al", "https://www.rula.com/therapists/ak", "https://www.rula.com/therapists/az",
    "https://www.rula.com/therapists/ar", "https://www.rula.com/therapists/ca", "https://www.rula.com/therapists/co",
    "https://www.rula.com/therapists/ct", "https://www.rula.com/therapists/de", "https://www.rula.com/therapists/dc",
    "https://www.rula.com/therapists/fl", "https://www.rula.com/therapists/ga", "https://www.rula.com/therapists/hi",
    "https://www.rula.com/therapists/id", "https://www.rula.com/therapists/il", "https://www.rula.com/therapists/in",
    "https://www.rula.com/therapists/ia", "https://www.rula.com/therapists/ks", "https://www.rula.com/therapists/ky",
    "https://www.rula.com/therapists/la", "https://www.rula.com/therapists/me", "https://www.rula.com/therapists/md",
    "https://www.rula.com/therapists/ma", "https://www.rula.com/therapists/mi", "https://www.rula.com/therapists/mn",
    "https://www.rula.com/therapists/ms", "https://www.rula.com/therapists/mo", "https://www.rula.com/therapists/mt",
    "https://www.rula.com/therapists/ne", "https://www.rula.com/therapists/nv", "https://www.rula.com/therapists/nh",
    "https://www.rula.com/therapists/nj", "https://www.rula.com/therapists/nm", "https://www.rula.com/therapists/ny",
    "https://www.rula.com/therapists/nc", "https://www.rula.com/therapists/nd", "https://www.rula.com/therapists/oh",
    "https://www.rula.com/therapists/ok", "https://www.rula.com/therapists/or", "https://www.rula.com/therapists/pa",
    "https://www.rula.com/therapists/ri", "https://www.rula.com/therapists/sc", "https://www.rula.com/therapists/sd",
    "https://www.rula.com/therapists/tn", "https://www.rula.com/therapists/tx", "https://www.rula.com/therapists/ut",
    "https://www.rula.com/therapists/vt", "https://www.rula.com/therapists/va", "https://www.rula.com/therapists/wa",
    "https://www.rula.com/therapists/wv", "https://www.rula.com/therapists/wi", "https://www.rula.com/therapists/wy"
];

const CONFIG = {
    headless: process.env.HEADLESS !== 'false',
    timeout: 120000,
    navigationTimeout: 60000,
    scrapeLimit: parseInt(process.env.SCRAPE_LIMIT) || 0
};

let client;
let db;

async function connectToMongo() {
    try {
        client = new MongoClient(MONGO_URI);
        await client.connect();
        db = client.db(MONGO_DB);
        console.log('✅ Connected to MongoDB');

        // Create indexes
        await db.collection('providers').createIndex({ profile_url: 1 }, { unique: true });
        await db.collection('providers').createIndex({ state: 1 });
        await db.collection('providers').createIndex({ scraped_at: 1 });
        await db.collection('scraping_logs').createIndex({ timestamp: 1 });

    } catch (error) {
        console.error('❌ MongoDB connection failed:', error);
        process.exit(1);
    }
}

async function logScrapingEvent(eventType, state, message, error = null) {
    const logEntry = {
        event_type: eventType,
        state: state,
        message: message,
        error: error,
        timestamp: new Date()
    };

    try {
        await db.collection('scraping_logs').insertOne(logEntry);
    } catch (err) {
        console.error('Failed to log event:', err);
    }

    console.log(`[${eventType.toUpperCase()}] ${state}: ${message}`);
    if (error) console.error(error);
}

async function ensureDirectory(dir) {
    try {
        await fs.mkdir(dir, { recursive: true });
    } catch (error) {
        // Directory might already exist
    }
}

(async () => {
    await connectToMongo();

    const browser = await firefox.launch({
        headless: CONFIG.headless,
        timeout: CONFIG.timeout
    });

    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        viewport: { width: 1280, height: 800 }
    });

    const page = await context.newPage();

    let totalProviders = 0;

    for (const [index, url] of STATES.entries()) {
        const stateSlug = new URL(url).pathname.split('/').pop();

        try {
            await logScrapingEvent('state_start', stateSlug, `Processing state (${index + 1}/${STATES.length})`);

            await page.goto(url, {
                waitUntil: 'domcontentloaded',
                timeout: CONFIG.navigationTimeout
            });

            await page.waitForSelector('div._cardContent_1w33m_76', { timeout: 60000 });
            await page.waitForTimeout(2000);

            // Click "Show more" until all providers are loaded
            let loadMoreAttempts = 0;
            const maxLoadMoreAttempts = 50;

            while (loadMoreAttempts < maxLoadMoreAttempts) {
                const loadMoreButton = await page.$('button[aria-label="View next providers"]');
                if (!loadMoreButton) break;

                await loadMoreButton.click();
                await page.waitForTimeout(2000);
                loadMoreAttempts++;

                const buttonState = await loadMoreButton.evaluate(btn => {
                    return {
                        disabled: btn.disabled,
                        visible: btn.offsetParent !== null
                    };
                });

                if (buttonState.disabled || !buttonState.visible) break;
            }

            // Extract providers data
            const providers = await page.$$eval('div._cardContent_1w33m_76', cards =>
                cards.map(card => {
                    const nameEl = card.querySelector('h2 a');
                    const bioEl = card.querySelector('p[data-testid="card-bio"]');
                    const locationEl = card.querySelector('div._cityState_14r5r_26 span:nth-child(2)');
                    const nextApptEl = card.querySelector('div._availability_1w33m_276 span:nth-child(2)');
                    const badgesEls = Array.from(card.querySelectorAll('button[data-testid="badge"] div'));
                    const virtualEl = card.querySelector('button[data-testid="virtual-icon"]');

                    return {
                        name: nameEl?.innerText.trim() || null,
                        profile_url: nameEl?.href || null,
                        image: card.querySelector('img')?.src || null,
                        city: locationEl?.innerText.trim() || null,
                        bio: bioEl?.innerText.trim() || null,
                        next_appointment: nextApptEl?.innerText.trim() || null,
                        badges: badgesEls.map(b => b.innerText.trim()),
                        virtual: !!virtualEl,
                        state: stateSlug,
                        scraped_at: new Date(),
                        detailed_scraped: false,
                        last_scraped: null
                    };
                }).filter(provider => provider.profile_url && provider.name)
            );

            // Apply limit if specified
            const providersToSave = CONFIG.scrapeLimit > 0
                ? providers.slice(0, CONFIG.scrapeLimit)
                : providers;

            // Save to MongoDB
            for (const provider of providersToSave) {
                try {
                    await db.collection('providers').updateOne(
                        { profile_url: provider.profile_url },
                        {
                            $set: {
                                ...provider,
                                last_updated: new Date()
                            },
                            $setOnInsert: {
                                created_at: new Date()
                            }
                        },
                        { upsert: true }
                    );
                } catch (error) {
                    if (error.code === 11000) {
                        // Duplicate key, just update
                        await db.collection('providers').updateOne(
                            { profile_url: provider.profile_url },
                            {
                                $set: {
                                    ...provider,
                                    last_updated: new Date()
                                }
                            }
                        );
                    } else {
                        throw error;
                    }
                }
            }

            totalProviders += providersToSave.length;
            await logScrapingEvent('state_complete', stateSlug, `Saved ${providersToSave.length} providers`);

        } catch (error) {
            await logScrapingEvent('state_error', stateSlug, `Error processing state: ${error.message}`, error);
        }

        // Small delay between states
        await page.waitForTimeout(1000);
    }

    await browser.close();
    await logScrapingEvent('scraping_complete', 'all', `Total providers scraped: ${totalProviders}`);

    await client.close();
    console.log('✅ Basic scraping completed!');
})();
