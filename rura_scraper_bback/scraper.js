// scraper.js
import { firefox } from 'playwright';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const states = [
    "https://www.rula.com/therapists/al",
    "https://www.rula.com/therapists/ak",
    "https://www.rula.com/therapists/az",
    "https://www.rula.com/therapists/ar",
    "https://www.rula.com/therapists/ca",
    "https://www.rula.com/therapists/co",
    "https://www.rula.com/therapists/ct",
    "https://www.rula.com/therapists/de",
    "https://www.rula.com/therapists/dc",
    "https://www.rula.com/therapists/fl",
    "https://www.rula.com/therapists/ga",
    "https://www.rula.com/therapists/hi",
    "https://www.rula.com/therapists/id",
    "https://www.rula.com/therapists/il",
    "https://www.rula.com/therapists/in",
    "https://www.rula.com/therapists/ia",
    "https://www.rula.com/therapists/ks",
    "https://www.rula.com/therapists/ky",
    "https://www.rula.com/therapists/la",
    "https://www.rula.com/therapists/me",
    "https://www.rula.com/therapists/md",
    "https://www.rula.com/therapists/ma",
    "https://www.rula.com/therapists/mi",
    "https://www.rula.com/therapists/mn",
    "https://www.rula.com/therapists/ms",
    "https://www.rula.com/therapists/mo",
    "https://www.rula.com/therapists/mt",
    "https://www.rula.com/therapists/ne",
    "https://www.rula.com/therapists/nv",
    "https://www.rula.com/therapists/nh",
    "https://www.rula.com/therapists/nj",
    "https://www.rula.com/therapists/nm",
    "https://www.rula.com/therapists/ny",
    "https://www.rula.com/therapists/nc",
    "https://www.rula.com/therapists/nd",
    "https://www.rula.com/therapists/oh",
    "https://www.rula.com/therapists/ok",
    "https://www.rula.com/therapists/or",
    "https://www.rula.com/therapists/pa",
    "https://www.rula.com/therapists/ri",
    "https://www.rula.com/therapists/sc",
    "https://www.rula.com/therapists/sd",
    "https://www.rula.com/therapists/tn",
    "https://www.rula.com/therapists/tx",
    "https://www.rula.com/therapists/ut",
    "https://www.rula.com/therapists/vt",
    "https://www.rula.com/therapists/va",
    "https://www.rula.com/therapists/wa",
    "https://www.rula.com/therapists/wv",
    "https://www.rula.com/therapists/wi",
    "https://www.rula.com/therapists/wy"
];

(async () => {
    await fs.mkdir(path.join(__dirname, 'data'), { recursive: true });
    const browser = await firefox.launch({ headless: false });
    const context = await browser.newContext({ userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)' });
    const page = await context.newPage();

    for (const [index, url] of states.entries()) {
        console.log(`\n[${index + 1}/${states.length}] Visiting: ${url}`);

        try {
            console.log(`Navigating to page...`);
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });
            console.log(`Waiting for first provider card...`);
            await page.waitForSelector('div._cardContent_1w33m_76', { timeout: 60000 });
            await page.waitForTimeout(1500);

            // Click "Show more" until all providers are loaded
            while (await page.$('button[aria-label="View next providers"]')) {
                console.log('Clicking "Show more"...');
                await page.click('button[aria-label="View next providers"]');
                await page.waitForTimeout(1500);
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
                        virtual: !!virtualEl
                    };
                })
            );

            // Save JSON to file
            const stateSlug = new URL(url).pathname.split('/').filter(Boolean).pop();
            const jsonPath = path.join(__dirname, 'data', `${stateSlug}.json`);
            await fs.writeFile(jsonPath, JSON.stringify(providers, null, 2), 'utf8');
            console.log(`Saved JSON data to ${jsonPath}`);
        } catch (err) {
            console.error(`Error processing ${url}:`, err.message);
        }
    }

    await browser.close();
})();
