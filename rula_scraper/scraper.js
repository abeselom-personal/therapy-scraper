// combined_scraper.js
import { firefox } from 'playwright';
import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import ExcelJS from 'exceljs';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import os from 'os';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// State URLs for initial scraping
const states = [
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

// State mapping for NPI lookup
const stateMap = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "district of columbia": "DC",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID", "illinois": "IL",
    "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA",
    "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY"
};

// Configuration
const CONFIG = {
    maxRetries: 3,
    retryDelay: 1000,
    timeout: 60000,
    navigationTimeout: 30000,
    selectorTimeout: 15000,
    headless: true,
    workers: Math.min(os.cpus().length - 1, 8),
    batchSize: 10
};

// Logging setup
const LOG_FILE = './scraping_log.txt';
const ERROR_LOG_FILE = './scraping_errors.json';

function logMessage(message, type = 'INFO') {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] [${type}] ${message}\n`;
    console.log(logEntry.trim());
    fsSync.appendFileSync(LOG_FILE, logEntry);
}

function logError(providerName, error, context = '') {
    const errorEntry = {
        timestamp: new Date().toISOString(),
        provider: providerName,
        error: error.message,
        stack: error.stack,
        context: context
    };
    logMessage(`Error for ${providerName}: ${error.message} ${context ? `[${context}]` : ''}`, 'ERROR');
    let errors = [];
    if (fsSync.existsSync(ERROR_LOG_FILE)) {
        try {
            errors = JSON.parse(fsSync.readFileSync(ERROR_LOG_FILE, 'utf-8'));
        } catch (e) {
            logMessage('Could not read error log file', 'WARN');
        }
    }
    errors.push(errorEntry);
    fsSync.writeFileSync(ERROR_LOG_FILE, JSON.stringify(errors, null, 2));
}

async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function retryOperation(operation, operationName, maxRetries = CONFIG.maxRetries) {
    let lastError;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            if (attempt > 1) {
                logMessage(`Retry attempt ${attempt} for ${operationName}`, 'RETRY');
                await delay(CONFIG.retryDelay * Math.pow(1.5, attempt - 1));
            }
            return await operation();
        } catch (error) {
            lastError = error;
            logMessage(`Attempt ${attempt} failed for ${operationName}: ${error.message}`, 'RETRY');
            if (attempt === maxRetries) {
                throw lastError;
            }
        }
    }
}

// NPI Lookup Function
async function fetchNPI(name, state, retries = 3) {
    if (!name || !state) return '';

    const stateCode = stateMap[state.toLowerCase()] || state.toUpperCase();
    if (!stateCode) return '';

    const nameParts = name.trim().split(/\s+/);
    const firstName = nameParts[0] || '';
    const lastName = nameParts.slice(1).join(' ') || '';

    if (!firstName || !lastName) return '';

    const url = `https://npiregistry.cms.hhs.gov/api/?version=2.1&first_name=${encodeURIComponent(firstName)}&last_name=${encodeURIComponent(lastName)}&state=${stateCode}&limit=10`;

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 10000);
            const response = await fetch(url, { signal: controller.signal });
            clearTimeout(timeout);

            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();

            if (data.results && data.results.length > 0) {
                return data.results[0].number || '';
            }
            return '';
        } catch (err) {
            logMessage(`[NPI ERROR] Attempt ${attempt} for ${name} in ${state}: ${err.message}`, 'WARN');
            if (attempt === retries) return '';
            await delay(1000 * attempt);
        }
    }
    return '';
}

// Step 1: Parallel State Scraping - FIXED VERSION
async function scrapeStatesParallel() {
    logMessage('Starting parallel state scraping...');
    await fs.mkdir(path.join(__dirname, 'data'), { recursive: true });

    const browser = await firefox.launch({ headless: CONFIG.headless });
    const results = [];

    const scrapeState = async (url, index) => {
        const stateSlug = url.split('/').pop();
        let success = false;
        let attempt = 0;

        while (!success && attempt < 3) {
            attempt++;
            const context = await browser.newContext({
                userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            });
            const page = await context.newPage();

            try {
                logMessage(`[${index + 1}/${states.length}] Attempt ${attempt}: ${url}`);
                await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });
                await page.waitForSelector('div._cardContent_1w33m_76', { timeout: 60000 });

                // Load all providers by clicking "View next providers"
                let loadMore = true;
                let clickAttempts = 0;
                const maxClickAttempts = 50; // Safety limit

                while (loadMore && clickAttempts < maxClickAttempts) {
                    try {
                        const loadMoreButton = await page.$('button[aria-label="View next providers"]');
                        if (!loadMoreButton) {
                            loadMore = false;
                            break;
                        }

                        const isDisabled = await loadMoreButton.evaluate(btn => btn.disabled);
                        if (isDisabled) {
                            loadMore = false;
                            break;
                        }

                        await loadMoreButton.click();
                        await delay(1500);
                        clickAttempts++;

                        // Verify the button is still there and not disabled
                        const buttonStillExists = await page.$('button[aria-label="View next providers"]');
                        if (!buttonStillExists) {
                            loadMore = false;
                            break;
                        }

                    } catch (e) {
                        logMessage(`Load more click failed on attempt ${clickAttempts}: ${e.message}`, 'WARN');
                        loadMore = false;
                        break;
                    }
                }

                // Extract provider data - FIXED: Pass url as parameter to evaluate function
                const providers = await page.$$eval('div._cardContent_1w33m_76', (cards, pageUrl) =>
                    cards.map(c => {
                        const nameLink = c.querySelector('h2 a');
                        const locationElement = c.querySelector('[class*="location"]');

                        return {
                            name: nameLink?.innerText.trim() || null,
                            profile_url: nameLink?.href || null,
                            city: locationElement?.innerText.trim() || '',
                            state: pageUrl.split('/').pop().toUpperCase()
                        };
                    }), url); // Pass url as parameter to the evaluate function

                await fs.writeFile(
                    path.join(__dirname, 'data', `${stateSlug}.json`),
                    JSON.stringify(providers, null, 2)
                );

                success = true;
                logMessage(`✓ Saved ${stateSlug}.json with ${providers.length} providers`);
                return { state: stateSlug, providers: providers.length, success: true };

            } catch (e) {
                logMessage(`Attempt ${attempt} failed for ${url}: ${e.message}`, 'ERROR');
            } finally {
                await page.close();
                await context.close();
            }
        }
        return { state: stateSlug, providers: 0, success: false };
    };

    // Run state scraping in parallel with limited concurrency
    const concurrentScrapes = 3;
    for (let i = 0; i < states.length; i += concurrentScrapes) {
        const batch = states.slice(i, i + concurrentScrapes);
        const batchPromises = batch.map((url, index) => scrapeState(url, i + index));
        const batchResults = await Promise.allSettled(batchPromises);

        batchResults.forEach((result, idx) => {
            if (result.status === 'fulfilled') {
                results.push(result.value);
            } else {
                logMessage(`Failed to scrape batch ${i + idx}: ${result.reason}`, 'ERROR');
            }
        });

        await delay(2000); // Brief pause between batches
    }

    await browser.close();

    const successful = results.filter(r => r.success).length;
    const totalProviders = results.reduce((sum, r) => sum + r.providers, 0);

    logMessage(`State scraping completed: ${successful}/${states.length} states, ${totalProviders} total providers`);
    return results;
}

// Step 2: Collect All Providers
async function collectAllProviders() {
    logMessage('Collecting all providers from state files...');
    const dataDir = './data';
    const outputFile = './collected.json';

    let allData = [];
    let fileCount = 0;
    let totalSize = 0;

    try {
        const files = await fs.readdir(dataDir);

        for (const file of files) {
            const fullPath = path.join(dataDir, file);
            const stats = await fs.stat(fullPath);

            if (stats.isFile() && file.endsWith('.json')) {
                try {
                    const content = JSON.parse(await fs.readFile(fullPath, 'utf-8'));
                    allData = allData.concat(content);
                    fileCount++;
                    totalSize += stats.size;
                    logMessage(`Loaded ${file} with ${content.length} providers`);
                } catch (e) {
                    logMessage(`Error reading ${file}: ${e.message}`, 'ERROR');
                }
            }
        }

        await fs.writeFile(outputFile, JSON.stringify(allData, null, 2));

        logMessage(`Collection completed: ${fileCount} files, ${allData.length} providers, ${(totalSize / 1024).toFixed(2)}KB total size`);

        return allData;
    } catch (error) {
        logMessage(`Error collecting providers: ${error.message}`, 'ERROR');
        return [];
    }
}

// Step 3: Profile Scraping Functions
async function safeClick(page, selector, context = '', timeout = CONFIG.selectorTimeout) {
    try {
        await page.waitForSelector(selector, { timeout, state: 'visible' });
        await page.click(selector);
        await delay(300);
        return true;
    } catch (error) {
        logMessage(`Could not click selector ${selector} ${context ? `[${context}]` : ''}: ${error.message}`, 'WARN');
        return false;
    }
}

async function extractBookingSummary(page, providerName) {
    return await retryOperation(async () => {
        try {
            await page.waitForSelector('[data-testid="profile panel"]', {
                timeout: CONFIG.selectorTimeout,
                state: 'attached'
            });

            const bookingData = await page.evaluate(() => {
                const getSessionDuration = () => {
                    const details = Array.from(document.querySelectorAll('div[class*="details"]'));
                    for (const detail of details) {
                        if (detail.textContent && detail.textContent.includes('minutes')) {
                            return detail.textContent.trim();
                        }
                    }
                    return '60 minutes';
                };

                const duration = getSessionDuration();
                const durationMinutes = duration.split(' ')[0];

                const dayElements = document.querySelectorAll('[role="tab"]');
                const daysData = [];

                dayElements.forEach(dayElement => {
                    const spans = dayElement.querySelectorAll('span');
                    if (spans.length >= 3) {
                        const day = spans[0].textContent.trim();
                        const date = spans[1].textContent.trim();
                        const slots = spans[2].textContent.trim();
                        daysData.push({ day, date, slots });
                    }
                });

                return { daysData, duration: durationMinutes };
            });

            if (bookingData.daysData.length > 0) {
                const formattedSummary = bookingData.daysData.map(day => {
                    return `${day.day} - ${day.date}: ${day.slots} (${bookingData.duration} min)`;
                }).join('; ');
                return formattedSummary;
            }
            return 'No booking slots available';
        } catch (error) {
            logMessage(`Booking summary extraction failed for ${providerName}: ${error.message}`, 'WARN');
            return 'Booking information not available';
        }
    }, `booking summary extraction for ${providerName}`);
}

async function getModalData(page, modalTrigger, modalName, providerName) {
    return await retryOperation(async () => {
        try {
            const clicked = await safeClick(page, modalTrigger, `${modalName} modal trigger`, 10000);
            if (!clicked) return [];

            const modalSelectors = [
                '._modal_1ved0_69 ul li a',
                '[role="dialog"] ul li a',
                '.modal ul li a',
                '[class*="modal"] ul li a',
                'ul li a'
            ];

            let modalContentFound = false;
            for (const selector of modalSelectors) {
                try {
                    await page.waitForSelector(selector, { timeout: 3000 });
                    modalContentFound = true;
                    break;
                } catch (e) {
                    continue;
                }
            }

            if (!modalContentFound) {
                throw new Error(`Modal content not found for ${modalName}`);
            }

            const data = await page.$$eval('ul li a', els =>
                els.map(a => a.textContent.trim()).filter(text => text.length > 0)
            );

            const closeSelectors = [
                'button[aria-label="Close"]',
                'button[aria-label="close"]',
                '[data-testid*="close"]',
                'button[class*="close"]',
                '.modal-close',
                'button:has(svg)'
            ];

            for (const selector of closeSelectors) {
                const closed = await safeClick(page, selector, `close ${modalName} modal`, 2000);
                if (closed) {
                    await delay(200);
                    break;
                }
            }

            try {
                await page.keyboard.press('Escape');
                await delay(200);
            } catch (e) { }

            return data;
        } catch (error) {
            logMessage(`Modal data extraction failed for ${modalName} - ${providerName}: ${error.message}`, 'WARN');
            return [];
        }
    }, `${modalName} modal extraction for ${providerName}`);
}

async function getTreatmentApproaches(page, providerName) {
    return await getModalData(
        page,
        '[data-testid="Treatment Approaches-modal-trigger"]',
        'Treatment Approaches',
        providerName
    );
}

async function getMainSpecialties(page, providerName) {
    return await getModalData(
        page,
        '[data-testid="Focus Areas-modal-trigger"]',
        'Focus Areas',
        providerName
    );
}

async function getInsuranceProviders(page, providerName) {
    return await getModalData(
        page,
        '[data-testid="Accepted Insurance Providers-modal-trigger"]',
        'Insurance Providers',
        providerName
    );
}

async function scrapeProviderData(provider, browserContext, srNo) {
    const page = await browserContext.newPage();
    let success = false;

    try {
        logMessage(`Starting scrape for: ${provider.name}`);

        page.setDefaultTimeout(CONFIG.timeout);
        page.setDefaultNavigationTimeout(CONFIG.navigationTimeout);

        await page.route('**/*', (route) => {
            const resourceType = route.request().resourceType();
            if (['image', 'font', 'media'].includes(resourceType)) {
                route.abort();
            } else {
                route.continue();
            }
        });

        await page.goto(provider.profile_url, {
            waitUntil: 'domcontentloaded',
            timeout: CONFIG.navigationTimeout
        });

        try {
            await Promise.race([
                page.waitForSelector('#__next', { timeout: 10000 }),
                page.waitForSelector('h1', { timeout: 10000 }),
                page.waitForSelector('body', { timeout: 10000 })
            ]);
        } catch (e) {
            logMessage(`Slow loading for ${provider.name}, continuing anyway`, 'WARN');
        }

        const providerData = await page.evaluate(() => {
            const getText = (selector) => {
                try {
                    const element = document.querySelector(selector);
                    return element ? element.textContent.trim() : '';
                } catch (e) {
                    return '';
                }
            };

            const getListItems = (sectionTitle) => {
                try {
                    const headers = Array.from(document.querySelectorAll('h2, h3, h4, h5, h6'));
                    const sectionHeader = headers.find(h =>
                        h.textContent && h.textContent.includes(sectionTitle)
                    );
                    if (!sectionHeader) return [];
                    const container = sectionHeader.closest('div, section, article');
                    if (!container) return [];
                    const items = Array.from(container.querySelectorAll('ul li'))
                        .map(li => li.textContent.trim())
                        .filter(text => text.length > 0);
                    return items;
                } catch (e) {
                    return [];
                }
            };

            const getSectionContent = (sectionTitle) => {
                try {
                    const headers = Array.from(document.querySelectorAll('h2, h3, h4, h5, h6'));
                    const header = headers.find(h =>
                        h.textContent && h.textContent.includes(sectionTitle)
                    );
                    if (!header) return '';
                    let content = '';
                    let current = header.nextElementSibling;
                    while (current && !current.matches('h2, h3, h4, h5, h6')) {
                        if (current.textContent?.trim()) {
                            content += current.textContent.trim() + ' ';
                        }
                        current = current.nextElementSibling;
                    }
                    return content.trim();
                } catch (e) {
                    return '';
                }
            };

            const getBadges = () => {
                try {
                    return Array.from(document.querySelectorAll('[class*="badge"], [class*="Badge"]'))
                        .map(b => b.textContent.trim())
                        .filter(text => text.length > 0);
                } catch (e) {
                    return [];
                }
            };

            const name = getText('h1');
            const profession = getText('[class*="license"], [class*="License"], [class*="profession"]');
            const bio = getSectionContent("Hi there, I'm") || getSectionContent("About") || getText('[class*="bio"]');
            const approach = getSectionContent("My approach");
            const focus = getSectionContent("My focus");

            const fullBio = [bio, approach, focus].filter(Boolean).join(' ');

            const focusAreas = getListItems("Focus Areas") || getListItems("Specialties");
            const license = getText('[class*="license"]');
            const education = getSectionContent("Education");
            const languages = getListItems("Languages").join(', ');
            const insuranceProviders = getListItems("Insurance").join(', ');
            const badges = getBadges();
            const location = getText('[class*="location"], [class*="address"]');

            return {
                name,
                profession,
                bio: fullBio,
                license,
                education,
                languages,
                insuranceProviders,
                badges: badges.join(', '),
                location
            };
        });

        const extractionPromises = [
            extractBookingSummary(page, provider.name),
            getTreatmentApproaches(page, provider.name),
            getMainSpecialties(page, provider.name),
            getInsuranceProviders(page, provider.name)
        ];

        const [bookingSummary, treatmentApproaches, mainSpecialties, insuranceProviders] = await Promise.allSettled(
            extractionPromises.map(p =>
                Promise.race([
                    p,
                    new Promise((_, reject) =>
                        setTimeout(() => reject(new Error('Extraction timeout')), 15000)
                    )
                ])
            )
        ).then(results =>
            results.map(result => result.status === 'fulfilled' ? result.value : [])
        );

        // NPI Lookup
        const npiNumber = await fetchNPI(providerData.name || provider.name, provider.state);

        const result = {
            'Url': provider.profile_url,
            'Name': providerData.name || provider.name,
            'Profession': providerData.profession,
            'Clinic Name': '',
            'Bio': providerData.bio || provider.bio,
            'Additional Focus Areas': providerData.focusAreas,
            'Treatment Approaches': treatmentApproaches.join(','),
            'Appointment Types': '',
            'Communities': '',
            'Age Groups': '',
            'Languages': providerData.languages,
            'Highlights': providerData.badges,
            'Gender': '',
            'Pronouns': '',
            'Race Ethnicity': '',
            'Licenses': providerData.license,
            'Locations': providerData.location || provider.city,
            'Education': providerData.education,
            'Faiths': '',
            'Min Session Price': '',
            'Max Session Price': '',
            'Pay Out Of Pocket Status': '',
            'Individual Service Rates': '',
            'General Payment Options': '',
            'Booking Summary': bookingSummary,
            'Booking Url': provider.profile_url,
            'Listed In States': provider.state,
            'States': provider.state,
            'Listed In Websites': 'rula',
            'Urls': provider.profile_url,
            'Connect Link - Facebook': '',
            'Connect Link - Instagram': '',
            'Connect Link - LinkedIn': '',
            'Connect Link - Twitter': '',
            'Connect Link - Website': '',
            'Main Specialties': mainSpecialties.join(','),
            'Accepted IPs': insuranceProviders.join(','),
            'Appointments in 7 Days': 0,
            'NPI Number': npiNumber,
            'Sr. NO': srNo
        };

        success = true;
        logMessage(`Successfully scraped: ${provider.name} - NPI: ${npiNumber || 'Not found'}`);
        return result;

    } catch (error) {
        logError(provider.name, error, 'scraping process');

        // Attempt NPI lookup even on failure
        const npiNumber = await fetchNPI(provider.name, provider.state);

        return {
            'Url': provider.profile_url,
            'Name': provider.name,
            'Profession': '',
            'Bio': '',
            'Locations': provider.city,
            'Listed In States': provider.state,
            'States': provider.state,
            'Listed In Websites': 'rula',
            'NPI Number': npiNumber,
            'Sr. NO': srNo,
            'Error': error.message
        };
    } finally {
        await page.close().catch(() => { });
        if (!success) {
            logMessage(`Scraping failed for: ${provider.name}`, 'ERROR');
        }
    }
}

// Worker thread function
async function workerProcess() {
    const { workerProviders, workerId, startIndex } = workerData;

    if (!workerProviders || !Array.isArray(workerProviders)) {
        logMessage(`Worker ${workerId} received invalid providers data`, 'ERROR');
        parentPort.postMessage({
            type: 'complete',
            workerId,
            results: []
        });
        return;
    }

    logMessage(`Worker ${workerId} started processing ${workerProviders.length} providers`);

    const browser = await firefox.launch({
        headless: CONFIG.headless,
        timeout: CONFIG.timeout
    });

    const browserContext = await browser.newContext();
    const results = [];

    try {
        for (let i = 0; i < workerProviders.length; i++) {
            const provider = workerProviders[i];
            const srNo = startIndex + i + 1;

            try {
                const result = await scrapeProviderData(provider, browserContext, srNo);
                results.push(result);

                parentPort.postMessage({
                    type: 'progress',
                    workerId,
                    provider: provider.name,
                    success: true
                });

            } catch (error) {
                logError(provider.name, error, 'worker process');

                const npiNumber = await fetchNPI(provider.name, provider.state);

                const failedResult = {
                    'Url': provider.profile_url,
                    'Name': provider.name,
                    'Profession': '',
                    'Bio': '',
                    'Locations': provider.city,
                    'Listed In States': provider.state,
                    'States': provider.state,
                    'Listed In Websites': 'rula',
                    'NPI Number': npiNumber,
                    'Sr. NO': srNo,
                    'Error': error.message
                };
                results.push(failedResult);

                parentPort.postMessage({
                    type: 'progress',
                    workerId,
                    provider: provider.name,
                    success: false,
                    error: error.message
                });
            }

            await delay(500);
        }
    } catch (error) {
        logMessage(`Worker ${workerId} fatal error: ${error.message}`, 'ERROR');
    } finally {
        await browserContext.close();
        await browser.close();
    }

    parentPort.postMessage({
        type: 'complete',
        workerId,
        results
    });
}

// Main thread function for profile scraping
async function scrapeProfilesParallel(providers) {
    return new Promise((resolve, reject) => {
        const workers = [];
        const results = [];
        const totalProviders = providers.length;

        const workersCount = Math.min(CONFIG.workers, totalProviders);
        const batchSize = Math.ceil(totalProviders / workersCount);

        logMessage(`Splitting ${totalProviders} providers across ${workersCount} workers (batch size: ${batchSize})`);

        let completedWorkers = 0;
        let processedCount = 0;
        let failedCount = 0;

        for (let i = 0; i < workersCount; i++) {
            const start = i * batchSize;
            const end = Math.min(start + batchSize, totalProviders);
            const workerProviders = providers.slice(start, end);

            if (workerProviders.length === 0) continue;

            const worker = new Worker(__filename, {
                workerData: {
                    workerProviders: workerProviders,
                    workerId: i + 1,
                    startIndex: start
                }
            });

            worker.on('message', (message) => {
                switch (message.type) {
                    case 'progress':
                        processedCount++;
                        if (message.success) {
                            logMessage(`[Worker ${message.workerId}] Processed: ${message.provider}`);
                        } else {
                            failedCount++;
                            logMessage(`[Worker ${message.workerId}] Failed: ${message.provider} - ${message.error}`, 'ERROR');
                        }

                        if (processedCount % 10 === 0 || processedCount === totalProviders) {
                            const progress = ((processedCount / totalProviders) * 100).toFixed(1);
                            logMessage(`Progress: ${processedCount}/${totalProviders} (${progress}%) - Failed: ${failedCount}`);
                        }
                        break;

                    case 'complete':
                        completedWorkers++;
                        results.push(...message.results);
                        logMessage(`Worker ${message.workerId} completed with ${message.results.length} results`);

                        if (completedWorkers === workersCount) {
                            resolve(results);
                        }
                        break;
                }
            });

            worker.on('error', (error) => {
                logMessage(`Worker ${i + 1} error: ${error.message}`, 'ERROR');
                failedCount += workerProviders.length;
            });

            worker.on('exit', (code) => {
                if (code !== 0) {
                    logMessage(`Worker ${i + 1} stopped with exit code ${code}`, 'ERROR');
                }
            });

            workers.push(worker);
        }

        if (workers.length === 0) {
            resolve([]);
        }
    });
}

async function createExcelFile(data) {
    try {
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Rula Providers Data');

        const headers = [
            'Url', 'Name', 'Profession', 'Clinic Name', 'Bio', 'Additional Focus Areas',
            'Treatment Approaches', 'Appointment Types', 'Communities', 'Age Groups',
            'Languages', 'Highlights', 'Gender', 'Pronouns', 'Race Ethnicity', 'Licenses',
            'Locations', 'Education', 'Faiths', 'Min Session Price', 'Max Session Price',
            'Pay Out Of Pocket Status', 'Individual Service Rates', 'General Payment Options',
            'Booking Summary', 'Booking Url', 'Listed In States', 'States', 'Listed In Websites',
            'Urls', 'Connect Link - Facebook', 'Connect Link - Instagram', 'Connect Link - LinkedIn',
            'Connect Link - Twitter', 'Connect Link - Website', 'Main Specialties', 'Accepted IPs',
            'Appointments in 7 Days', 'NPI Number', 'Sr. NO'
        ];

        worksheet.addRow(headers);
        const headerRow = worksheet.getRow(1);
        headerRow.font = { bold: true, color: { argb: 'FFFFFFFF' } };
        headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF2F75B5' } };

        data.forEach(row => worksheet.addRow(headers.map(h => row[h] || '')));

        worksheet.columns.forEach(col => {
            let maxLength = 10;
            col.eachCell({ includeEmpty: true }, c => {
                const len = c.value ? c.value.toString().length : 10;
                if (len > maxLength) maxLength = len;
            });
            col.width = Math.min(maxLength, 50);
        });

        await workbook.xlsx.writeFile('rula_providers_data.xlsx');
        logMessage('Excel file saved as rula_providers_data.xlsx', 'SUCCESS');
    } catch (error) {
        logMessage(`Failed to create Excel file: ${error.message}`, 'ERROR');
        throw error;
    }
}

// Initialize logging
function initializeLogging() {
    const timestamp = new Date().toISOString().replace(/:/g, '-');
    fsSync.writeFileSync(LOG_FILE, `=== Scraping Session Started: ${timestamp} ===\n\n`);
    fsSync.writeFileSync(ERROR_LOG_FILE, '[]');
}

// Main execution function
async function main() {
    try {
        initializeLogging();
        logMessage('Starting complete scraping process...');

        // Step 1: Scrape states in parallel
        await scrapeStatesParallel();

        // Step 2: Collect all providers
        const allProviders = await collectAllProviders();

        if (allProviders.length === 0) {
            logMessage('No providers found to scrape!', 'ERROR');
            return;
        }

        // Step 3: Scrape profiles in parallel with NPI integration
        logMessage(`Starting profile scraping for ${allProviders.length} providers...`);
        const startTime = Date.now();
        const results = await scrapeProfilesParallel(allProviders);
        const endTime = Date.now();

        const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);

        // Step 4: Create Excel file
        await createExcelFile(results);

        const successCount = results.filter(r => !r.Error).length;
        const failCount = results.filter(r => r.Error).length;
        const npiCount = results.filter(r => r['NPI Number']).length;

        logMessage(`Process completed in ${duration} minutes. Success: ${successCount}, Failed: ${failCount}, NPI Found: ${npiCount}`, 'SUCCESS');

    } catch (error) {
        logMessage(`Fatal error in main process: ${error.message}`, 'FATAL');
        process.exit(1);
    }
}

// Entry point
if (isMainThread) {
    main().catch(error => {
        console.error('Fatal error:', error);
        process.exit(1);
    });
} else {
    // Worker thread execution
    workerProcess().catch(error => {
        logMessage(`Worker fatal error: ${error.message}`, 'FATAL');
        process.exit(1);
    });
}
