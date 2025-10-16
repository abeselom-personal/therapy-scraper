import { firefox } from 'playwright';
import { MongoClient } from 'mongodb';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import os from 'os';
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

const CONFIG = {
    maxRetries: 3,
    retryDelay: 1000,
    timeout: 60000,
    navigationTimeout: 30000,
    selectorTimeout: 15000,
    headless: process.env.HEADLESS !== 'false',
    workers: Math.min(os.cpus().length - 1, 4),
    batchSize: 10,
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
        return true;
    } catch (error) {
        console.error('❌ MongoDB connection failed:', error);
        return false;
    }
}

async function logScrapingEvent(eventType, providerName, message, error = null) {
    const logEntry = {
        event_type: eventType,
        provider: providerName,
        message: message,
        error: error,
        timestamp: new Date(),
        worker: workerData?.workerId || 'main'
    };

    try {
        await db.collection('scraping_logs').insertOne(logEntry);
    } catch (err) {
        console.error('Failed to log event:', err);
    }

    const logMessage = `[${eventType.toUpperCase()}] ${providerName}: ${message}`;
    if (isMainThread) {
        console.log(logMessage);
    } else {
        parentPort.postMessage({
            type: 'log',
            message: logMessage
        });
    }

    if (error) console.error(error);
}

async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function retryOperation(operation, operationName, maxRetries = CONFIG.maxRetries) {
    let lastError;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            if (attempt > 1) {
                await logScrapingEvent('retry', operationName, `Retry attempt ${attempt}`);
                await delay(CONFIG.retryDelay * Math.pow(1.5, attempt - 1));
            }

            return await operation();
        } catch (error) {
            lastError = error;
            await logScrapingEvent('retry_failed', operationName, `Attempt ${attempt} failed: ${error.message}`);

            if (attempt === maxRetries) {
                throw lastError;
            }
        }
    }
}

async function safeClick(page, selector, context = '', timeout = CONFIG.selectorTimeout) {
    try {
        await page.waitForSelector(selector, { timeout, state: 'visible' });
        await page.click(selector);
        await delay(300);
        return true;
    } catch (error) {
        await logScrapingEvent('click_failed', context, `Could not click selector ${selector}: ${error.message}`);
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
            await logScrapingEvent('booking_extraction_failed', providerName, `Booking summary extraction failed: ${error.message}`);
            return 'Booking information not available';
        }
    }, `booking summary extraction for ${providerName}`);
}

async function getModalData(page, modalTrigger, modalName, providerName) {
    return await retryOperation(async () => {
        try {
            const clicked = await safeClick(page, modalTrigger, `${modalName} modal trigger`, 10000);
            if (!clicked) {
                return [];
            }

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
            await logScrapingEvent('modal_extraction_failed', providerName, `${modalName} modal data extraction failed: ${error.message}`);
            return [];
        }
    }, `${modalName} modal extraction for ${providerName}`);
}

async function scrapeProviderData(provider, browserContext, srNo) {
    const page = await browserContext.newPage();
    let success = false;

    try {
        await logScrapingEvent('scraping_start', provider.name, `Starting detailed scrape`);

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
            await logScrapingEvent('slow_loading', provider.name, `Slow loading, continuing anyway`);
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
            getModalData(page, '[data-testid="Treatment Approaches-modal-trigger"]', 'Treatment Approaches', provider.name),
            getModalData(page, '[data-testid="Focus Areas-modal-trigger"]', 'Focus Areas', provider.name),
            getModalData(page, '[data-testid="Accepted Insurance Providers-modal-trigger"]', 'Insurance Providers', provider.name)
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

        const detailedData = {
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
            'Listed In States': provider.city.split(', ')[1] || 'AK',
            'States': provider.city.split(', ')[1] || 'AK',
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
            'Sr. NO': srNo,
            'detailed_data': {
                treatment_approaches: treatmentApproaches,
                main_specialties: mainSpecialties,
                insurance_providers: insuranceProviders,
                booking_summary: bookingSummary,
                scraped_at: new Date()
            },
            'detailed_scraped': true,
            'last_scraped': new Date()
        };

        success = true;
        await logScrapingEvent('scraping_success', provider.name, `Successfully scraped detailed data`);
        return detailedData;

    } catch (error) {
        await logScrapingEvent('scraping_error', provider.name, `Scraping failed: ${error.message}`, error);

        return {
            'Url': provider.profile_url,
            'Name': provider.name,
            'Profession': provider.badges ? provider.badges.join(', ') : '',
            'Bio': provider.bio || '',
            'Locations': provider.city,
            'Listed In States': provider.city.split(', ')[1] || 'AK',
            'States': provider.city.split(', ')[1] || 'AK',
            'Listed In Websites': 'rula',
            'Sr. NO': srNo,
            'Error': error.message,
            'detailed_scraped': false,
            'last_scraped': new Date()
        };
    } finally {
        await page.close().catch(() => { });
        if (!success) {
            await logScrapingEvent('scraping_failed', provider.name, `Scraping process failed`);
        }
    }
}

// Worker thread function
async function workerProcess() {
    const { workerProviders, workerId, startIndex } = workerData;

    const workerClient = new MongoClient(MONGO_URI);
    await workerClient.connect();
    const workerDb = workerClient.db(MONGO_DB);

    if (!workerProviders || !Array.isArray(workerProviders)) {
        await logScrapingEvent('worker_error', `Worker ${workerId}`, `Received invalid providers data`);
        parentPort.postMessage({
            type: 'complete',
            workerId,
            results: []
        });
        await workerClient.close();
        return;
    }

    await logScrapingEvent('worker_start', `Worker ${workerId}`, `Started processing ${workerProviders.length} providers`);

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

                // Update MongoDB with detailed data
                await workerDb.collection('providers').updateOne(
                    { profile_url: provider.profile_url },
                    {
                        $set: result,
                        $currentDate: { last_updated: true }
                    }
                );

                parentPort.postMessage({
                    type: 'progress',
                    workerId,
                    provider: provider.name,
                    success: true
                });

            } catch (error) {
                await logScrapingEvent('worker_error', provider.name, `Worker process error: ${error.message}`, error);

                const failedResult = {
                    'Url': provider.profile_url,
                    'Name': provider.name,
                    'Profession': provider.badges ? provider.badges.join(', ') : '',
                    'Bio': provider.bio || '',
                    'Locations': provider.city,
                    'Listed In States': provider.city.split(', ')[1] || 'AK',
                    'States': provider.city.split(', ')[1] || 'AK',
                    'Listed In Websites': 'rula',
                    'Sr. NO': srNo,
                    'Error': error.message,
                    'detailed_scraped': false
                };
                results.push(failedResult);

                await workerDb.collection('providers').updateOne(
                    { profile_url: provider.profile_url },
                    {
                        $set: failedResult,
                        $currentDate: { last_updated: true }
                    }
                );

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
        await logScrapingEvent('worker_fatal', `Worker ${workerId}`, `Fatal error: ${error.message}`, error);
    } finally {
        await browserContext.close();
        await browser.close();
        await workerClient.close();
    }

    parentPort.postMessage({
        type: 'complete',
        workerId,
        results
    });
}

// Main thread function
async function mainProcess() {
    if (!await connectToMongo()) {
        process.exit(1);
    }

    // Get providers that need detailed scraping
    const query = { detailed_scraped: { $ne: true } };
    if (CONFIG.scrapeLimit > 0) {
        const providers = await db.collection('providers')
            .find(query)
            .limit(CONFIG.scrapeLimit)
            .toArray();
        return await processProviders(providers);
    } else {
        const providers = await db.collection('providers')
            .find(query)
            .toArray();
        return await processProviders(providers);
    }
}

async function processProviders(providers) {
    return new Promise((resolve, reject) => {
        const workers = [];
        const results = [];
        const totalProviders = providers.length;

        if (totalProviders === 0) {
            console.log('✅ No providers need detailed scraping');
            resolve([]);
            return;
        }

        // Split providers into batches for workers
        const workersCount = Math.min(CONFIG.workers, totalProviders);
        const batchSize = Math.ceil(totalProviders / workersCount);

        console.log(`Splitting ${totalProviders} providers across ${workersCount} workers (batch size: ${batchSize})`);

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

            worker.on('message', async (message) => {
                switch (message.type) {
                    case 'log':
                        console.log(`[Worker ${message.workerId}] ${message.message}`);
                        break;
                    case 'progress':
                        processedCount++;
                        if (message.success) {
                            console.log(`[Worker ${message.workerId}] Processed: ${message.provider}`);
                        } else {
                            failedCount++;
                            console.log(`[Worker ${message.workerId}] Failed: ${message.provider} - ${message.error}`);
                        }

                        if (processedCount % 10 === 0 || processedCount === totalProviders) {
                            const progress = ((processedCount / totalProviders) * 100).toFixed(1);
                            console.log(`Progress: ${processedCount}/${totalProviders} (${progress}%) - Failed: ${failedCount}`);
                        }
                        break;

                    case 'complete':
                        completedWorkers++;
                        results.push(...message.results);
                        console.log(`Worker ${message.workerId} completed with ${message.results.length} results`);

                        if (completedWorkers === workersCount) {
                            resolve(results);
                        }
                        break;
                }
            });

            worker.on('error', (error) => {
                console.log(`Worker ${i + 1} error: ${error.message}`);
                failedCount += workerProviders.length;
            });

            worker.on('exit', (code) => {
                if (code !== 0) {
                    console.log(`Worker ${i + 1} stopped with exit code ${code}`);
                }
            });

            workers.push(worker);
        }

        if (workers.length === 0) {
            resolve([]);
        }
    });
}

// Main execution
if (isMainThread) {
    (async () => {
        try {
            console.log('Starting detailed scraping process...');

            const startTime = Date.now();
            const results = await mainProcess();
            const endTime = Date.now();

            const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);

            const successCount = results.filter(r => !r.Error).length;
            const failCount = results.filter(r => r.Error).length;

            await logScrapingEvent('scraping_complete', 'all', `Detailed scraping completed in ${duration} minutes. Success: ${successCount}, Failed: ${failCount}`);

            console.log(`✅ Detailed scraping completed in ${duration} minutes. Success: ${successCount}, Failed: ${failCount}`);

        } catch (error) {
            await logScrapingEvent('fatal_error', 'main', `Fatal error in main process: ${error.message}`, error);
            console.error(`❌ Fatal error: ${error.message}`);
            process.exit(1);
        } finally {
            if (client) {
                await client.close();
            }
        }
    })();
} else {
    // Worker thread execution
    workerProcess().catch(async error => {
        await logScrapingEvent('worker_fatal', `Worker ${workerData.workerId}`, `Worker fatal error: ${error.message}`, error);
        process.exit(1);
    });
}
