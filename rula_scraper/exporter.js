import { firefox } from 'playwright';
import fs from 'fs';
import ExcelJS from 'exceljs';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load and combine all provider data
const dataDir = './data';
let allProviders = [];

try {
    const files = fs.readdirSync(dataDir);
    files.forEach(file => {
        if (file.endsWith('.json')) {
            const content = JSON.parse(fs.readFileSync(path.join(dataDir, file), 'utf-8'));
            allProviders = allProviders.concat(content);
        }
    });
    console.log(`Loaded ${allProviders.length} providers from ${files.length} state files`);
} catch (error) {
    console.error('Error loading provider data:', error);
    process.exit(1);
}

const providers = allProviders;
console.log(`Total providers to process: ${providers.length}`);

// Configuration
const CONFIG = {
    maxRetries: 3,
    retryDelay: 1000,
    timeout: 60000,
    navigationTimeout: 30000,
    selectorTimeout: 15000,
    headless: process.env.HEADLESS !== 'false',
    workers: Math.min(os.cpus().length - 1, 4),
    batchSize: 10
};

// Logging setup
const LOG_FILE = './scraping_log.txt';
const ERROR_LOG_FILE = './scraping_errors.json';

function logMessage(message, type = 'INFO') {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] [${type}] ${message}\n`;
    console.log(logEntry.trim());
    fs.appendFileSync(LOG_FILE, logEntry);
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
    if (fs.existsSync(ERROR_LOG_FILE)) {
        try {
            errors = JSON.parse(fs.readFileSync(ERROR_LOG_FILE, 'utf-8'));
        } catch (e) {
            logMessage('Could not read error log file', 'WARN');
        }
    }

    errors.push(errorEntry);
    fs.writeFileSync(ERROR_LOG_FILE, JSON.stringify(errors, null, 2));
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
            'Sr. NO': srNo
        };

        success = true;
        logMessage(`Successfully scraped: ${provider.name}`);
        return result;

    } catch (error) {
        logError(provider.name, error, 'scraping process');

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

// Main thread function
async function mainProcess() {
    return new Promise((resolve, reject) => {
        const workers = [];
        const results = [];
        const totalProviders = providers.length;

        // Split providers into batches for workers
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
            'Appointments in 7 Days', 'Sr. NO'
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
    fs.writeFileSync(LOG_FILE, `=== Scraping Session Started: ${timestamp} ===\n\n`);
    fs.writeFileSync(ERROR_LOG_FILE, '[]');
}

// Main execution
if (isMainThread) {
    (async () => {
        try {
            initializeLogging();
            logMessage('Starting multithreaded scraping process...');
            logMessage(`Total providers to process: ${providers.length}`);

            const startTime = Date.now();
            const results = await mainProcess();
            const endTime = Date.now();

            const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);

            await createExcelFile(results);

            const successCount = results.filter(r => !r.Error).length;
            const failCount = results.filter(r => r.Error).length;

            logMessage(`Scraping completed in ${duration} minutes. Success: ${successCount}, Failed: ${failCount}`, 'SUCCESS');

        } catch (error) {
            logMessage(`Fatal error in main process: ${error.message}`, 'FATAL');
            process.exit(1);
        }
    })();
} else {
    // Worker thread execution
    workerProcess().catch(error => {
        logMessage(`Worker fatal error: ${error.message}`, 'FATAL');
        process.exit(1);
    });
}
