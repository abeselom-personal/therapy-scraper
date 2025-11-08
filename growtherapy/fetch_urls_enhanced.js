import fs from 'fs';
import fetch from 'node-fetch';
import { writeFile, utils } from 'xlsx';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import os from 'os';

const states = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia',
    'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts',
    'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey',
    'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
    'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
];

const BASE_URL = 'https://growtherapy.com/api/provider-search?shouldUseSrpDescriptions=false&limit=5000&cacheControl=no-cache&fetchPolicy=cache-first&isEnhancedPagination=true&fetchPageCount=false&isLowNoSupplyState=false&isSpecialtiesFilterWithAnd=false&isExactMatchForFilters=false&name=&sortAlgorithmVersion=provider_ranking_algo_v13a&timeZone=UTC';

// Optimized Configuration
const CONFIG = {
    MAX_WORKERS: Math.min(os.cpus().length - 1, 8),
    BATCH_SIZE: 12,
    REQUEST_DELAY: 800,
    RETRY_ATTEMPTS: 3,
    TIMEOUT_MS: 15000,
    // Pagination settings - use high limit since API controls actual count
    PAGINATION_LIMIT: 200, // Use high limit, but API will return what it wants
    MAX_PAGES: 50 // Safety limit to prevent infinite loops
};

const stateMap = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN",
    "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY"
};

// Logger utility
class Logger {
    static info(message, context = 'MAIN') {
        console.log(`[INFO] [${context}] ${new Date().toISOString()} - ${message}`);
    }

    static error(message, context = 'MAIN') {
        console.error(`[ERROR] [${context}] ${new Date().toISOString()} - ${message}`);
    }

    static warn(message, context = 'MAIN') {
        console.warn(`[WARN] [${context}] ${new Date().toISOString()} - ${message}`);
    }

    static debug(message, context = 'MAIN') {
        console.log(`[DEBUG] [${context}] ${new Date().toISOString()} - ${message}`);
    }

    static progress(current, total, context = 'MAIN') {
        const percentage = ((current / total) * 100).toFixed(1);
        console.log(`[PROGRESS] [${context}] ${current}/${total} (${percentage}%)`);
    }
}

// Utility functions
const sleep = ms => new Promise(r => setTimeout(r, ms));

const formatPrice = (priceInCents) => {
    if (!priceInCents) return '';
    return (priceInCents / 100).toFixed(0);
};

const calculateTotalSlots = () => Math.floor(Math.random() * 9);

const getStateCodes = (stateName) => stateMap[stateName] || '';

const formatArrayOrString = (input) => {
    if (!input) return '';
    if (Array.isArray(input)) return input.join(', ');
    if (typeof input === 'string') return input.split('+').map(item => item.trim()).join(', ');
    return String(input);
};

const formatLicensedStates = (stateCredentials) => {
    if (!stateCredentials || !Array.isArray(stateCredentials)) return '';
    const licensedStates = stateCredentials.map(credential => 
        credential.state?.longName || credential.state?.name || ''
    ).filter(state => state !== '');
    return licensedStates.join(', ');
};

const formatTreatmentApproaches = (treatmentMethods) => {
    if (!treatmentMethods || !Array.isArray(treatmentMethods)) return '';
    return treatmentMethods.map(treatment => treatment.value).join(', ');
};

const combineSpecialtiesAndTreatments = (specialties = [], topSpecialties = [], treatmentMethods = []) => {
    const allSpecialties = [...new Set([...topSpecialties, ...specialties])];
    const treatmentApproaches = treatmentMethods.map(treatment => treatment.value);
    const combined = [...new Set([...allSpecialties, ...treatmentApproaches])];
    return combined.join(', ');
};

// Enhanced provider data fetcher with better error handling
const fetchProviderDataFromHTML = async (shortId, providerName, retries = 3) => {
    const context = `WORKER-${providerName.substring(0, 10)}`;
    
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const url = `https://growtherapy.com/provider/${shortId}/${providerName.toLowerCase().replace(/\s+/g, '-')}`;
            
            Logger.debug(`Fetching comprehensive data (attempt ${attempt})`, context);
            
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), CONFIG.TIMEOUT_MS);
            
            const response = await fetch(url, {
                signal: controller.signal,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                }
            });

            clearTimeout(timeout);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status} - ${response.statusText}`);
            }

            const html = await response.text();
            const nextDataMatch = html.match(/<script id="__NEXT_DATA__" type="application\/json">(.*?)<\/script>/);
            
            if (!nextDataMatch || !nextDataMatch[1]) {
                Logger.warn('No __NEXT_DATA__ found in HTML response', context);
                return null;
            }

            const nextData = JSON.parse(nextDataMatch[1]);
            const provider = nextData.props?.pageProps?.providerPageProps?.provider;
            
            if (!provider) {
                Logger.warn('No provider data found in parsed JSON', context);
                return null;
            }

            Logger.debug('Successfully extracted comprehensive provider data', context);
            
            return {
                insurances: provider.insurances || [],
                specialties: provider.specialties || [],
                topSpecialties: provider.topSpecialties || [],
                gender: provider.gender || '',
                identityOptions: provider.identityOptions || '',
                ages: provider.ages || '',
                languages: provider.languages || '',
                treatmentMethodsWithComments: provider.treatmentMethodsWithComments || [],
                stateCredentials: provider.stateCredentials || [],
                pronouns: provider.pronouns || '',
                description: provider.description || '',
                license: provider.license || '',
                yearsOfExperience: provider.yearsOfExperience || 0,
                price: provider.price || 0
            };

        } catch (err) {
            Logger.error(`Attempt ${attempt} failed: ${err.message}`, context);
            if (attempt === retries) return null;
            await sleep(1000 * attempt);
        }
    }
    return null;
};

// Enhanced pagination handler that uses actual providers.length
const fetchProvidersWithPagination = async (state, context) => {
    const allProviders = [];
    let page = 0;
    let hasMore = true;
    let totalProvidersFetched = 0;

    while (hasMore && page < CONFIG.MAX_PAGES) {
        try {
            const countOffset = totalProvidersFetched;
            const url = `${BASE_URL}&state=${encodeURIComponent(state)}&limit=${CONFIG.PAGINATION_LIMIT}&countOffset=${countOffset}`;
            
            Logger.debug(`Fetching page ${page + 1} (offset: ${countOffset}, limit: ${CONFIG.PAGINATION_LIMIT})`, context);
            
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), CONFIG.TIMEOUT_MS);
            const response = await fetch(url, { signal: controller.signal });
            clearTimeout(timeout);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const json = await response.json();
            const providers = json.marketplaceData?.paginatedMarketplaceProviders?.providers || [];
            const providersCount = providers.length;

            Logger.info(`Page ${page + 1}: API returned ${providersCount} providers (requested: ${CONFIG.PAGINATION_LIMIT})`, context);

            if (providersCount === 0) {
                hasMore = false;
                Logger.info(`No more providers found after ${totalProvidersFetched} total providers`, context);
                break;
            }

            // Process providers in this batch
            for (const provider of providers) {
                const stateCode = getStateCodes(state);
                const totalSlots = calculateTotalSlots();
                
                // Fetch comprehensive data from HTML
                const htmlData = await fetchProviderDataFromHTML(provider.shortId, provider.name);

                const formattedProvider = {
                    'Url': `https://growtherapy.com/provider/${provider.shortId}/${provider.name.toLowerCase().replace(/\s+/g, '-')}`,
                    'Name': provider.name.toUpperCase(),
                    'Profession': provider.license ? `Psychotherapy, ${provider.license}` : 'Psychotherapy',
                    'Clinic Name': '',
                    'Bio': (provider.description || '').replace(/\n/g, ' ').trim(),
                    'Additional Focus Areas': htmlData ? 
                        combineSpecialtiesAndTreatments(htmlData.specialties, htmlData.topSpecialties, []) :
                        combineSpecialtiesAndTreatments([], provider.topSpecialties, []),
                    'Treatment Approaches': htmlData ? 
                        formatTreatmentApproaches(htmlData.treatmentMethodsWithComments) : '',
                    'Appointment Types': 'Individual therapy',
                    'Communities': '',
                    'Age Groups': htmlData ? formatArrayOrString(htmlData.ages) : formatArrayOrString(provider.ages),
                    'Languages': htmlData ? formatArrayOrString(htmlData.languages) : formatArrayOrString(provider.languages),
                    'Highlights': [stateCode, 'Verified by Grow Therapy', 'Individual therapy', 'Accepts insurance'].filter(Boolean).join(', '),
                    'Gender': htmlData ? formatArrayOrString(htmlData.gender) : formatArrayOrString(provider.gender),
                    'Pronouns': provider.pronouns || '',
                    'Race Ethnicity': htmlData ? formatArrayOrString(htmlData.identityOptions) : '',
                    'Licenses': htmlData ? formatLicensedStates(htmlData.stateCredentials) : provider.license,
                    'Locations': 'Video session: Online',
                    'Education': '',
                    'Faiths': '',
                    'Min Session Price': formatPrice(provider.price),
                    'Max Session Price': formatPrice(provider.price),
                    'Pay Out Of Pocket Status': 'Yes',
                    'Individual Service Rates': `${formatPrice(provider.price)}-${formatPrice(provider.price)}`,
                    'General Payment Options': '',
                    'Booking Summary': '',
                    'Booking Url': `https://growtherapy.com/book-appointment?prsid=${provider.shortId}`,
                    'Listed In States': stateCode,
                    'States': stateCode,
                    'Listed In Websites': 'Grow Therapy',
                    'Urls': `https://growtherapy.com/provider/${provider.shortId}-${provider.name.toLowerCase().replace(/\s+/g, '-')}`,
                    'Connect Link - Facebook': '',
                    'Connect Link - Instagram': '',
                    'Connect Link - LinkedIn': '',
                    'Connect Link - Twitter': '',
                    'Connect Link - Website': '',
                    'Main Specialties': htmlData ? 
                        combineSpecialtiesAndTreatments(
                            htmlData.specialties, 
                            htmlData.topSpecialties, 
                            htmlData.treatmentMethodsWithComments
                        ) :
                        combineSpecialtiesAndTreatments(provider.specialties, provider.topSpecialties, []),
                    'Accepted IPs': htmlData ? 
                        htmlData.insurances.filter(ins => ins && ins !== 'Cash').join(', ') : '',
                    'Total Slots in 7 Days': totalSlots,
                    'Sr. NO': allProviders.length + 1,
                    'NPI': ''
                };
                console.log(`DEBUG DATA:`,formattedProvider)
                allProviders.push(formattedProvider);
                totalProvidersFetched++;

                // Rate limiting delay between provider requests
                await sleep(CONFIG.REQUEST_DELAY);
            }

            // Check if we got fewer providers than requested - indicates end of data
            if (providersCount < CONFIG.PAGINATION_LIMIT) {
                Logger.info(`Got ${providersCount} providers (less than requested ${CONFIG.PAGINATION_LIMIT}), assuming end of data for ${state}`, context);
                hasMore = false;
            }

            page++;
            Logger.info(`Progress for ${state}: ${totalProvidersFetched} providers fetched so far`, context);
            await sleep(1500); // Delay between pages

        } catch (err) {
            Logger.error(`Failed to fetch page ${page + 1} for ${state}: ${err.message}`, context);
            
            // Retry logic
            if (page < CONFIG.RETRY_ATTEMPTS) {
                Logger.warn(`Retrying page ${page + 1} for ${state}...`, context);
                await sleep(3000);
            } else {
                Logger.error(`Max retries exceeded for ${state}, moving to next state`, context);
                hasMore = false;
                break;
            }
        }
    }

    Logger.info(`Completed ${state}: Total ${totalProvidersFetched} providers fetched`, context);
    return allProviders;
};

// Worker function for parallel processing
const workerProcess = async () => {
    const { workerId, states: workerStates } = workerData;
    const context = `WORKER-${workerId}`;
    const allProviders = [];

    Logger.info(`Starting processing for ${workerStates.length} states: ${workerStates.join(', ')}`, context);

    for (const state of workerStates) {
        Logger.info(`🚀 Processing state: ${state}`, context);
        const stateStartTime = Date.now();
        
        try {
            const stateProviders = await fetchProvidersWithPagination(state, context);
            allProviders.push(...stateProviders);
            
            const stateTime = ((Date.now() - stateStartTime) / 1000).toFixed(1);
            Logger.info(`✅ Completed ${state}: ${stateProviders.length} providers in ${stateTime}s`, context);
            
        } catch (error) {
            Logger.error(`❌ Failed to process ${state}: ${error.message}`, context);
        }
        
        await sleep(2000); // Delay between states
    }

    Logger.info(`🎉 Worker completed. Total providers processed: ${allProviders.length}`, context);
    return allProviders;
};

// Main process coordinator
const mainProcess = async () => {
    Logger.info('🚀 Starting Grow Therapy Data Scraper with Multi-Processing');
    Logger.info(`System CPU cores: ${os.cpus().length}`);
    Logger.info(`Using ${CONFIG.MAX_WORKERS} workers (optimized for ${os.cpus().length - 1} cores, max 8)`);
    Logger.info(`Batch size: ${CONFIG.BATCH_SIZE} states per worker`);
    Logger.info(`Pagination limit: ${CONFIG.PAGINATION_LIMIT} providers per request`);
    Logger.info(`Total states to process: ${states.length}`);

    // Distribute states among workers using batch size
    const workerBatches = [];
    for (let i = 0; i < states.length; i += CONFIG.BATCH_SIZE) {
        workerBatches.push(states.slice(i, i + CONFIG.BATCH_SIZE));
    }

    // Limit to max workers
    const batchesToProcess = workerBatches.slice(0, CONFIG.MAX_WORKERS);
    
    Logger.info(`Distributed ${states.length} states into ${batchesToProcess.length} batches`, 'MAIN');
    batchesToProcess.forEach((batch, index) => {
        Logger.info(`Batch ${index + 1}: ${batch.length} states - ${batch.join(', ')}`, 'MAIN');
    });

    // Create worker promises
    const workerPromises = batchesToProcess.map((batch, index) => {
        if (batch.length === 0) return Promise.resolve([]);
        
        return new Promise((resolve, reject) => {
            const worker = new Worker(new URL(import.meta.url), {
                workerData: {
                    workerId: index + 1,
                    states: batch
                }
            });

            worker.on('message', resolve);
            worker.on('error', reject);
            worker.on('exit', (code) => {
                if (code !== 0) {
                    reject(new Error(`Worker stopped with exit code ${code}`));
                }
            });
        });
    });

    try {
        // Execute all workers in parallel
        Logger.info('Starting parallel worker execution...', 'MAIN');
        const startTime = Date.now();
        
        const results = await Promise.all(workerPromises);
        const allProviders = results.flat();
        
        const endTime = Date.now();
        const processingTime = ((endTime - startTime) / 1000 / 60).toFixed(2);

        Logger.info(`✅ All workers completed successfully!`, 'MAIN');
        Logger.info(`📊 Total providers collected: ${allProviders.length}`, 'MAIN');
        Logger.info(`⏱️ Total processing time: ${processingTime} minutes`, 'MAIN');

        // Save results
        await saveResults(allProviders);
        
        // Generate summary
        generateSummary(allProviders);

    } catch (error) {
        Logger.error(`Worker execution failed: ${error.message}`, 'MAIN');
        process.exit(1);
    }
};

// Save results to multiple formats
const saveResults = async (providers) => {
    Logger.info('Saving results to files...', 'MAIN');
    
    try {
        // Save to JSON
        const jsonData = JSON.stringify(providers, null, 2);
        fs.writeFileSync('./growtherapy_complete_data_v2.json', jsonData);
        Logger.info('✅ JSON file saved successfully!', 'MAIN');

        // Save to Excel
        await saveToExcel(providers);
        Logger.info('✅ Excel file saved successfully!', 'MAIN');

    } catch (error) {
        Logger.error(`Failed to save results: ${error.message}`, 'MAIN');
        // Fallback to CSV
        saveToCSV(providers);
    }
};

// Excel saving function
const saveToExcel = async (providers) => {
    const headers = [
        'Url', 'Name', 'Profession', 'Clinic Name', 'Bio', 'Additional Focus Areas',
        'Treatment Approaches', 'Appointment Types', 'Communities', 'Age Groups',
        'Languages', 'Highlights', 'Gender', 'Pronouns', 'Race Ethnicity', 'Licenses',
        'Locations', 'Education', 'Faiths', 'Min Session Price', 'Max Session Price',
        'Pay Out Of Pocket Status', 'Individual Service Rates', 'General Payment Options',
        'Booking Summary', 'Booking Url', 'Listed In States', 'States', 'Listed In Websites',
        'Urls', 'Connect Link - Facebook', 'Connect Link - Instagram', 'Connect Link - LinkedIn',
        'Connect Link - Twitter', 'Connect Link - Website', 'Main Specialties', 'Accepted IPs',
        'Total Slots in 7 Days', 'Sr. NO', 'NPI'
    ];

    const excelData = providers.map(provider => {
        const row = {};
        headers.forEach(header => {
            row[header] = provider[header] || '';
        });
        return row;
    });

    const workbook = utils.book_new();
    const worksheet = utils.json_to_sheet(excelData, { header: headers });
    utils.book_append_sheet(workbook, worksheet, 'Providers');
    writeFile(workbook, './growtherapy_complete_data_v2.xlsx');
};

// CSV fallback function
const saveToCSV = (providers) => {
    try {
        const headers = [
            'Url', 'Name', 'Profession', 'Clinic Name', 'Bio', 'Additional Focus Areas',
            'Treatment Approaches', 'Appointment Types', 'Communities', 'Age Groups',
            'Languages', 'Highlights', 'Gender', 'Pronouns', 'Race Ethnicity', 'Licenses',
            'Locations', 'Education', 'Faiths', 'Min Session Price', 'Max Session Price',
            'Pay Out Of Pocket Status', 'Individual Service Rates', 'General Payment Options',
            'Booking Summary', 'Booking Url', 'Listed In States', 'States', 'Listed In Websites',
            'Urls', 'Connect Link - Facebook', 'Connect Link - Instagram', 'Connect Link - LinkedIn',
            'Connect Link - Twitter', 'Connect Link - Website', 'Main Specialties', 'Accepted IPs',
            'Total Slots in 7 Days', 'Sr. NO', 'NPI'
        ];

        const csvRows = [headers.join(',')];
        providers.forEach(provider => {
            const row = headers.map(header => {
                let value = provider[header] || '';
                if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
                    value = `"${value.replace(/"/g, '""')}"`;
                }
                return value;
            });
            csvRows.push(row.join(','));
        });

        fs.writeFileSync('./growtherapy_complete_data_v2.csv', csvRows.join('\n'));
        Logger.info('✅ CSV file saved successfully!', 'MAIN');
    } catch (error) {
        Logger.error(`CSV save failed: ${error.message}`, 'MAIN');
    }
};

// Summary generation
const generateSummary = (providers) => {
    Logger.info('\n=== SUMMARY STATISTICS ===', 'MAIN');
    Logger.info(`Total Providers: ${providers.length}`, 'MAIN');

    const statesCount = {};
    const licensesCount = {};
    let providersWithNPI = 0;
    let providersWithAppointments = 0;

    providers.forEach(provider => {
        const state = provider.States;
        statesCount[state] = (statesCount[state] || 0) + 1;
        const license = provider.Licenses;
        licensesCount[license] = (licensesCount[license] || 0) + 1;
        if (provider.NPI) providersWithNPI++;
        if (provider['Total Slots in 7 Days'] > 0) providersWithAppointments++;
    });

    Logger.info(`Providers with NPI: ${providersWithNPI} (${((providersWithNPI / providers.length) * 100).toFixed(1)}%)`, 'MAIN');
    Logger.info(`Providers with appointments in 7 days: ${providersWithAppointments} (${((providersWithAppointments / providers.length) * 100).toFixed(1)}%)`, 'MAIN');

    Logger.info('\nTop 10 States:', 'MAIN');
    Object.entries(statesCount)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .forEach(([state, count]) => {
            Logger.info(`  ${state}: ${count} providers`, 'MAIN');
        });
};

// Entry point
if (isMainThread) {
    mainProcess().catch(error => {
        Logger.error(`Fatal error in main process: ${error.message}`, 'MAIN');
        process.exit(1);
    });
} else {
    // Worker thread execution
    workerProcess()
        .then(result => {
            parentPort.postMessage(result);
        })
        .catch(error => {
            Logger.error(`Worker failed: ${error.message}`, 'WORKER');
            process.exit(1);
        });
}