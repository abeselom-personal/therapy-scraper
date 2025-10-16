import { firefox } from 'playwright';
import fs from 'fs';

const statesAbbr = [
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'dc', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la',
    'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or',
    'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy'
];

const abbrToState = {
    al: 'Alabama', ak: 'Alaska', az: 'Arizona', ar: 'Arkansas', ca: 'California', co: 'Colorado',
    ct: 'Connecticut', de: 'Delaware', dc: 'District of Columbia', fl: 'Florida', ga: 'Georgia',
    hi: 'Hawaii', id: 'Idaho', il: 'Illinois', in: 'Indiana', ia: 'Iowa', ks: 'Kansas', ky: 'Kentucky',
    la: 'Louisiana', me: 'Maine', md: 'Maryland', ma: 'Massachusetts', mi: 'Michigan', mn: 'Minnesota',
    ms: 'Mississippi', mo: 'Missouri', mt: 'Montana', ne: 'Nebraska', nv: 'Nevada', nh: 'New Hampshire',
    nj: 'New Jersey', nm: 'New Mexico', ny: 'New York', nc: 'North Carolina', nd: 'North Dakota',
    oh: 'Ohio', ok: 'Oklahoma', or: 'Oregon', pa: 'Pennsylvania', ri: 'Rhode Island', sc: 'South Carolina',
    sd: 'South Dakota', tn: 'Tennessee', tx: 'Texas', ut: 'Utah', vt: 'Vermont', va: 'Virginia',
    wa: 'Washington', wv: 'West Virginia', wi: 'Wisconsin', wy: 'Wyoming'
};

(async () => {
    const browser = await firefox.launch({ headless: false });
    const context = await browser.newContext();
    const page = await context.newPage();
    const urls = [];

    for (let i = 0; i < statesAbbr.length; i++) {
        const abbr = statesAbbr[i];
        const state = abbrToState[abbr];
        const url = `https://www.rula.com/therapists/${abbr}`;
        console.log(`Visiting ${url} (${i + 1}/${statesAbbr.length}) - State: ${state}`);

        await page.goto(url);
        await page.waitForTimeout(1000);

        // check for entry modal
        const modal = page.locator('div._window_at2f6_88');
        if (await modal.count() > 0) {
            const modalText = await modal.locator('div.entry-modal_content__Pw2pV').innerText();
            console.log(`Entry modal detected. URL: ${url}, Title/Text: "${modalText}"`);
            continue; // skip this URL
        }

        // get provider count
        let providerCount = 'N/A';
        try {
            const providerSpan = page.locator('div[data-testid="results-description"] span').filter({ hasText: 'available providers' });
            providerCount = await providerSpan.first().innerText();
        } catch (err) {
            console.log(`Could not get provider count for ${state}: ${err}`);
        }

        console.log(`State: ${state}, Providers: ${providerCount}`);
        urls.push(url);
    }

    fs.writeFileSync('routes.txt', urls.join('\n'));
    console.log('All URLs saved to routes.txt');

    await browser.close();
})();
