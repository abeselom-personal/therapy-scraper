// test_playwright.js
import { firefox } from 'playwright';

(async () => {
    try {
        const browser = await firefox.launch({ headless: true });
        console.log('✅ Playwright is working correctly!');
        await browser.close();
        process.exit(0);
    } catch (error) {
        console.error('❌ Playwright error:', error);
        process.exit(1);
    }
})();
