import ExcelJS from 'exceljs';
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

async function connectToMongo() {
    try {
        const client = new MongoClient(MONGO_URI);
        await client.connect();
        const db = client.db(MONGO_DB);
        console.log('✅ Connected to MongoDB');
        return { client, db };
    } catch (error) {
        console.error('❌ MongoDB connection failed:', error);
        throw error;
    }
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

        data.forEach((row, index) => {
            const rowData = headers.map(header => {
                // Handle nested detailed_data
                if (header === 'Treatment Approaches' && row.detailed_data) {
                    return row.detailed_data.treatment_approaches?.join(', ') || row[header] || '';
                }
                if (header === 'Main Specialties' && row.detailed_data) {
                    return row.detailed_data.main_specialties?.join(', ') || row[header] || '';
                }
                if (header === 'Accepted IPs' && row.detailed_data) {
                    return row.detailed_data.insurance_providers?.join(', ') || row[header] || '';
                }
                if (header === 'Booking Summary' && row.detailed_data) {
                    return row.detailed_data.booking_summary || row[header] || '';
                }
                return row[header] || '';
            });
            worksheet.addRow(rowData);
        });

        worksheet.columns.forEach(col => {
            let maxLength = 10;
            col.eachCell({ includeEmpty: true }, c => {
                const len = c.value ? c.value.toString().length : 10;
                if (len > maxLength) maxLength = len;
            });
            col.width = Math.min(maxLength, 50);
        });

        // Ensure exports directory exists
        await fs.mkdir(path.join(__dirname, 'exports'), { recursive: true });

        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const filename = `rula_providers_${timestamp}.xlsx`;
        const filepath = path.join(__dirname, 'exports', filename);

        await workbook.xlsx.writeFile(filepath);
        console.log(`✅ Excel file saved as ${filename}`);

        return filepath;
    } catch (error) {
        console.error('❌ Failed to create Excel file:', error);
        throw error;
    }
}

async function exportStatistics(db) {
    try {
        const stats = {
            total_providers: await db.collection('providers').countDocuments(),
            detailed_scraped: await db.collection('providers').countDocuments({ detailed_scraped: true }),
            basic_only: await db.collection('providers').countDocuments({ detailed_scraped: { $ne: true } }),
            by_state: await db.collection('providers').aggregate([
                { $group: { _id: '$state', count: { $sum: 1 } } },
                { $sort: { count: -1 } }
            ]).toArray(),
            scraping_logs: await db.collection('scraping_logs').countDocuments(),
            last_scraped: await db.collection('providers').find().sort({ last_scraped: -1 }).limit(1).toArray()
        };

        console.log('\n📊 Export Statistics:');
        console.log(`Total Providers: ${stats.total_providers}`);
        console.log(`Detailed Scraped: ${stats.detailed_scraped}`);
        console.log(`Basic Only: ${stats.basic_only}`);
        console.log('\n📈 By State:');
        stats.by_state.forEach(state => {
            console.log(`  ${state._id}: ${state.count}`);
        });
        console.log(`\n📝 Scraping Logs: ${stats.scraping_logs}`);

        if (stats.last_scraped.length > 0) {
            console.log(`🕒 Last Scraped: ${stats.last_scraped[0].last_scraped}`);
        }

        return stats;
    } catch (error) {
        console.error('❌ Failed to generate statistics:', error);
    }
}

(async () => {
    let client;

    try {
        console.log('Starting MongoDB export...');

        const { client: mongoClient, db } = await connectToMongo();
        client = mongoClient;

        // Get all providers with detailed data preferred
        const providers = await db.collection('providers')
            .find({})
            .sort({ detailed_scraped: -1, last_updated: -1 })
            .toArray();

        console.log(`📋 Found ${providers.length} providers to export`);

        if (providers.length === 0) {
            console.log('❌ No providers found to export');
            process.exit(1);
        }

        // Export statistics
        await exportStatistics(db);

        // Create Excel file
        const filepath = await createExcelFile(providers);

        console.log(`✅ Export completed successfully!`);
        console.log(`📁 File: ${filepath}`);

    } catch (error) {
        console.error('❌ Export failed:', error);
        process.exit(1);
    } finally {
        if (client) {
            await client.close();
        }
    }
})();
