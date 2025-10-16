import fs from 'fs';
import path from 'path';

const dataDir = './data';
const outputFile = './collected.json';

let allData = [];
let fileCount = 0;
let totalSize = 0;

fs.readdirSync(dataDir).forEach(file => {
    const fullPath = path.join(dataDir, file);
    if (fs.statSync(fullPath).isFile() && file.endsWith('.json')) {
        const content = JSON.parse(fs.readFileSync(fullPath, 'utf-8'));
        allData.push(content);
        fileCount++;
        totalSize += fs.statSync(fullPath).size;
    }
});

fs.writeFileSync(outputFile, JSON.stringify(allData, null, 2));
console.log({
    totalFiles: fileCount,
    totalSizeKB: (totalSize / 1024).toFixed(2),
    outputFile
});
