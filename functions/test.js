
const path = require('path');
const conf = require('config')
const Sheets = require('./Sheets.js');


execAPI().catch(console.error);

async function execAPI() {
    const sheets = new Sheets();
    await sheets.authorize(path.join(__dirname, conf.gsServiceAccount)).catch(console.error);
    data = await sheets.get(conf.sheetId, conf.dataRange).catch(console.error);

    console.log(data);
}