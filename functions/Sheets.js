const { google } = require('googleapis');
const sheets = google.sheets('v4');

module.exports = class Sheets {

  async authorize(keyFile) {
    this.auth = await google.auth.getClient({
      keyFile: keyFile,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
  }

  async get(spreadsheetId, range) {
    const auth = this.auth;

    const apiOptions = {
      auth,
      spreadsheetId,
      range,
    };

    const res = await sheets.spreadsheets.values.get(apiOptions);

    return res.data.values;
  }

}
