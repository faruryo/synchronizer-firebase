
const conf = require('config')
const Sheets = require('./Sheets.js');
const path = require('path');

function convert_type(type, value) {
  if (type === "str") return value
  else if (type === "int") return parseInt(value)
  else if (type === "float") return parseFloat(value)
  else return value
}


exports.execNyankoBatch = execNyankoBatch;
async function execNyankoBatch(firestore) {
  let sheets = new Sheets();

  // Authentication Google API
  await sheets.authorize(path.join(__dirname, conf.nyanko.sheets.gsServiceAccount)).catch(console.error);

  const listOfAsyncJobs = [];
  listOfAsyncJobs.push(importDataFromSheetsToFirestore(sheets, firestore));

  listOfAsyncJobs.push(importMetadataFromSheetsToFirestore(sheets, firestore));

  return Promise.all(listOfAsyncJobs);
}

async function importDataFromSheetsToFirestore(sheets, firestore) {
  let collection = firestore.collection(conf.nyanko.firebase.dataCollectionName)

  // sheetsから実データを取得する
  const data_values
    = await sheets.get(conf.nyanko.sheets.sheetId, conf.nyanko.sheets.dataRange).catch(console.error);

  // sheetsからHeader物理名を取得する
  const header_physical_name_values
    = await sheets.get(conf.nyanko.sheets.sheetId, conf.nyanko.sheets.headerPhysicalNameRange).catch(console.error);

  // sheetsからHeaderTypeを取得する
  const header_type_values
    = await sheets.get(conf.nyanko.sheets.sheetId, conf.nyanko.sheets.headerTypeRange).catch(console.error);

  // sheetsのデータを変換しfirestoreに格納する
  data_values.forEach(row => {
    let row_dic = {}
    // 1行のデータを辞書型に変換する
    for (let i = 0, len = row.length; i < len; i++) {
      const header_name = header_physical_name_values[0][i];
      const header_type = header_type_values[0][i];
      const value = row[i];

      // headerをkey, cellの値をvalueにして辞書を作成する
      if (header_name && header_type && value) {
        row_dic[header_name] = convert_type(header_type, value);
      }
    }
    // idをdoc名としてfirestoreに格納する
    if (row_dic['id']) {
      collection.doc(row_dic['id']).set(row_dic);
    }
  });

  console.log("importDataFromSheetsToFirestore:End");
}

async function importMetadataFromSheetsToFirestore(sheets, firestore) {
  let collection = firestore.collection(conf.nyanko.firebase.metadataCollectionName)

  // sheetsからHeader物理名を取得する
  const header_physical_name_values
    = await sheets.get(conf.nyanko.sheets.sheetId, conf.nyanko.sheets.headerPhysicalNameRange).catch(console.error);

  // sheetsからHeader論理名を取得する
  const header_logical_name_values
    = await sheets.get(conf.nyanko.sheets.sheetId, conf.nyanko.sheets.headerLogicalNameRange).catch(console.error);

  // sheetsからHeaderTypeを取得する
  const header_type_values
    = await sheets.get(conf.nyanko.sheets.sheetId, conf.nyanko.sheets.headerTypeRange).catch(console.error);

  // 論理名を書き込む
  let header_logical_name_row_dic = {}
  for (let i = 0, len = header_physical_name_values[0].length; i < len; i++) {
    const header_name = header_physical_name_values[0][i];
    const value = header_logical_name_values[0][i];

    if (header_name && value) {
      header_logical_name_row_dic[header_name] = header_logical_name_values[0][i];
    }
  }
  collection.doc('headerLogicalName').set(header_logical_name_row_dic).catch(console.error);

  // タイプを書き込む
  let header_type_row_dic = {}
  for (let i = 0, len = header_physical_name_values[0].length; i < len; i++) {
    const header_name = header_physical_name_values[0][i];
    const value = header_type_values[0][i];

    if (header_name && value) {
      header_type_row_dic[header_name] = header_type_values[0][i];
    }
  }
  collection.doc('headerType').set(header_type_row_dic).catch(console.error);

  console.log("importMetadataFromSheetsToFirestore:End");
}
