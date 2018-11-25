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
async function execNyankoBatch(admin) {

  console.log("execNyankoBatch:Start");

  let sheets = new Sheets();
  // Authentication Google API
  await sheets.authorize(path.join(__dirname, conf.nyanko.sheets.gsServiceAccount)).catch(console.error);

  const nyanko_data = await getNyankoDataFromSheets(sheets);

  let listOfAsyncJobs = [];
  listOfAsyncJobs.push(importNyankoToFirestore(nyanko_data, admin));
  listOfAsyncJobs.push(importMetaNyankoToFirestore(nyanko_data, admin));

  await Promise.all(listOfAsyncJobs);

  return console.log("execNyankoBatch:End");
}

async function getNyankoDataFromSheets(sheets) {
  console.log("getNyankoDataFromSheets:Start");

  const ranges = [{
      name: "data",
      sheetId: conf.nyanko.sheets.sheetId,
      range: conf.nyanko.sheets.dataRange
    },
    {
      name: "header_logical_names",
      sheetId: conf.nyanko.sheets.sheetId,
      range: conf.nyanko.sheets.headerLogicalNameRange
    },
    {
      name: "header_physical_names",
      sheetId: conf.nyanko.sheets.sheetId,
      range: conf.nyanko.sheets.headerPhysicalNameRange
    },
    {
      name: "header_types",
      sheetId: conf.nyanko.sheets.sheetId,
      range: conf.nyanko.sheets.headerTypeRange
    },
  ]

  const data_list = await getSheetsRanges(sheets, ranges);

  let data = {}
  for (let i in ranges) {
    data[ranges[i].name] = data_list[i]
  }

  console.log("getNyankoDataFromSheets:End");

  return Promise.resolve(data);
}

function getSheetsRanges(sheets, ranges) {
  let asyncJobs = [];
  for (let range of ranges) {
    asyncJobs.push(sheets.get(range.sheetId, range.range));
  }

  return Promise.all(asyncJobs);
}

function importNyankoToFirestore(nyanko_data, admin) {

  let firestore = admin.firestore();
  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  let export_data = {};

  // sheetsのデータを変換しfirestoreに格納する
  nyanko_data['data'].forEach(row => {
    let row_dic = {}
    // 1行のデータを辞書型に変換する
    for (let i = 0, len = row.length; i < len; i++) {
      const header_name = nyanko_data['header_physical_names'][0][i];
      const header_type = nyanko_data['header_types'][0][i];
      const value = row[i];

      // headerをkey, cellの値をvalueにして辞書を作成する
      if (header_name && header_type && value) {
        row_dic[header_name] = convert_type(header_type, value);
      }
    }
    // idをdoc名としてfirestoreに格納する
    if (row_dic['id']) {
      row_dic['update_timestamp'] = timestamp;
      export_data[row_dic['id']] =row_dic;
    }
  });

  return importToFirestore(firestore, conf.nyanko.firebase.dataCollectionName, export_data);
}

function importToFirestore(firestore, collectionName, data) {
  let batch = firestore.batch();
  let collection = firestore.collection(collectionName)

  const BATCH_MAX = 450;

  let count = 0;
  let listOfAsyncJobs = [];
  for(let key of Object.keys(data)) {
    batch.set(collection.doc(key), data[key]);
    count++;
    if(count > BATCH_MAX) {
      console.log("importToFirestore:commit");
      listOfAsyncJobs.push(batch.commit());
      batch = firestore.batch();
      count = 0;
    }
  }
  if(count !== 0) {
    listOfAsyncJobs.push(batch.commit());
  }

  return Promise.all(listOfAsyncJobs);
}

function importMetaNyankoToFirestore(nyanko_data, admin) {
  console.log("importMetaNyankoToFirestore:Start");

  let firestore = admin.firestore();
  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  let collection = firestore.collection(conf.nyanko.firebase.metadataCollectionName)
  let batch = firestore.batch();

  // 論理名を書き込む
  let header_logical_name_row_dic = {}
  for (let i = 0, len = nyanko_data['header_physical_names'].length; i < len; i++) {
    const header_name = nyanko_data['header_physical_names'][i];
    const value = nyanko_data['header_logical_names'][i];

    if (header_name && value) {
      header_logical_name_row_dic[header_name] = nyanko_data['header_logical_names'][i];
    }
  }
  header_logical_name_row_dic['update_timestamp'] = timestamp;
  batch.set(collection.doc('headerLogicalName'), header_logical_name_row_dic);

  // タイプを書き込む
  let header_type_row_dic = {}
  for (let i = 0, len = nyanko_data['header_physical_names'].length; i < len; i++) {
    const header_name = nyanko_data['header_physical_names'][i];
    const value = nyanko_data['header_types'][i];

    if (header_name && value) {
      header_type_row_dic[header_name] = nyanko_data['header_types'][i];
    }
  }
  header_type_row_dic['update_timestamp'] = timestamp;
  batch.set(collection.doc('headerType'), header_type_row_dic);

  return batch.commit();
}