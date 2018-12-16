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

  const canRun = await canExecNyankoBatch(admin);

  if(!canRun) {
    console.log("execNyankoBatch:Bad Condition");
    console.log("execNyankoBatch:End");
    return false;
  }

  let sheets = new Sheets();
  // Authentication Google API
  await sheets.authorize(path.join(__dirname, conf.nyanko.sheets.gsServiceAccount)).catch(console.error);

  const nyanko_data = await getNyankoDataFromSheets(sheets);

  let listOfAsyncJobs = [];
  listOfAsyncJobs.push(importNyankoToFirestore(nyanko_data, admin));
  listOfAsyncJobs.push(importMetaNyankoToFirestore(nyanko_data, admin));

  let result = await Promise.all(listOfAsyncJobs).then(results => {
    return true;
  }).catch(reject => {
    console.error(reject);
    return false;
  });

  console.log("execNyankoBatch:End");
  return result;
}

exports.execNyankoBatchById = execNyankoBatchById;
async function execNyankoBatchById(admin, ids) {
  console.log("execNyankoBatchById:Start");

  let sheets = new Sheets();
  // Authentication Google API
  await sheets.authorize(path.join(__dirname, conf.nyanko.sheets.gsServiceAccount)).catch(console.error);

  const nyanko_data = await getNyankoDataFromSheets(sheets);

  let listOfAsyncJobs = [];
  listOfAsyncJobs.push(importNyankoToFirestoreById(nyanko_data, admin, ids));

  let result = await Promise.all(listOfAsyncJobs).then(results => {
    return true;
  }).catch(reject => {
    console.error(reject);
    return false;
  });

  console.log("execNyankoBatchById:End");
  return result;
}

async function canExecNyankoBatch(admin) {
  console.log("canExecNyankoBatch:Start");

  const nowSecond = admin.firestore.Timestamp.now().seconds;

  let collection = admin.firestore().collection(conf.nyanko.firebase.dataCollectionName)
  let maxUpdatedAtQuerySnapshot = await collection.orderBy('updatedAt', 'desc').limit(1).get();
  const maxUpdatedAtSecond = maxUpdatedAtQuerySnapshot.docs[0].data().updatedAt.seconds;

  const can = nowSecond - 60 > maxUpdatedAtSecond;
  console.log(nowSecond + " - 60 > " + maxUpdatedAtSecond + " = " + can);
  
  console.log("canExecNyankoBatch:End");
  return can;
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
  let export_data = convertNyankodataToFirestoredata(nyanko_data, timestamp);

  return importToFirestore(firestore, conf.nyanko.firebase.dataCollectionName, export_data);
}

function importNyankoToFirestoreById(nyanko_data, admin, ids) {

  let firestore = admin.firestore();
  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  let all_data = convertNyankodataToFirestoredata(nyanko_data, timestamp);

  // Idsのデータのみ抽出する
  const export_data = {};
  for(let id of ids) {
    if(all_data[id]) {
      export_data[id] = all_data[id];
    }
  }
  console.debug(ids + " => " + Object.keys(export_data).length + " data were found");

  if(Object.keys(export_data).length === 0) {
    return Promise.reject(new Error('Not found by specified Ids'));
  }

  return importToFirestore(firestore, conf.nyanko.firebase.dataCollectionName, export_data);
}

/**
 * sheetsから取り出したNyankoデータをFirestoreに投入する連想配列の形式に変換する
 * @param {*} nyanko_data 
 * @param {*} timestamp 
 */
function convertNyankodataToFirestoredata(nyanko_data, timestamp) {

  let all_data = {};

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
    // idをdoc名として連想配列に格納する
    if (row_dic['id']) {
      if (timestamp) {
        row_dic['updatedAt'] = timestamp;
      }
      all_data[row_dic['id']] = row_dic;
    }
  });

  return all_data;
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
  for (let i = 0, len = nyanko_data['header_physical_names'][0].length; i < len; i++) {
    const header_name = nyanko_data['header_physical_names'][0][i];
    const value = nyanko_data['header_logical_names'][0][i];

    if (header_name && value) {
      header_logical_name_row_dic[header_name] = value;
    }
  }
  header_logical_name_row_dic['updatedAt'] = timestamp;
  batch.set(collection.doc('headerLogicalName'), header_logical_name_row_dic);

  // タイプを書き込む
  let header_type_row_dic = {}
  for (let i = 0, len = nyanko_data['header_physical_names'][0].length; i < len; i++) {
    const header_name = nyanko_data['header_physical_names'][0][i];
    const value = nyanko_data['header_types'][0][i];

    if (header_name && value) {
      header_type_row_dic[header_name] = value;
    }
  }
  header_type_row_dic['updatedAt'] = timestamp;
  batch.set(collection.doc('headerType'), header_type_row_dic);

  return batch.commit();
}