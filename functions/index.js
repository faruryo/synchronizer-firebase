// The Cloud Functions for Firebase SDK to create Cloud Functions and setup triggers.
const functions = require('firebase-functions');

// The Firebase Admin SDK to access the Firebase Realtime Database.
const admin = require('firebase-admin');
admin.initializeApp();


exports.startNyankoBatch = functions.https.onRequest((req, res) => {

    var db = admin.firestore();
    const settings = { timestampsInSnapshots: true };
    db.settings(settings);

    const batch = require('./nyanko-batch.js');
    batch.execNyankoBatch(db)


    return res.send('ok');
});