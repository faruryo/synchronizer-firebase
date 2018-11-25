// The Cloud Functions for Firebase SDK to create Cloud Functions and setup triggers.
const functions = require('firebase-functions');

// The Firebase Admin SDK to access the Firebase Realtime Database.
const admin = require('firebase-admin');
admin.initializeApp();

var db = admin.firestore();
const settings = { timestampsInSnapshots: true };
db.settings(settings);

exports.startNyankoBatch = functions.https.onRequest(async (req, res) => {

    const batch = require('./nyanko-batch.js');

    try {
        await batch.execNyankoBatch(admin);
        response.send("ok");
    }
    catch (error) {
        console.log(error);
        response.satatus(500).send(error);
    }
});