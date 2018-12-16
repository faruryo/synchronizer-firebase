// The Cloud Functions for Firebase SDK to create Cloud Functions and setup triggers.
const functions = require('firebase-functions');

// The Firebase Admin SDK to access the Firestore.
const admin = require('firebase-admin');
admin.initializeApp();

var db = admin.firestore();
const settings = { timestampsInSnapshots: true };
db.settings(settings);

exports.startNyankoBatch = functions.https.onRequest(async (request, response) => {

    const batch = require('./nyanko-batch.js');

    try {
        let result = false;
        if (request.method === "GET") {
            result = await batch.execNyankoBatch(admin);
        } else if (request.method === "PUT") {
            result = await batch.execNyankoBatchById(admin, request.body);
        }
        if(result) {
            response.send("ok");
        } else {
            response.send("ng");
        }
    }
    catch (error) {
        console.log(error);
        response.satatus(500).send(error);
    }
});