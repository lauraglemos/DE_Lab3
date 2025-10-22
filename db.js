const mongoose = require('mongoose');
require('dotenv').config();


//mongoose.connect("mongodb://127.0.0.1:27017/cuadros");


function makeNewConnection(uri) {
    const db = mongoose.createConnection(uri, {
      
    });

    db.on('error', function (error) {
        console.log(`MongoDB :: connection ${this.name} ${JSON.stringify(error)}`);
        db.close().catch(() => console.log(`MongoDB :: failed to close connection ${this.name}`));
    });

    db.on('connected', function () {
        mongoose.set('debug', function (col, method, query, doc) {
            console.log(`MongoDB :: ${this.conn.name} ${col}.${method}(${JSON.stringify(query)},${JSON.stringify(doc)})`);
        });
        console.log(`MongoDB :: connected ${this.name}`);
    });

    db.on('disconnected', function () {
        console.log(`MongoDB :: disconnected ${this.name}`);
    });

    return db;
}

const imagesConnection = makeNewConnection('mongodb://127.0.0.1:27017/images');



module.exports = {
    imagesConnection,
    cuadrosConnection,
    comprasConnection,


};
