dbstream-mongo
==============

Mongo DB access layer compatible with the [dbstream](https://github.com/avinoamr/dbstream) API

### Usage

```javascript
var db = require( "dbstream-mongo" );
var connection = db.connect( "mongodb://127.0.0.1:27017/test", { collection: "test" } );

// write or update
var cursor = new connection.Cursor()
cursor.write({ id: 1, name: "Hello" });
cursor.write({ id: 2, name: "World" });
cursor.on("finish", function() {
  console.log("Saved 2 objects");
});
cursor.end();

// read
new connection.Cursor()
  .find({ id: 2 })
  .limit( 1 )
  .on("data", function (obj) {
    console.log("Loaded", obj);
  });
```

### API

This module implements the [dbstream](https://github.com/avinoamr/dbstream) API. For the complete documention see: https://github.com/avinoamr/dbstream

###### connect( url, options )

* `url` a mongodb connection url string
* `options` standard mongodb connection options, plus a `collection` name key
* Returns a dbstream [Connection](https://github.com/avinoamr/dbstream#connection) object

Creates a MongoDB connection. Uses the [MongoClient.connect](http://mongodb.github.io/node-mongodb-native/api-generated/mongoclient.html#mongoclient-connect) arguments.


