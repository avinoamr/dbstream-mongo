var mongodb = require( "mongodb" );
var events = require( "events" );
var extend = require( "extend" );
var db = require( "dbstream" );
var util = require( "util" );

module.exports.mongodb = mongodb
module.exports.ObjectID = mongodb.ObjectID

util.inherits( Cursor, db.Cursor );
function Cursor( conn ) {
    Cursor.super_.call( this );
    this._conn = conn;
}

Cursor.prototype._save = function ( object, callback ) {
    this._conn.open( function ( err, collection ) {
        if ( err ) return callback( toError( err ) );
        
        var conn = this;
        var serialized = replace( {}, object );
        if ( typeof serialized.id != "undefined" ) {
            serialized._id = toObjectID( serialized.id );
            delete serialized.id;
        }

        collection.save( serialized, ondone );

        function ondone ( err, result ) {
            conn.done();
            if ( err ) return callback( toError( err ) );
            if ( typeof result == "object" ) {
                result.id = fromObjectID( result._id );
                delete result._id;
                replace( object, result );
            }
            callback();
        }

    });
};

Cursor.prototype._remove = function ( object, callback ) {
    if ( !object.id ) {
        var msg = "Unable to remove object without an ID";
        return callback( new Error( msg ) );
    }

    var id = toObjectID( object.id );
    this._conn.open( function ( err, collection ) {
        if ( err ) return callback( toError( err ) );
        var conn = this;
        collection.remove({ _id: id }, function ( err ) {
            conn.done();
            callback( toError( err ) );
        })
    });
};

Cursor.prototype._load = function () {
    if ( this._reading ) return;
    this._reading = true;

    var options = {};
    if ( this._limit ) options.limit = this._limit;
    if ( this._skip )  options.skip = this._skip;
    if ( this._sort )  options.sort = ( this._sort || [] ).map( function ( s ) {
        return [ s.key, s.direction ];
    });

    var query = replace( {}, this._query );
    if ( query.id ) {
        query._id = toObjectID( query.id );
        delete query.id;
    }

    var that = this;
    this._conn.open( function ( err, collection ) {
        if ( err ) return this.emit( "error", toError( err ) );
        var conn = this;
        collection
            .find( query, options )
            .stream()
            .on( "error", function ( err ) {
                conn.done();
                that.emit( "error", toError( err ) );
            })
            .on( "end", function () {
                conn.done();
                that.push( null );
                that._reading = false;
            })
            .on( "data", function ( obj ) {
                obj.id = fromObjectID( obj._id );
                delete obj._id;
                that.push( obj );
            });
    });
}

// dbstream API requires that objects be modified in-place, and not
// replaced completely with a new reference in order to allow users to access
// the modified object
function replace ( obj, other ) {
    var name;

    // remove non-existing keys
    for ( name in obj ) {
        if ( typeof other[ name ] == "undefined" ) {
            delete obj[ name ];
        }
    }

    // set new keys
    for ( name in other ) {
        obj[ name ] = other[ name ];
    }

    return obj;
}


var connections = {};

function closeDb( url ) {
    if ( !connections[ url ] || !connections[ url ].db ) {
        return;
    }

    connections[ url ].clients -= 1;
    if ( connections[ url ].clients > 0 ) {
        return; // still has other open clients
    }

    // already scheduled to be closed
    if ( connections[ url ].closeTimeout ) {
        return;
    }

    connections[ url ].closeTimeout = setTimeout( function () {
        connections[ url ].db.close()
        delete connections[ url ];
    }, 10000 );
}

function getDb ( url, options, callback ) {

    // not connected at all
    if ( !connections[ url ] ) {
        connections[ url ] = { callbacks: [ callback ], clients: 1 }
        mongodb.MongoClient.connect( url, options, function ( err, db ) {
            if ( !err && db ) {
                connections[ url ].db = db;
            }

            connections[ url ].callbacks.forEach( function ( callback ) {
                callback.call( null, toError( err ), db );
            })
        });
        return;
    }

    clearTimeout( connections[ url ].closeTimeout );
    delete connections[ url ].closeTimeout;

    connections[ url ].clients += 1;

    // already running, subscribe to get the connection callback
    if ( !connections[ url ].db ) {
        connections[ url ].callbacks.push( callback );
        return
    }

    // already connected
    callback( null, connections[ url ].db );
}

module.exports.connect = function( url, options ) {
    if ( !options.collection ) {
        throw new Error( "options.collection is required" );
    }

    var conn = new events.EventEmitter();
    var callbacks = [];
    conn.open = function ( callback ) {
        getDb( url, options, function ( err, db ) {
            callback.call( conn, err, err ? null : db.collection( options.collection ) );
        })
    }
    conn.done = function () {
        closeDb( url );
    }

    util.inherits( _Cursor, Cursor );
    function _Cursor() {
        _Cursor.super_.call( this, conn );
    }

    conn.Cursor = _Cursor;
    return conn;
}

function toObjectID( id ) {
    if (id.$in) {
        id.$in = id.$in.map(toObjectID)
        return id
    }

    if ( typeof id == "string" && id.length == 24 && mongodb.ObjectID.isValid( id ) ) {
        return mongodb.ObjectID( id );
    } else {
        return id;
    }
}

function fromObjectID( id ) {
    if ( id instanceof mongodb.ObjectID ) {
        return id.toString();
    } else {
        return id;
    }
}

function toError( err ) {
    if ( !err ) return;
    if ( err instanceof Error ) return err;

    // mongo sdk might return an object with the format { err: message }
    // instead of a proper Javascript Error instance
    return extend( new Error( err.message || err.err ), err );
}