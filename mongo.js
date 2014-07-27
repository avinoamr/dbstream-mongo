var mongodb = require( "mongodb" );
var events = require( "events" );
var db = require( "dbstream" );
var util = require( "util" );

util.inherits( Cursor, db.Cursor );
function Cursor( conn ) {
    Cursor.super_.call( this );
    this._conn = conn;
}

Cursor.prototype._save = function ( object, callback ) {
    this._conn.open( function ( err, collection ) {
        if ( err ) return callback( err );

        var serialized = replace( {}, object );
        if ( typeof serialized.id != "undefined" ) {
            serialized._id = serialized.id;
            delete serialized.id;
        }

        function ondone ( err, result ) {
            if ( err ) return callback( err );

            // console.log( result == 1)
            if ( typeof result == "object" ) {
                result.id = result._id;
                delete result._id;
                replace( object, result );
            }
            callback();
        }

        collection.save( serialized, ondone );
    }, this );
};

Cursor.prototype._remove = function ( object, callback ) {
    if ( !object.id ) {
        var msg = "Unable to remove object without an ID";
        return callback( new Error( msg ) );
    }

    var id = object.id;
    this._conn.open( function ( err, collection ) {
        if ( err ) return callback( err );
        collection.remove({ _id: id }, function ( err ) {
            callback( err );
        })
    }, this );
};

Cursor.prototype._load = function () {
    if ( this._reading ) return;
    this._reading = true;

    var options = {};
    if ( this._limit ) options.limit = this._limit;
    if ( this._skip )  options.skip = this._skip;
    if ( this._sort )  options.sort = this._sort;

    var query = replace( {}, this._query );
    if ( query.id ) {
        query._id = query.id;
        delete query.id;
    }

    this._conn.open( function ( err, collection ) {
        if ( err ) return this.emit( "error", err );
        var that = this;
        collection
            .find( query, options )
            .stream()
            .on( "error", function ( err ) {
                that.emit( "error", err );
            })
            .on( "end", function () {
                that.push( null );
                that._reading = false;
            })
            .on( "data", function ( obj ) {
                obj.id = obj._id;
                delete obj._id;
                that.push( obj );
            });
    }, this );
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



module.exports.connect = function( url, options ) {
    if ( !options.collection ) {
        throw new Error( "options.collection is required" );
    }

    var collection, err, started = false;
    var conn = new events.EventEmitter();

    conn.open = function ( callback, ctx ) {
        if ( collection || err ) {
            return callback.call( ctx || this, err, collection )
        }

        conn.once( "connect", function() {
            callback.apply( ctx || this, arguments );
        });

        if ( started ) return;
        started = true;
        mongodb.MongoClient.connect( url, options, function ( _err, db ) {
            err = _err;
            if ( !err ) {
                collection = db.collection( options.collection );
            }
            conn.emit( "connect", err, collection );
        });
    }

    util.inherits( _Cursor, Cursor );
    function _Cursor() {
        _Cursor.super_.call( this, conn );
    }

    conn.Cursor = _Cursor;
    return conn;
}