var test = require( "dbstream/test" );
var mongodb = require( "mongodb" );
var stream = require( "stream" );
var events = require( "events" );
var assert = require( "assert" );
var db = require( "./mongo" );
var sift = require( "sift" );

var connect = mongodb.MongoClient.connect;

var options = { collection: "test" };
var addr = "mongodb://127.0.0.1:27017/test1";

describe( "Mongo", function() {

    beforeEach( function () {
        mongodb.MongoClient.connect = mock_connect;
    })

    after( function () {
        mongodb.MongoClient.connect = connect;
    })

    it( "Implements the dbstream API", test( db.connect( addr, options ) ) );

    it( "Supports multiple collections", function ( done ) {
        var addr = "mongodb://127.0.0.1:27017/test2";
        var conn1 = db.connect( addr, { collection: "test2" } )
        var conn2 = db.connect( addr, { collection: "test3" } )

        var data = [];
        new conn1.Cursor()
            .on( "error", done )
            .on( "finish", function () {
                new conn2.Cursor()
                    .on( "error", done )
                    .on( "data", data.push.bind( data ) )
                    .on( "end", function () {
                        assert.deepEqual( data, [] );
                        done();
                    })
                    .find({})
            })
            .end( { hello: "world" } )

    })

    it( "throws connection errors", function ( done ) {
        mongodb.MongoClient.connect = function ( url, options, callback ) {
            callback( { err: "Something went wrong" } );
        }

        var addr = "mongodb://127.0.0.1:27017/test4";
        var conn = db.connect( addr, { collection: "test4" } )

        var data = [];
        new conn.Cursor()
            .on( "error", function ( err ) {
                assert( err.message, "Something went wrong" )
                done();
            })
            .end( { hello: "world" } )
    })

    // ensure that all the connections were closed
    after( function ( done ) {
        this.timeout( 15000 );
        setTimeout( function () {
            var dbkeys = Object.keys( dbs );
            assert.equal( dbkeys.length, 0, "No all connections were closed: " + dbkeys )
            done();
        }, 11000 );
    })

});

var dbs = {};

function mock_connect ( url, options, callback ) {
    dbs[ url ] || ( dbs[ url ] = {} );
    process.nextTick(function() {
        var db = new events.EventEmitter();
        db.collection = function ( name ) {
            if ( !dbs[ url ][ name ] ) {
                dbs[ url ][ name ] = mock_collection();
            }
            return dbs[ url ][ name ];
        };
        db.close = function( callback ) {
            process.nextTick( function () {
                delete dbs[ url ];
                this.collection = function () {
                    throw new Error( "Db has been closed" );
                }
            }.bind( this ) );
        };

        callback( null, db );
    })
}

function mock_collection() {
    var data = [];
    return {
        save: function ( obj, callback ) {
            obj = copy( obj );
            if ( !obj._id ) {
                obj._id = ( Math.random() * 1e17 ).toString( 36 );
            }
            this.remove({ _id: obj._id }, function ( err, is_update ) {
                data.push( obj );
                process.nextTick(function(){
                    callback( null, ( is_update ) ? 1 : copy( obj ) );
                })
            });
        },
        remove: function( query, callback ) {
            // console.log( query, data );
            var sifter = sift( query );
            var removed = 0;
            data = data.filter(function ( obj ) {
                if ( !sifter.test( obj ) ) {
                    return true;
                } else {
                    removed += 1;
                    return false;
                }
            });
            process.nextTick(function () {
                callback( null, removed );
            });
        },
        find: function ( query, options ) {
            var sort = options.sort || [];
            var skip = options.skip || 0;
            var limit = options.limit || Infinity;
            var s = new stream.Readable({ objectMode: true });
            var results = sift( query, data );

            results.sort( function ( d1, d2 ) {
                for ( var s = 0 ; s < sort.length ; s += 1 ) {
                    s = sort[ s ];
                    if ( d1[ s[ 0 ] ] == d2[ s[ 0 ] ] ) continue;
                    return d1[ s[ 0 ] ] > d2[ s[ 0 ] ] 
                        ? s[ 1 ] : -s[ 1 ];
                }
                return 0;
            })
            results.splice( 0, skip )
            results.splice( limit );

            s._read = function () {
                if ( results.length == 0 ) return this.push( null );
                this.push( copy( results.shift() ) );
            }
            return {
                stream: function () {
                    return s;
                }
            }
        },
    }
}

function copy ( obj ) {
    return JSON.parse( JSON.stringify( obj ) );
}

