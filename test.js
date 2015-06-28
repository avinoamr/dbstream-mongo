var test = require( "dbstream/test" );
var mongodb = require( "mongodb" );
var stream = require( "stream" );
var db = require( "./mongo" );
var sift = require( "sift" );

mongodb.MongoClient.connect = mock_connect;

var options = { collection: "test" };
var connection = db.connect( "mongodb://127.0.0.1:27017/test", options );

describe( "Mongo", function() {

    it( "Implements the dbstream API", test( connection ) );

});

var dbs = {};
function mock_connect ( url, options, callback ) {
    dbs[ url ] || ( dbs[ url ] = {} );
    process.nextTick(function() {
        callback( null, {
            collection: function ( name ) {
                if ( !dbs[ url ][ name ] ) {
                    dbs[ url ][ name ] = mock_collection();
                }
                return dbs[ url ][ name ];
            }
        })
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

