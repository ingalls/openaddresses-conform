var test = require('tape'),
    conform = require('../index.js'),
    fs = require('fs'),
    rimraf = require('rimraf'),
    debug = require('debug'),
    async = require('async');

test('Download test', function(t) {
    debug.enable('conform:*');

    t.plan(5);

    var cachedir = './test/tmp';
    rimraf.sync(cachedir);    
    fs.mkdirSync(cachedir);    

    // async is arguably overkill, but if we don't use it the progress meter
    // looks crazy
    var testSeries = [];

    // zipped shapefile
    testSeries.push(function(callback) {
        debug('downloading us-wa-snohmish');
        conform.downloadCache(conform.loadSource('./test/fixtures/us-wa-snohmish.json'), cachedir, function(){        
            debug('us-wa-snohmish.json download completed');
            t.assert(fs.existsSync(cachedir + '/us-wa-snohmish.zip'), 'Zipfile downloaded successfully');
            t.assert(['./test/tmp/us-wa-snohmish/us-wa-snohmish.dbf',
            './test/tmp/us-wa-snohmish/us-wa-snohmish.prj',
            './test/tmp/us-wa-snohmish/us-wa-snohmish.shp',
            './test/tmp/us-wa-snohmish/us-wa-snohmish.shx',
            './test/tmp/us-wa-snohmish/us-wa-snohmish.xml'].every(function(f){
                return fs.existsSync(f);
            }), 'us-wa-snohmish shapefile components extracted successfully');
            callback();
        });
    });

    // unzipped geojson
    testSeries.push(function(callback) {
        debug('downloading us-wy-albany');
        conform.downloadCache(conform.loadSource('./test/fixtures/us-wy-albany.json'), cachedir, function() {
            debug('us-wy-albany download completed');            
            t.assert(fs.existsSync(cachedir + '/us-wy-albany.json'), 'us-wy-albany JSON downloaded successfully');
            callback();
        });
    });

    // zipped CSV
    testSeries.push(function(callback) {
        debug('downloading us-or-portland');
        conform.downloadCache(conform.loadSource('./test/fixtures/us-or-portland.json'), cachedir, function() {
            debug('us-or-portland download completed');
            t.assert(fs.existsSync(cachedir + '/us-or-portland.zip'), 'us-or-portland zipfile downloaded successfully');            
            t.assert(fs.existsSync(cachedir + '/us-or-portland.csv'), 'us-or-portland CSV downloaded successfully');
            callback();
        });
    });

    async.series(testSeries);
});