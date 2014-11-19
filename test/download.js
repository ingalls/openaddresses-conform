var test = require('tape'),
    conform = require('../index.js'),
    fs = require('fs'),
    rimraf = require('rimraf'),
    debug = require('debug'),
    async = require('async');

test('Download/unzip test', function(t) {
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
        debug('downloading us-wa-snohmish-test');
        conform.downloadCache(conform.loadSource('./test/fixtures/us-wa-snohmish-test.json'), cachedir, function(){        
            debug('us-wa-snohmish-test.json download completed');
            t.assert(fs.existsSync(cachedir + '/us-wa-snohmish-test.zip'), 'Zipfile downloaded successfully');
            t.assert(['./test/tmp/us-wa-snohmish-test/us-wa-snohmish-test.dbf',
            './test/tmp/us-wa-snohmish-test/us-wa-snohmish-test.prj',
            './test/tmp/us-wa-snohmish-test/us-wa-snohmish-test.shp',
            './test/tmp/us-wa-snohmish-test/us-wa-snohmish-test.shx',
            './test/tmp/us-wa-snohmish-test/us-wa-snohmish-test.xml'].every(function(f){
                return fs.existsSync(f);
            }), 'us-wa-snohmish-test shapefile components extracted successfully');
            callback();
        });
    });

    // unzipped geojson
    testSeries.push(function(callback) {
        debug('downloading us-wy-albany-test');
        conform.downloadCache(conform.loadSource('./test/fixtures/us-wy-albany-test.json'), cachedir, function() {
            debug('us-wy-albany-test download completed');            
            t.assert(fs.existsSync(cachedir + '/us-wy-albany-test.json'), 'us-wy-albany-test JSON downloaded successfully');
            callback();
        });
    });

    // zipped CSV
    testSeries.push(function(callback) {
        debug('downloading us-or-portland-test');
        conform.downloadCache(conform.loadSource('./test/fixtures/us-or-portland-test.json'), cachedir, function() {
            debug('us-or-portland-test download completed');
            t.assert(fs.existsSync(cachedir + '/us-or-portland-test.zip'), 'us-or-portland-test zipfile downloaded successfully');            
            t.assert(fs.existsSync(cachedir + '/us-or-portland-test.csv'), 'us-or-portland-test CSV downloaded successfully');
            callback();
        });
    });

    async.series(testSeries, function() {
        // cleanup
        rimraf(cachedir);
    });
});