var test = require('tape'),
    conform = require('../index.js'),
    fs = require('fs'),
    path = require('path');
    rimraf = require('rimraf'),
    debug = require('debug'),
    async = require('async'),
    transform = require('stream-transform'),
    parse = require('csv-parse');

function _countRows(source, cachedir, callback) {
    var readStream = fs.createReadStream(path.normalize(cachedir + '/' + source[0] + '/out.csv'));
    var parser = parse({ relax: true });
    var rows = 0;
    var validRows = 0;
    var lonlatRegex = new RegExp(/[^0123456789\-\.]/);

    var tester = transform(function(data) {
        // trim whitespace from all fields
        data = data.map(function(x) { return x.trim(); });

        // tests for four columns:
        // 0: lon: numeric lat/lon [^0-9\-\.]
        // 1: lat: numeric lat/lon [^0-9\-\.]
        // 2: (house) number: non-empty
        // 3: street: non-empty
        if ((!lonlatRegex.test(data[0])) && 
        (!lonlatRegex.test(data[1])) && 
        (data[2].length > 0) && 
        (data[3].length > 0)) {
            validRows++;
        }

        rows++;
    });

    readStream
        .pipe(parser)
        .pipe(tester)
        .on('finish', function() {
            callback(null, { rows: rows, validRows: validRows });
        });
}

test('Conform test', function(t) {
    debug.enable('conform:*');

    var cachedir = './test/tmp/';
    rimraf.sync(cachedir);    
    fs.mkdirSync(cachedir);   

    sourcesAndExpectedRows = [
        // ['us-wy-albany-test', 19357, 15562], // disabled due to ESRI bug
        ['us-or-portland-test', 415220, 415217],
        ['us-va-james_city-test', 32352, 32125],
        ['us-wa-snohmish-test', 268211, 268177]
    ];

    t.plan(sourcesAndExpectedRows.length * 3);

    // async is arguably overkill, but if we don't use it the progress meter
    // looks crazy
    var testSeries = [];
    sourcesAndExpectedRows.forEach(function(source) {
        testSeries.push(function(tscallback) {
            conform.main(['./test/fixtures/' + source[0] + '.json'], cachedir, function(err, result) {
                t.assert( (err === null), source[0] + ' loaded without errors');
                _countRows(source, cachedir, function(err, result) {
                    t.assert(source[1] === result.rows, source[0] + '/out.csv contains expected number of rows (' + result.rows + '/' + source[1] + ')');
                    t.assert(source[2] === result.validRows, source[0] + '/out.csv contains expected number of valid rows (' + result.validRows + '/' + source[2] + ')');                
                    tscallback();
                });
            });
        });
    });

    async.series(testSeries, function() {
        // cleanup
        if(fs.existsSync(cachedir)) rimraf.sync(cachedir);        
    });
});