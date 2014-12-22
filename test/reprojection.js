var test = require('tape'),
    conform = require('../index.js'),
    fs = require('fs'),
    path = require('path'),
    rimraf = require('rimraf'),
    async = require('async'),
    md5 = require('MD5'),
    parse = require('csv-parse'),
    transform = require('stream-transform');


function validReprojectionSample(source, cachedir, callback) {
    var readStream = fs.createReadStream(path.normalize(cachedir + '/' + source[0] + '/out.csv'));
    
    var correctCoordinates = [];
    var correctCoordinateStream = fs.createReadStream('./test/fixtures/reprojection-correct.csv');        
    var parser = parse({ relax: true });
    var parser2 = parse({ relax: true });

    var correctCollector = transform(function(data) {
        if(rows > 0)
            correctCoordinates.push([(Math.floor(parseFloat(data[0]) * 10000) * 0.00001), (Math.floor(parseFloat(data[1]) * 10000) * 0.00001)]);
        rows++;
    });

    var tester = transform(function(data) {
        if (rows > 0) {
            var lon = Math.floor(parseFloat(data[0]) * 10000) * 0.00001;
            var lat = Math.floor(parseFloat(data[1]) * 10000) * 0.00001;
            if ((lon !== correctCoordinates[rows-1][0]) || (lat !== correctCoordinates[rows-1][1])) {
                return callback(false);
            }
        }
        rows++;
    });

    var rows = 0;
    correctCoordinateStream
        .pipe(parser)
        .pipe(correctCollector)
        .on('finish', function() {
            rows = 0;
            readStream
                .pipe(parser2)
                .pipe(tester)
                .on('finish', function() {
                    callback(true);
                });            
        });
}


test('Reprojection test', function(t) {
    require('debug').enable('conform:*');
    var debug = require('debug')('conform:test:encoding');

    t.plan(2);

    // make a working directory
    var cachedir = './test/tmp/';
    rimraf.sync(cachedir);    
    fs.mkdirSync(cachedir);   

    conform.main(['./test/fixtures/kr-seoul-yongsangu-test.json'], cachedir, function(err, result) {
        t.ok( (err === null), 'kr-seoul-yongsangu-test.json loaded without errors');
        validReprojectionSample(['kr-seoul-yongsangu-test'], cachedir, function(result) {
            t.ok(result, 'Reprojected values match expected values to 5 decimal places');
        });
        rimraf.sync('./test/tmp');
    });
      
});