var test = require('tape'),
    encoding = require('../Tools/encoding.js'),
    fs = require('fs'),
    path = require('path');
    rimraf = require('rimraf'),
    async = require('async'),
    md5 = require('MD5');

test('Encoding test', function(t) {
    require('debug').enable('conform:*');
    var debug = require('debug')('conform:test:encoding');

    t.plan(1);

    // make a working directory
    var cachedir = './test/tmp/';
    rimraf.sync(cachedir);    
    fs.mkdirSync(cachedir);   

    // make a copy of the EUCKR-encoded file
    var read = fs.createReadStream('./test/fixtures/euckr-sample.csv');
    var write = fs.createWriteStream('./test/tmp/euckr-sample.csv');
    
    write.on('finish', function(){

        var source = {
            id: 'euckr-sample',
            conform: {
                type: 'csv',
                encoding: 'EUCKR'
            }
        };

        encoding.utf8(source, './test/tmp/', function(){
            var md5a = md5(fs.readFileSync('./test/tmp/euckr-sample.csv'));
            var md5b = md5(fs.readFileSync('./test/fixtures/utf8-sample.csv'));
            t.ok( (md5a === md5b), 'Converted EUCKR-encoded file matches UTF-8 reference');
            rimraf.sync('./test/tmp');
        });

    });
    
    read.pipe(write);        
});