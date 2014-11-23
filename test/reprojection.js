var test = require('tape'),
    conform = require('../index.js'),
    fs = require('fs'),
    path = require('path'),
    rimraf = require('rimraf'),
    async = require('async'),
    md5 = require('MD5');

test('Reprojection test', function(t) {
    require('debug').enable('conform:*');
    var debug = require('debug')('conform:test:encoding');

    var magicMD5 = 'a562fa33bdce429e7269d1439ba202c4';

    t.plan(2);

    // make a working directory
    var cachedir = './test/tmp/';
    rimraf.sync(cachedir);    
    fs.mkdirSync(cachedir);   

    conform.main(['./test/fixtures/kr-seoul-yongsangu-test.json'], cachedir, function(err, result) {
        t.ok( (err === null), 'kr-seoul-yongsangu-test.json loaded without errors');
        var md5a = md5(fs.readFileSync('./test/tmp/kr-seoul-yongsangu-test/out.csv'));
        t.ok( (md5a === magicMD5), 'Output file fingerprint matches (' + magicMD5 + ')');
        rimraf.sync('./test/tmp');
    });
      
});