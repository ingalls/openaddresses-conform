var test = require('tape'),
    conform = require('../index.js'),
    fs = require('fs'),
    rimraf = require('rimraf'),
    debug = require('debug');

test('Download test', function(t) {
    debug.enable('conform:*');

    t.plan(1);

    var cachedir = './test/tmp';
    rimraf.sync(cachedir);    
    fs.mkdirSync(cachedir);

    var source = conform.loadSource('./test/fixtures/us-wa-snohmish.json');

    conform.downloadCache(source, cachedir, function(){
        debug('Download complete');
        t.assert(fs.existsSync(cachedir + '/us-wa-snohmish.zip'), 'File downloaded successfully');
    });
});