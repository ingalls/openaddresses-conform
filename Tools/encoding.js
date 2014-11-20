var fs = require('fs'),
    iconv = require('iconv-lite'),
    conform = require('../index.js');

module.exports.utf8 = function(source, cachedir, callback) {
    var debug = require('debug')('conform:utf8');
    debug('Changing encoding from ' + source.conform.encoding);

    var loc = cachedir + source.id + '.' + conform.fileTypeExtensions[source.conform.type];

    var instream = fs.createReadStream(loc);
    var outstream = fs.createWriteStream('./tmp.iconv');

    outstream.on('close', function() {
        fs.rename('./tmp.iconv', loc, function(err) {
            callback(err);
        });
    });

    instream.pipe(iconv.decodeStream(source.conform.encoding)).pipe(outstream);
}