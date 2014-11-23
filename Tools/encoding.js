var fs = require('fs'),
    Iconv = require('iconv').Iconv,
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

    var iconv = Iconv(source.conform.encoding, 'utf-8');

    instream.pipe(iconv).pipe(outstream);
}