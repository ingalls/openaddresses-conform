#!/usr/bin/env node
/*jslint indent: 4, node: true */

var argv = require('minimist')(process.argv.slice(2)),
    fs = require('fs'),
    path = require('path'),
    ProgressBar = require('progress'),
    unzip = require('unzip'),
    request = require('request'),
    AWS = require('aws-sdk'),    
    async = require('async');


var fileTypeExtensions = {
    'shapefile': 'shp',
    'shapefile-polygon': 'shp',
    'geojson': 'json',
    'csv': 'csv'
};

var uploadToAWS = false;

function _isShapefileExtension(ext) {
    // ensure ext begins with a . ('.txt' not 'txt') 
    if(ext[0]!=='.') ext = '.' + ext;    
    var acceptableExtensions = ['.shp', '.shx', '.dbf', '.prj', '.sbn', '.sbx', '.fbn', '.fbx', '.ain', '.aih', '.ixs', '.mxs', '.atx', '.xml', '.cpg', '.qix', '.shp.xml'];
    return acceptableExtensions.some(function(v) {
        return ext.match(v + '$');
    });
}

function _isFlatFileExtension(ext) {
    // ensure ext begins with a . ('.txt' not 'txt') 
    if(ext[0]!=='.') ext = '.' + ext;
    return ['.json', '.csv', '.geojson'].some(function(v) { 
        return ext.match(v + '$');
    });
}

function cachedFileLocation(source, cachedir){
    if (cachedir[cachedir.length-1] !== '/') cachedir += '/';
    
    if(source.compression)
        return cachedir + source.id + '.' + source.compression;
    else if(['shapefile', 'shapefile-polygon'].indexOf(source.conform.type) > -1)
        return cachedir + source.id + '/' + source.id + '.' + fileTypeExtensions[source.conform.type];
    else
        return cachedir + source.id + '.' + fileTypeExtensions[source.conform.type];
}

function ConformCLI(){
    require('debug').enable('conform:*');

    //Command Line Args
    var sourcedir = argv._[0],
        cachedir = argv._[1],
        bucketName = undefined,
        uploadToAWS = false;

    if (argv._.length == 3)
    {
        uploadToAWS = true;
        bucketName = (argv._[2] == 'aws' ? 'openaddresses' : argv._[2]);
    }

    var cacheIndex = 0,
        source = null,
        parsed;

    if (!sourcedir || !cachedir) {
        console.log('usage: openaddresses-conform <path-to-sources> <path-to-cache> <options>');
        console.log('       openaddresses-conform  <single source>  <path-to-cache> <options>');
        console.log('\nOptions:');
        console.log('bucket name - If credentials are found automatically uploads to this s3 bucket. Otherwise stored locally in out.csv');
        process.exit(0);
    }

    if (cachedir.substr(cachedir.length-1) != "/")
        cachedir = cachedir + "/";

    var sources = [];

    if (sourcedir.indexOf(".json") != -1) {    
        sources.push(sourcedir);
    } else {
        // Catch missing /
        if (sourcedir.substr(sourcedir.length-1) != "/")
            sourcedir = sourcedir + "/";

        // Setup list of sources
        sources = fs.readdirSync(sourcedir);

        // Only retain *.json
        for (var i = 0; i < sources.length; i++) {
            if (sources[i].indexOf('.json') == -1) {
                sources.splice(i, 1);
                i--;
            }
        }
    }

    main(sources, cachedir);
}

function loadSource(sourcefile) {    
    var source = JSON.parse(fs.readFileSync(sourcefile, 'utf8'));
    source.id = path.basename(sourcefile, '.json');
    if(typeof source.conform.headers === 'undefined') {
        source.conform.headers = 1;
        source.conform.skiplines = 1;
    }
    else {
        source.conform.headers = parseInt(source.conform.headers);
        if (typeof source.conform.skiplines === 'undefined')
            source.conform.skiplines = parseInt(source.conform.headers);        
        else {
            source.conform.skiplines = parseInt(source.conform.skiplines);
            if (source.conform.skiplines < source.conform.headers) throw 'Cannot skip fewer lines than the header line\'s location';
        }
    }
    
    return source;
}

function main(sources, cachedir, callback)
{
    var debug = require('debug')('conform:main');
    
    var failedSources = {};
    var toDoList = [];
    
    // ensure cachedir ends with a '/'
    if(cachedir[cachedir.length - 1] !== path.sep)
        cachedir = cachedir + path.sep;

    sources.forEach(function(sourceFile, i) {
        source = loadSource(sourceFile);
        toDoList.push(function(cb) {            
            processSource(source, cachedir, function(err, results) {
                // mark sources as failed if an error occurred
                if(err) failedSources[source.id] = err;
                // but don't pass through errors -- we want to process all sources                
                cb(null); 
            });
        }); 
    });

    var done = function(err, results) {        
        if (failedSources.length > 0) {
            if(callback) {
                callback(failedSources);
            }
            else {
                debug('Done with ' + failedSources.length + ' failure(s) on the following sources:');
                Object.keys(failedSources).forEach(function(failure) { debug(failure + ': ' + failedSources[failure].toString()); });                
                process.exit(0);
            }
        }
        else {
            if(callback) {
                callback(null);
            }
            else {
                debug('Done with no errors');    
                process.exit(0);
            }            
        }
    }

    async.series(toDoList, done);
}

function processSource(source, cachedir, callback) {    
    var tasks = [];
    var debug = require('debug')('conform:processSource');
    tasks.push(function(cb) {
        downloadCache(source, cachedir, cb);
    });
    tasks.push(function(cb) {          
        conformCache(source, cachedir, cb);
    });
    async.series(tasks, callback);;
}

function downloadCache(source, cachedir, callback) {    
    var debug = require('debug')('conform:downloadCache');    

    if ((!source.cache) || (source.skip === true) || (!source.conform)) {
        debug("Skipping: " + source.id);
        callback(null);
    } else {
        debug("Processing: " + source.id);

        // add trailing slash if it's missing
        if (cachedir[cachedir.length-1] !== '/') cachedir += '/';

        // skip download if the cache has already been downloaded                            
        if (!fs.existsSync(cachedFileLocation(source, cachedir))) {
            debug('did not find cached file at ' + cachedFileLocation(source, cachedir));
            debug('fetching ' + source.cache);
            var stream = request(source.cache);

            var bar;
            if(debug.enabled) {
                stream
                    .on('response', function(res) {
                        var len = parseInt(res.headers['content-length'], 10);
                        bar = new ProgressBar('  Downloading [:bar] :percent :etas', {
                            complete: '=',
                            incomplete: '-',
                            width: 20,
                            total: len
                        });
                    })
                    .on('data', function(chunk) {
                        if (bar) bar.tick(chunk.length);
                    });
            }
            stream.on('end', function() {
                    if (source.compression)
                        unzipCache(source, cachedir, callback);
                    else
                        callback();
                });

            var downloadDestination = cachedir + source.id + '.' + (source.compression ? source.compression : fileTypeExtensions[source.conform.type]);
            stream.pipe(fs.createWriteStream(downloadDestination));

        } else {
            debug("Cached file exists, skipping download");
            if (source.compression)
                unzipCache(source, cachedir, callback);
            else
                callback();
        }
    }
}

function unzipCache(source, cachedir, callback) {
    var fstream = require('fstream');
    var debug = require('debug')('conform:unzipCache');

    if (['shapefile', 'shapefile-polygon'].indexOf(source.conform.type) > -1) {
        if (!fs.existsSync(cachedir + source.id)) {
            debug('creating directory for shapefile')
            fs.mkdirSync(cachedir + source.id);
        }
    } 
    
    debug("Starting decompression to " + cachedir);

    var readStreamSource = cachedir + source.id + '.' + source.compression;
    var read = fs.createReadStream(readStreamSource);            

    var q = async.queue(function(task, qcallback) {        
        task.entry.pipe(fs.createWriteStream(task.outpath).on('finish', qcallback));
    }, 10);

    // track the number of output files we encounter. if >1, our path scheme is not gonna work
    matchingFiles = [];    
    read
        .pipe(unzip.Parse())
        .on('entry', function(entry){

            // cannot use path.extname() because of extensions like .shp.xml
            var pathParts = path.basename(entry.path).split(path.sep);
            pathParts = pathParts[pathParts.length-1].split('.')
            pathParts.shift()
            var extension = pathParts.join('.')
            if (extension[0] !== '.') extension = '.' + extension;

            var entryExistsWithinSourceFilePath = source.conform.file && (entry.path.indexOf(path.basename(source.conform.file, path.extname(source.conform.file))) > -1);

            // skip directories entirely
            if (entry.type === 'Directory') {
                entry.autodrain();
            }            
            // IF CSV/JSON THEN
            //    - IF NOT source.conform.file specified, take first JSON/CSV, error on multiple
            //    - IF source.conform.file specified, only take that path
            else if (_isFlatFileExtension(extension) && (!source.conform.file || (source.conform.file && (source.conform.file===entry.path)))) {
                debug('queueing ' + entry.path + ' for decompression');

                if(!source.conform.file) {
                    matchingFiles.push(entry.path);
                    if(matchingFiles.length > 1) throw 'Cannot parse archive - contains multiple eligible files: ' + matchingFiles.join(', ');
                }

                var outpath = cachedir + source.id + '.' + source.conform.type;
                q.push({entry: entry, outpath: outpath});
            }
            // IF Shapefile THEN
            //    - IF NOT source.conform.file specified, take first shapefile, error on multiple
            //    - IF source.conform.file specified, check if entry is party of that shapefile eg source.conform.file=='addresspoints/address.shp' && entry.path=='addresspoints/address.prj'
            else if (_isShapefileExtension(extension) && (!source.conform.file || (source.conform.file && entryExistsWithinSourceFilePath))) {
                debug('queueing ' + entry.path + ' for decompression');

                // .shp must be present, so we'll count those to keep track of overall #
                if(!source.conform.file && (extension === '.shp')) {
                    matchingFiles.push(entry.path);
                    if(matchingFiles.length > 1) throw 'Cannot parse archive - contains multiple eligible shapefiles: ' + matchingFiles.join(', ');
                }
            
                var outpath = cachedir + source.id + '/' + source.id + extension;
                q.push({entry: entry, outpath: outpath});             
            }
            else {
                // save ourselves some memory
                entry.autodrain();
            }
        })
        .on('finish', function() {
            // all unzip tasks have been queued, we can safely set the drain function
            // if we do it earlier it will fire multiple times as the queue
            // repeatedly empties between refills
            q.drain = function() {
                debug('Decompression complete');
                callback();    
            }            
        });
}

function conformCache(source, cachedir, callback){
    
    var debug = require('debug')('conform:conformCache');
    var csv = require('./Tools/csv');

    async.series([

        // convert to utf-8
        function(cb) {
            var encoding = require('./Tools/encoding');
            if(source.conform.encoding && (['geojson', 'csv'].indexOf(source.conform.type) > -1)) 
                encoding.utf8(source, cachedir, function(err) {
                    cb(err);
                });
            else
                cb();
        },

        //Convert to CSV
        function(cb) { 
            var convert = require('./Tools/convert');

            var s_srs = source.conform.srs ? source.conform.srs : false;

            if (source.conform.type === "shapefile")
                convert.shp2csv(cachedir + source.id + "/", source.id + '.shp', s_srs, cb);
            else if (source.conform.type === "shapefile-polygon")
                convert.polyshp2csv(cachedir + source.id + "/", source.id + '.shp', s_srs, cb);
            else if (source.conform.type === "geojson")
                convert.json2csv(cachedir + source.id + '.' + fileTypeExtensions[source.conform.type], cb);
            else if (source.conform.type === "csv") {
                convert.csv(source, cachedir, fileTypeExtensions[source.conform.type], cb);
            } 
            else
                cb();                
        }, 

        // Merge Columns        
        function(cb) { 
            if (source.conform.test) // Stops at converting to find col names
                process.exit(0);
            if (source.conform.merge)
                csv.mergeStreetName(source, cachedir, cb);
            else
                cb();
        }, 

        // Advanced merge columns
        function(cb) {
            if(source.conform.advanced_merge)
                csv.advancedMerge(source, cachedir, cb);
            else
                cb();
        },

        // Split Address Columns            
        function(cb) { 
            var csv = require('./Tools/csv');
            if (source.conform.split)
                csv.splitAddress(source, cachedir, cb);
            else
                cb();
        }, 

        // Drop Columns
        function(cb) {         
            csv.dropCol(source, cachedir, cb);
        }, 

        // Expand Abbreviations, Fix Capitalization & drop null rows            
        function(cb) { 
            csv.expand(source, cachedir, cb);
        }], 

        function(err, results) {
            debug("complete");

            if(err) 
                callback(err)
            else if (uploadToAWS)
                updateCache(callback);
            else   
                callback();
        }
    );
}

function updateManifest(source) {
    var debug = require('debug')('conform:updateManifest');
    fs.writeFileSync(sourcedir + source, JSON.stringify(source, null, 4));
    debug("Updating Manifest of " + source);
}

function updateCache(source, cachedir) {

    var debug = require('debug')('conform:updateCache');

    source.processed = "http://s3.amazonaws.com/" + bucketName + "/" + source.id.replace(".json", ".csv");

    debug("Updating s3 with " + source.id);

    var s3 = new AWS.S3();
    fs.readFile(cachedir + source.id + "/out.csv", function (err, data) {
        if (err)
            throw new Error('Could not find data to upload');

        var buffer = new Buffer(data, 'binary');

        var s3 = new AWS.S3();

        s3.putObject({
            Bucket: bucketName,
            Key: source.id.replace(".json", ".csv"),
            Body: buffer,
            ACL: 'public-read'
        }, function (response) {
            debug('Successfully uploaded package.');
            updateManifest(source);
            downloadCache(++cacheIndex);
        });
    });
}

module.exports = {
    downloadCache: downloadCache,
    main: main,
    loadSource: loadSource,
    fileTypeExtensions: fileTypeExtensions
}

if (require.main === module) { ConformCLI(); }
