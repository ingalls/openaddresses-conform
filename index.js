#!/usr/bin/env node
/*jslint indent: 4, node: true */

//NPM Dependancies
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

function ConformCLI(){
    require('debug').enable('conform:*');

    //Command Line Args
    var sourceDir = argv._[0],
        cacheDir = argv._[1],
        bucketName = undefined,
        aws = false;

    if (argv._.length == 3)
    {
        aws = true;
        bucketName = (argv._[2] == 'aws' ? 'openaddresses' : argv._[2]);
    }

    var cacheIndex = 0,
        source = null,
        parsed;

    if (!sourceDir || !cacheDir) {
        console.log('usage: openaddresses-conform <path-to-sources> <path-to-cache> <options>');
        console.log('       openaddresses-conform  <single source>  <path-to-cache> <options>');
        console.log('\nOptions:');
        console.log('bucket name - If credentials are found automatically uploads to this s3 bucket. Otherwise stored locally in out.csv');
        process.exit(0);
    }

    if (cacheDir.substr(cacheDir.length-1) != "/")
        cacheDir = cacheDir + "/";

    var sources = [];

    if (sourceDir.indexOf(".json") != -1) {
        var dir = sourceDir.split("/"),
            singleSource = dir[dir.length-1];

        sourceDir = sourceDir.replace(singleSource,"");

        sources.push(singleSource);
    } else {
        //Catch missing /
        if (sourceDir.substr(sourceDir.length-1) != "/")
            sourceDir = sourceDir + "/";

        //Setup list of sources
        sources = fs.readdirSync(sourceDir);

        //Only retain *.json
        for (var i = 0; i < sources.length; i++) {
            if (sources[i].indexOf('.json') == -1) {
                sources.splice(i, 1);
                i--;
            }
        }
    }

    conformMain(sources);
}

function loadSource(sourcefile) {
    var source = JSON.parse(fs.readFileSync(sourcefile, 'utf8'));
    source.id = path.basename(sourcefile, '.json');
    return source;
}

function conformMain(sources)
{
    var toDoList = [];
    sources.forEach(function(sourceFile, i) {
        source = loadSource(sourceFile);

        toDoList.push(function(cb) {
            downloadCache(source, cacheDir, cb);
        });
        toDoList.push(function(cb) {
            conformCache(source, cb);
        });        
    });

    var done = function(err, results) {
        console.log('Done!');
        process.exit(0);
    }

    async.series(toDoList, done);
}

function downloadCache(source, cachedir, callback) {    
    var debug = require('debug')('conform:downloadCache');    

    if ((!source.cache) || (source.skip === true) || (!source.conform)) {
        debug("Skipping: " + source.id);
        callback(null);
    } else {
        debug("Processing: " + source.id);

        // add trailing slash, if it's missing
        if (cachedir[cachedir.length-1] !== '/') cachedir += '/';

        // skip download if the cache has already been downloaded
        
        var sourceFile = cachedir + source.id + '.' + fileTypeExtensions[source.conform.type];
        if (!fs.existsSync(sourceFile)) {
            var stream = request(source.cache);

            var bar;
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
                })
                .on('end', function() {
                    if (source.compression)
                        unzipCache(source, cachedir, callback);
                    else
                        callback();
                });

            var downloadDestination = cachedir + source.id + '.' + (source.compression ? source.compression : fileTypeExtensions[source.conform.type]);
            stream.pipe(fs.createWriteStream(downloadDestination));

        } else {
            debug("Cache exists, skipping download");
            callback();
        }
    }
}

function unzipCache(source, cachedir, callback) {
    var fstream = require('fstream');
    var debug = require('debug')('conform:unzipCache');

    debug("Starting Decompression");

    var cacheSource = cachedir + source.id;
    if (fs.existsSync(cacheSource)) {
        debug("Folder Exists");
        if (fs.existsSync(cacheSource + "/out.csv"))
            fs.unlinkSync(cacheSource + "/out.csv");        
    } else {
        fs.mkdirSync(cachedir + source.id);
    }

    var readStreamSource = cachedir + source.id + '.' + source.compression;
    if (source.conform.type in ['csv', 'json'])        
        writeStreamDest = cachedir + source.id + '.' + source.conform.type;
    else
        writeStreamDest = cachedir + source.id;
    
    var read = fs.createReadStream(readStreamSource),
        write = fstream.Writer(writeStreamDest);

    write.on('close', function() {
        debug("Finished Decompression"); //Daisy, Daisy...
        callback(null);
    });

    read.pipe(unzip.Parse()).pipe(write);
}

function conformCache(callback){

    async.series(
        function(cb) { //Convert to CSV
            var convert = require('./Tools/convert');

            var s_srs = parsed.conform.srs ? parsed.conform.srs : false;

            if (parsed.conform.type === "shapefile")
                convert.shp2csv(cacheDir + source.replace(".json","") + "/", parsed.conform.file, s_srs, this, cb);
            else if (parsed.conform.type === "shapefile-polygon")
                convert.polyshp2csv(cacheDir + source.replace(".json","") + "/", parsed.conform.file, s_srs, this, cb);
            else if (parsed.conform.type === "geojson")
                convert.json2csv(cacheDir + source, this, cb);
            else if (parsed.conform.type === "csv")
                convert.csv(cacheDir + source.replace(".json", ".csv"), this, cb);
            else
                cb(null);                
        },         
        function(cb) { //Merge Columns
            if (err) errorHandle(err);

            if (parsed.conform.test) //Stops at converting to find col names
                process.exit(0);

            var csv = require('./Tools/csv');

            if (parsed.conform.merge)
                csv.mergeStreetName(parsed.conform.merge.slice(0),cacheDir + source.replace(".json", "") + "/out.csv",this);
            else
                csv.none(this);
        }, function(err) { //Split Address Columns
            if (err) errorHandle(err);

            var csv = require('./Tools/csv');

            if (parsed.conform.split)
                csv.splitAddress(parsed.conform.split, 1, cacheDir + source.replace(".json", "") + "/out.csv", this);
            else
                csv.none(this);
        }, function(err) { //Drop Columns
            if (err) errorHandle(err);

            var csv = require('./Tools/csv');
            var keep = [parsed.conform.lon, parsed.conform.lat, parsed.conform.number, parsed.conform.street];

            csv.dropCol(keep, cacheDir + source.replace(".json", "") + "/out.csv", this);
        }, function(err) { //Expand Abbreviations, Fix Capitalization & drop null rows
            if (err) errorHandle(err);

            var csv = require('./Tools/csv');

            csv.expand(cacheDir + source.replace(".json", "") + "/out.csv", this);
        }, function(err) {
            if (err) errorHandle(err);

            var csv = require('./Tools/csv');
            //csv.deDup(cacheDir + source.replace(".json", "") + "/out.csv",this); //Not ready for production
            csv.none(this);
        }, function(err) { //Start Next Download
            if (err) errorHandle(err);

            console.log("Complete");

            if (aws)
                updateCache();
            else
                downloadCache(++cacheIndex);
        }
    );

}

function errorHandle(err){
    console.log("ERROR: " + err);
    console.log("Skipping to next source");
    downloadCache(++cacheIndex);
}

function updateManifest(source) {
    fs.writeFileSync(sourceDir + source, JSON.stringify(source, null, 4));
    console.log("  Updating Manifest of " + source);
}


function updateCache(source) {

    var debug = require('debug')('conform:updateCache');

    parsed.processed = "http://s3.amazonaws.com/" + bucketName + "/" + source.id.replace(".json", ".csv");

    debug("  Updating s3 with " + source.id);

    var s3 = new AWS.S3();
    fs.readFile(cacheDir + source.replace(".json", "") + "/out.csv", function (err, data) {
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
            debug('  Successfully uploaded package.');
            updateManifest(source);
            downloadCache(++cacheIndex);
        });
    });
}

module.exports = {
    downloadCache: downloadCache,
    conformMain: conformMain,
    loadSource: loadSource
}

if (require.main === module) { ConformCLI(); }
