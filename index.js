#!/usr/bin/env node

//NPM Dependancies
var argv = require('minimist')(process.argv.slice(2)),
    fs = require('fs'),
    ProgressBar = require('progress'),
    unzip = require('unzip'),
    request = require('request'),
    AWS = require('aws-sdk');

//Command Line Args
var sourceDir = argv._[0],
    cacheDir = argv._[1];

var cacheIndex = 0,
    source = null,
    parsed;

if (!sourceDir || !cacheDir) {
    throw new Error('usage: openaddresses-conform <path-to-sources> <path-to-cache>');
}

//Setup list of sources
var sources = fs.readdirSync(sourceDir);

//Only retain *.json
for (var i = 0; i < sources.length; i++) {
    if (sources[i].indexOf('.json') == -1) {
        sources.splice(i, 1);
        i--;
    }
}

//Begin Downloading Sources
downloadCache(cacheIndex);

function downloadCache(index) {
    if (index >= sources.length) {
        console.log("Complete!");
        process.exit(0);
    }

    source = sources[index];
    
    parsed = JSON.parse(fs.readFileSync(sourceDir + source, 'utf8'));

    if (!parsed.cache || parsed.skip === true || !parsed.conform) {
        console.log("Skipping: " + source);
        downloadCache(++cacheIndex);
    } else {
        console.log("Downloading: " + source);

        var stream = request(parsed.cache);

        showProgress(stream);
        stream.pipe(fs.createWriteStream(cacheDir + source.replace(".json", ".zip")));
    }
}

function unzipCache() {
    var fstream = require('fstream');

    fs.mkdirSync(cacheDir + source.replace(".json",""));
    
    var read = fs.createReadStream(cacheDir + source.replace(".json", ".zip")),
        write = fstream.Writer(cacheDir + source.replace(".json","") + "/");

    write.on('close', function() {
        console.log("  Finished Decompression"); //Daisy, Daisy...
        conformCache();
    });

    read.pipe(unzip.Parse()).pipe(write);
}

function conformCache(){
    var flow = require('flow');

    flow.exec(
        function() { //Convert Shapefile
            var convert = require('./Tools/convert');
            
            if (parsed.conform.type == "shapefile"){
                convert.shp2csv(cacheDir + source.replace(".json","") + "/", this);
            } else
                downloadCache(++cacheIndex);
        }, function(err) { //Merge Columns
            if (err) errorHandle(err);
            csv = require('./Tools/csv');

            if (parsed.conform.merge)
                csv.mergeStreetName(parsed.conform.merge.slice(0),cacheDir + source.replace(".json", "") + "/out.csv",this);
            else
                csv.none(this);
        }, function(err) { //Split Address Columns
            if (err) errorHandle(err);

            if (parsed.conform.split)
                csv.splitAddress(parsed.conform.split, 1, cacheDir + source.replace(".json", "") + "/out.csv", this);
            else
                csv.none(this);
        }, function(err) { //Drop Columns
            if (err) errorHandle(err);

            var csv = require('./Tools/csv');
            var keep = [parsed.conform.lon, parsed.conform.lat, parsed.conform.number, parsed.conform.street];

            csv.dropCol(keep, cacheDir + source.replace(".json", "") + "/out.csv", this);
        }, function(err) { //Expand Abbreviations & Fix Capitalization
            if (err) errorHandle(err);

            var csv = require('./Tools/csv');
            csv.expand(cacheDir + source.replace(".json", "") + "/out.csv", this);
            
        }, function(err) { //Start Next Download
            if (err) errorHandle(err);
            
            console.log("Complete");
            updateCache();
        }
    );

}

function errorHandle(err){
    console.log("ERROR: " + err);
    console.log("Skipping to next source");
    downloadCache(++cacheIndex);
}

function updateManifest() {
    fs.writeFileSync(sourceDir + source, JSON.stringify(parsed, null, 4));
    console.log("  Updating Manifest of " + source);
}

function updateCache() {
    parsed.processed = "http://s3.amazonaws.com/openaddresses/" + source.replace(".json", ".csv");
    
    console.log("  Updating s3 with " + source);
    
    var s3 = new AWS.S3();
    fs.readFile(cacheDir + source.replace(".json", "") + "/out.csv", function (err, data) {
        if (err)
            throw new Error('Could not find data to upload'); 
        
        var buffer = new Buffer(data, 'binary');

        var s3 = new AWS.S3();
        
        s3.client.putObject({
            Bucket: 'openaddresses',
            Key: source.replace(".json", ".csv"),
            Body: buffer,
            ACL: 'public-read'
        }, function (response) {
            console.log('  Successfully uploaded package.');
            updateManifest();
            downloadCache(++cacheIndex);
        });
    });
}


function showProgress(stream) {
    var bar;
    stream.on('response', function(res) {
        var len = parseInt(res.headers['content-length'], 10);
        bar = new ProgressBar('  downloading [:bar] :percent :etas', {
            complete: '=',
            incomplete: '-',
            width: 20,
            total: len
        });
    });
    stream.on('data', function(chunk) {
        if (bar) bar.tick(chunk.length);
    }).on('end', function() {
        unzipCache();
    });
}
