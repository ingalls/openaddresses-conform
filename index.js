#!/usr/bin/env node

//NPM Dependancies
var argv = require('minimist')(process.argv.slice(2)),
    fs = require('fs'),
    ProgressBar = require('progress'),
    unzip = require('unzip'),
    request = require('request');

//Command Line Args
var sourceDir = argv._[0],
    cacheDir = argv._[1];

var cacheIndex = 0,
    source = null,
    parsed;

if (!sourceDir || !cacheDir) {
    throw new Error('usage: openaddresses-cache <path-to-sources> <path-to-cache>');
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
        console.log("  Finished Unzipping");
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
                csv.mergeStreetName(parsed.conform.merge,cacheDir + source.replace(".json","") + "/out.csv",this);
            else
                csv.none(this);
        }, function(err) {
            console.log("Complete");
            downloadCache(++cacheIndex);
        }
    );

}

function errorHandle(err){
    console.log("ERROR: " + err);
    console.log("Skipping to next source");
    downloadCache(++cacheIndex);
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
