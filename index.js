/*jslint indent: 4, node: true */
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
    cacheDir = argv._[1],
    aws = false;

if (argv._[2] === 'aws')
    aws = true;

var cacheIndex = 0,
    source = null,
    parsed;

if (!sourceDir || !cacheDir) {
    console.log('usage: openaddresses-conform <path-to-sources> <path-to-cache> <options>');
    console.log('       openaddresses-conform  <single source>  <path-to-cache> <options>');
    console.log('\nOptions:');
    console.log('aws - If credentials are found automatically uploads to s3. Otherwise stored locally in out.csv');
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

//Begin Downloading Sources
downloadCache(cacheIndex);

function downloadCache(index) {
    if (index >= sources.length) {
        console.log("Complete!");
        process.exit(0);
    }

    source = sources[index];

    try {
        parsed = JSON.parse(fs.readFileSync(sourceDir + source, 'utf8'));

        if (!parsed.cache || parsed.skip === true || !parsed.conform) {
            console.log("Skipping: " + source);
            downloadCache(++cacheIndex);
        } else {
            console.log("Processing: " + source);

            // skip download if the cache has already been downloaded
            var sourceFile = parsed.cache.split('/');
            sourceFile = sourceFile[sourceFile.length-1];
            if (!fs.existsSync(cacheDir + sourceFile)) {
                var stream = request(parsed.cache);
            } else {
                console.log("Cache exists, skipping download");
                var stream = fs.createReadStream(cacheDir + sourceFile);
            }
            showProgress(stream);

            if (parsed.conform.type === "geojson")
                stream.pipe(fs.createWriteStream(cacheDir + source));
            else if (parsed.conform.type === "csv")
                stream.pipe(fs.createWriteStream(cacheDir + source.replace(".json", ".csv")));
            else
                stream.pipe(fs.createWriteStream(cacheDir + source.replace(".json", ".zip"))); //This should replace with compression value not zip
        }
    } catch (err) {
        console.log("Malformed JSON!");
        downloadCache(++index);
    }
}

function unzipCache() {
    var fstream = require('fstream');

    console.log("  Starting Decompression");

    var cacheSource = cacheDir + source.replace(".json", "");
    if (fs.existsSync(cacheSource)) {
        console.log("  Folder Exists");
        if (fs.existsSync(cacheSource + "/out.csv"))
            fs.unlinkSync(cacheSource + "/out.csv");
        return conformCache();
    } else {
        fs.mkdirSync(cacheDir + source.replace(".json",""));
    }
    
    var read = fs.createReadStream(cacheDir + source.replace(".json", ".zip")),
        write = fstream.Writer(cacheDir + source.replace(".json","/"));

    write.on('close', function() {
        console.log("  Finished Decompression"); //Daisy, Daisy...
        conformCache();
    });

    read.pipe(unzip.Parse()).pipe(write);
}

function conformCache(){
    var flow = require('flow');

    flow.exec(
        function() { //Convert to CSV
            var convert = require('./Tools/convert');

            if (parsed.conform.type === "shapefile")
                convert.shp2csv(cacheDir + source.replace(".json","") + "/", parsed.conform.file, this);
            else if (parsed.conform.type === "shapefile-polygon")
                convert.polyshp2csv(cacheDir + source.replace(".json","") + "/", parsed.conform.file, this);
            else if (parsed.conform.type === "geojson")
                convert.json2csv(cacheDir + source, this);
            else if (parsed.conform.type === "csv")
                convert.csv(cacheDir + source.replace(".json", ".csv"),this);
            else
                downloadCache(++cacheIndex);
        }, function(err) { //Merge Columns
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
        bar = new ProgressBar('  Downloading [:bar] :percent :etas', {
            complete: '=',
            incomplete: '-',
            width: 20,
            total: len
        });
    });
    stream.on('data', function(chunk) {
        if (bar) bar.tick(chunk.length);
    }).on('end', function() {
        if (parsed.compression == "zip")
            unzipCache();
        else
            conformCache();
    });
}
