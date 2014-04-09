exports.dropCol = function dropCol(keep, loc, callback){
    var fs = require('fs'),
        readline = require('readline'),
        stream = require('stream');

    console.log("  Dropping Unnecessary Data");

    if (fs.exists('./tmp.csv'))
        fs.unlinkSync('./tmp.csv');
  
    var instream = fs.createReadStream(loc),
        outstream = new stream;

    var rl = readline.createInterface(instream, outstream),
        keepArray = []; //Stores the column numbers to keep

    var linenum = 1;

    rl.on('line', function(line) {
        var elements = line.split(',');
    
        if (linenum == 1){
            fs.appendFileSync('./tmp.csv', 'LON, LAT, NUMBER, STREET\n'); //Ready Output File
            
            elements = elements.join('|').toLowerCase().split('|');
            keep = keep.join('|').toLowerCase().split('|');
      
            keepArray.push('lon'); //X Must be first column
            keepArray.push('lat'); //Y Must be 2nd column
      
            for (var i = 2; i < elements.length; i++){
                if (elements[i] == keep[2])
                    keepArray.push('num');
                else if (elements[i] == keep[3])
                    keepArray.push('str');
                else
                    keepArray.push('null');
            } 
        } else {
            var lon, lat, num, str;
      
            for (var i = 0; i < elements.length; i++){
                if (keepArray[i] == 'lon') {
                    lon = elements[i];
                } else if (keepArray[i] == 'lat') {
                    lat = elements[i];
                } else if (keepArray[i] == 'num') { 
                    num = elements[i];
                } else if (keepArray[i] == 'str') {
                    str = elements[i];
                }
            }

            fs.appendFileSync('./tmp.csv', lon + ',' + lat + ',' + parseInt(num) + ',' + str + '\n');
        }

        linenum++;
    });

    rl.on('close', function() {
        fs.unlinkSync(loc);
        var write = fs.createWriteStream(loc);

        write.on('close', function() {
            fs.unlinkSync('./tmp.csv');
            callback();
        });
        
        fs.createReadStream('./tmp.csv').pipe(write);
    });
}

//Joins Columns in order that they are given in array into 'street' column
exports.mergeStreetName = function mergeStreetName(cols, loc, callback){
    var fs = require('fs'),
        readline = require('readline'),
        stream = require('stream'),
        instream = fs.createReadStream(loc),
        outstream = new stream,
        rl = readline.createInterface(instream, outstream),
        linenum = 1;
    if (fs.exists('./tmp.csv'))
        fs.unlinkSync('./tmp.csv');

    console.log("  Merging Columns");

    for (var i = 0; i < cols.length; i++) {
        cols[i] = cols[i].toLowerCase();
    }

    rl.on('line', function(line) {
        var elements = line.split(',');
    
        if (linenum == 1){
            for (var i = 0; i < elements.length; i++) {
                elements[i] = elements[i].toLowerCase();
                for (var e = 0; e < cols.length; e++) {
                    if (cols[e] == elements[i]){
                        cols[e] = i;
                    }
                }
            }
            elements.splice(3,0,"auto_street");
            fs.appendFileSync('./tmp.csv', elements+'\n'); //Writes Headers to File
        } else {
            var street = "";

            for (var i = 0; i < cols.length; i++){
                if (elements[cols[i]])
                    street = street.trim() + " " +  elements[cols[i]].trim();
            }
            elements.splice(3,0,street.trim());
            fs.appendFileSync('./tmp.csv', elements+'\n');
        }
        ++linenum;
    });

    rl.on('close', function() {
        fs.unlinkSync(loc);
        var write = fs.createWriteStream(loc);

        write.on('close', function() {
            fs.unlinkSync('./tmp.csv');
            callback();
        });
        
        fs.createReadStream('./tmp.csv').pipe(write);
    });
}

exports.expand = function expand(loc, callback) {
    var fs = require('fs'),
        readline = require('readline'),
        stream = require('stream'),
        expand = require('./expand.json');
        
    var instream = fs.createReadStream(loc),
        outstream = new stream,
        rl = readline.createInterface(instream, outstream),
        linenum = 1;

    rl.on('line', function(line) {
        var elements = line.split(',');
    
        if (linenum == 1) {
            fs.appendFileSync('./tmp.csv', elements+'\n'); //Write Headers
        } else {
            if (elements[2] !== "NaN" && elements[3]) {
                
                elements[3] = elements[3].toLowerCase();
                elements[3] = elements[3].replace(/\./g,'');
      
                if (linenum % 10000 == 0)
                    console.log('  Processed Addresses: ' + linenum);
          
                for (var i = 0; i < expand.abbr.length; i++) {
                    var key = expand.abbr[i].k;
                    var value = expand.abbr[i].v
                    var tokenized = elements[3].split(' ');
            
                    for(var e = 0; e < tokenized.length; e++) {
                        if (tokenized[e] == key)
                        tokenized[e] = value;
                    }
            
                    elements[3] = tokenized.join(' ');
                }
          
                //Take care of the pesky st vs saint
                var tokenized = elements[3].split(' ');
                var length = tokenized.length;
          
                //Only converts to street if in the last half of the words
                for (var i = 0; length - i >= Math.floor(length/2); i++) {
                    if (tokenized[length - i] === "st")
                        tokenized[length - i] = "street";
                }
                
                //Only converts to saint if in the last half of the words
                for (var i = 0; i <= Math.ceil(length/2); i++) {
                    if (tokenized[i] === "st")
                        tokenized[i] = "saint";
                }

                elements[3] = tokenized.join(" ");
                elements[3] = elements[3].toLowerCase().replace( /(^|\s)([a-z])/g , function(m,p1,p2){ return p1+p2.toUpperCase(); } );
          
                fs.appendFileSync('./tmp.csv', elements+'\n');
            }
        }
    
        linenum++;
    });

    rl.on('close', function() {
        fs.unlinkSync(loc);
        var write = fs.createWriteStream(loc);

        write.on('close', function() {
            fs.unlinkSync('./tmp.csv');
            callback();
        });
        
        fs.createReadStream('./tmp.csv').pipe(write);
    });
}

//If the address is given as one field
//This will take a given number of fields as the number address (numFields)
//numFields defaults to 1
//and use the remainder as the street address
//Creates two new columns called auto_num & auto_str
exports.splitAddress = function splitAddress(col, numFields, loc, callback){
    console.log('  Splitting Address Column');

    col = col.toLowerCase();

    var fs = require('fs'),
        readline = require('readline'),
        stream = require('stream');
  
    var instream = fs.createReadStream(loc),
        outstream = new stream,
        rl = readline.createInterface(instream, outstream);

    var linenum = 1,
        element, //Stores the element # to split
        length; //Stores start position of auto_num

    rl.on('line', function(line) {
        var elements = line.split(',');
    
        if (linenum == 1){
            fs.appendFileSync('./tmp.csv', elements+ ',auto_number,auto_street\n'); //Write Headers
            length = elements.length + 1; //Stores index of auto_num
      
            for (var i = 0; i < elements.length; i++){
                col = col
                
                if (col == elements[i].toLowerCase())
                element = i;
            }
        } else {
            if (elements[element]) {
                var token = elements[element].split(' ');
                var street = token[numFields];
                var number = token[0];
        
                for (var num = 1; num < numFields; num++){
                    number = number + " " + token[num];
                }
          
                for (var str = numFields+1; str < token.length; str++){
                    street = street + " " + token[str];
                }
          
                elements[length - 1] = number;
                elements[length] = street;
                fs.appendFileSync('./tmp.csv', elements + '\n');
            }
        }

        linenum++;
    });

    rl.on('close', function() {
        fs.unlinkSync(loc);
        var write = fs.createWriteStream(loc);

        write.on('close', function() {
            fs.unlinkSync('./tmp.csv');
            callback();
        });
        
        fs.createReadStream('./tmp.csv').pipe(write);
    });
}

exports.none = function none(callback) {
    callback();
}
