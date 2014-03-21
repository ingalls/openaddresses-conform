exports.dropCol = function dropCol(keep, callback){
  var fs = require('fs');
  var readline = require('readline');
  var stream = require('stream');
  
  var instream = fs.createReadStream('./tmp/out.csv');
  var outstream = new stream;
  var rl = readline.createInterface(instream, outstream);

  var keepArray = []; //Stores the column numbers to keep

  var linenum = 1;

  rl.on('line', function(line) {
    var elements = line.split(',');
    
    if (linenum == 1){
      fs.appendFileSync('./tmp/final.csv', 'LON, LAT, NUMBER, STREET\n'); //Ready Output File
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
      
      fs.appendFileSync('./tmp/final.csv', lon + ',' + lat + ',' + parseInt(num) + ',' + str + '\n');
      
    }
    linenum++;
  });


  rl.on('close', function() {
    process.nextTick(function(){
      callback();
    });
  });
}

//Joins two columns -> column is then named with name Value
exports.mergeCol = function mergeCol(col1, col2, name, callback){
  var fs = require('fs');
  var readline = require('readline');
  var stream = require('stream');
  
  var instream = fs.createReadStream('./tmp/out.csv');
  var outstream = new stream;
  var rl = readline.createInterface(instream, outstream);
  var linenum = 1;
  
  var mergeArray = [];
  var deleteElement, mergeElement; //Element to store merge in and element to delete

  rl.on('line', function(line) {
    var elements = line.split(',');
    
    if (linenum == 1){
      elements = elements.join('|').toLowerCase().split('|');
      col1 = col1.toLowerCase();
      col2 = col2.toLowerCase();

      for (var i = 0; i < elements.length; i++){
        if (elements[i] == col1){
          elements[i] = name;
          mergeArray.push('1');
          mergeElement = i;
        } else if (elements[i] == col2){
          mergeArray.push('2');
          elements.splice(i, 1);
          deleteElement = i;
        } else {
          mergeArray.push('0');
        }
      } 
      fs.appendFileSync('./tmp/tmp.csv', elements+'\n'); //Writes Headers to File
    } else {
      for (var i = 0; i < elements.length; i++){
        var merge1, merge2;
        
        if (mergeArray[i] == '1') {
          merge1 = elements[i];
        } else if (mergeArray[i] == '2') {
          merge2 = elements[i];
        }
      }
      
      elements[mergeElement] = merge1 + ' ' + merge2;
      elements.splice(deleteElement, 1);
      
      fs.appendFileSync('./tmp/tmp.csv', elements + '\n');
    }
    linenum++;
  });

  rl.on('close', function() {
    
    var sh = require('execSync');
    sh.run('rm ./tmp/out.csv');
    sh.run('mv ./tmp/tmp.csv ./tmp/out.csv');
    
    process.nextTick(function(){
      callback();
    });
  });
}

exports.expand = function expand(callback){
  var fs = require('fs');
  var readline = require('readline');
  var stream = require('stream');
  var instream = fs.createReadStream('./tmp/final.csv');
  var outstream = new stream;
  var rl = readline.createInterface(instream, outstream);
  var linenum = 1;

  var expand = require('./expand.json');

  rl.on('line', function(line) {
    var elements = line.split(',');
    
    if (linenum == 1){
      fs.appendFileSync('./tmp/tmp.csv', elements+'\n'); //Write Headers
    } else {
      
      elements[3] = elements[3].toLowerCase();
      elements[3] = elements[3].replace(/\./g,'');
      
      if (linenum % 1000 == 0)
        console.log('  Processed Addresses: ' + linenum);
      
      
      for (var i = 0; i < expand.abbr.length; i++){
        var key = expand.abbr[i].k;
        var value = expand.abbr[i].v
        var tokenized = elements[3].split(' ');
        
        for(var e = 0; e < tokenized.length; e++){
          if (tokenized[e] == key)
            tokenized[e] = value;
        }
        
        elements[3] = tokenized.join(' ');
      }
      
      //Take care of the pesky st vs saint
      var tokenized = elements[3].split(' ');
      var length = tokenized.length;
      
      //Only converts to street if in the last half of the words
      for (var i = 0; length - i >= Math.floor(length/2); i++){
        if (tokenized[length - i] === "st")
          tokenized[length - i] = "street";
      }
      //Only converts to saint if in the last half of the words
      for (var i = 0; i <= Math.ceil(length/2); i++){
        if (tokenized[i] === "st")
          tokenized[i] = "saint";
      }
      elements[3] = tokenized.join(" ");
      
      
      fs.appendFileSync('./tmp/tmp.csv', elements+'\n');
    }
    
    linenum++;
  });

  rl.on('close', function() {
    
    var sh = require('execSync');
    sh.run('rm ./tmp/final.csv');
    sh.run('mv ./tmp/tmp.csv ./tmp/final.csv');
    
    process.nextTick(function(){
      callback();
    });
  });

}

//If the address is given as one field
//This will take a given number of fields as the number address (numFields)
//numFields defaults to 1
//and use the remainder as the street address
//Creates two new columns called auto_num & auto_str
exports.splitAddress = function splitAddress(col, numFields, callback){
  console.log('  Splitting Address Column');
  
  var fs = require('fs');
  var readline = require('readline');
  var stream = require('stream');
  
  var instream = fs.createReadStream('./tmp/out.csv');
  var outstream = new stream;
  var rl = readline.createInterface(instream, outstream);

  var linenum = 1;
  var element; //Stores the element # to split
  var length; //Stores start position of auto_num

  rl.on('line', function(line) {
    var elements = line.split(',');
    
    if (linenum == 1){
      fs.appendFileSync('./tmp/tmp.csv', elements+ ',auto_num,auto_str\n'); //Write Headers
      length = elements.length + 1; //Stores index of auto_num
      
      for (var i = 0; i < elements.length; i++){
        if (col.toLowerCase() == elements[i].toLowerCase())
          element = i;
      }
    } else {
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
      fs.appendFileSync('./tmp/tmp.csv', elements + '\n');
      
    }
    linenum++;
  });

  rl.on('close', function() {
    
    var sh = require('execSync');
    sh.run('rm ./tmp/out.csv');
    sh.run('mv ./tmp/tmp.csv ./tmp/out.csv');
    
    process.nextTick(function(){
      callback();
    });
  });
}

exports.none = function none(callback){
  process.nextTick(function(){
    callback();
  });
}
