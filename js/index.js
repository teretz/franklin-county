const fs = require('fs');
const readline = require('readline');

const inputFile = 'input.csv';
const outputFile = 'output.csv';

const headers = [
  'LastName', 'FirstName', 'MiddleName', 'Address', 'City', 'State', 'ZipCode',
  'ArrestStatus', 'ChargeOneDesc', 'ChargeTwoDesc', 'ChargeThreeDesc',
  'ChargeOneWarrantNumber', 'ChargeTwoWarrantNumber', 'ChargeThreeWarrantNumber'
];

const output = fs.createWriteStream(outputFile);

// Write headers to the output file
output.write(headers.join(',') + '\n');

const rl = readline.createInterface({
  input: fs.createReadStream(inputFile),
  output: process.stdout,
  terminal: false
});

let record = {};
let chargeCount = 0;

const lineReaderPromise = new Promise((resolve, reject) => {
  rl.on('line', (line) => {
    const addressMatch = line.match(/(.+)\s+(\d+\s+.+),\s*([A-Z\s]+),\s*([A-Z\s]+)\s+(\d+)/);
    if (addressMatch) {
      if (Object.keys(record).length > 0 && chargeCount > 0) {
        output.write(Object.values(record).join(',') + '\n');
      }

      // Start a new record
      record = {};
      chargeCount = 0;

      const [_, name, address, city, state, zip] = addressMatch;
      const names = name.split(/\s+/);
      const lastName = names.shift();
      const firstName = names.shift() || '';
      const middleName = names.join(' ') || '';

      record.LastName = lastName;
      record.FirstName = firstName;
      record.MiddleName = middleName;
      record.Address = address;
      record.City = city;
      record.State = state;
      record.ZipCode = zip;
      record.ArrestStatus = line.includes('ARRESTED ON WARRANT') ? 'ARRESTED ON WARRANT' : names.join(' ').trim();
    } else if (line.trim() !== '') {
      chargeCount++;
      const [chargeDesc, warrantNumber] = line.trim().split('\t\t');
      record[`Charge${chargeCount}Desc`] = chargeDesc || '';
      record[`Charge${chargeCount}WarrantNumber`] = warrantNumber || '';
      if (chargeCount === 3) {
        output.write(Object.values(record).join(',') + '\n');
        record = {};
        chargeCount = 0;
      }
    }
  });

  rl.on('close', () => {
    if (Object.keys(record).length > 0) {
      output.write(Object.values(record).join(',') + '\n');
    }
    output.end();
    resolve();
  });

  rl.on('error', reject);
});

lineReaderPromise
  .then(() => {
    console.log('Parsing complete. Output written to ' + outputFile);
  })
  .catch((err) => {
    console.error('An error occurred:', err);
  });
