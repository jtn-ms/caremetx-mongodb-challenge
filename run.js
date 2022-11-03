require('dotenv').config()
const mongodb = require("mongodb").MongoClient;
const csvtojson = require("csvtojson");
const fs = require("fs");
const fastcsv = require("fast-csv");

var pipeline = [
  {
    "$match": {
      "consent": "Y",
      "email": {"$ne":""},
    }
  }
]

// create Patients Collection
let stream = fs.createReadStream("data.csv");
let csvData = [];
let csvStream = fastcsv
  .parse()
  .on("data", function(line) {
    const data = line[0].split("|")
    csvData.push({
        program_id: data[0],
        data_source: data[1],
        card_number: data[2],
        member_id: data[3],
        first_name:data[4],
        last_name: data[5],
        dob: data[6],
        address1: data[7],
        address2: data[8],
        city: data[9],
        state: data[10],
        zip: data[11],
        telephone: data[12],
        email: data[13],
        consent: data[14],
        mobile_phone: data[15],
    });
  })
  .on("end", function() {
    // remove the first line: header
    csvData.shift();

    console.log(csvData);

    mongodb.connect(
      process.env.MONGODB_URL,
      { useNewUrlParser: true, useUnifiedTopology: true },
      (err, db) => {
        if (err) throw err;
        var dbo = db.db(process.env.MONGODB_DB_NAME);
        dbo.collection("patients").insertMany(csvData, (err, res) => {
          if (err) throw err;
          console.log(`Inserted: ${res.insertedCount} rows`);
          // create Emails Collection
          dbo.collection("patients").aggregate(pipeline).toArray(function(err, patients) {
            if (err) throw err;
            console.log(patients);
        
            var now = Date.now();
            var emails = [];
            for (const patient of patients) {
              for (let i=1;i<=4;i++){
                emails.push({
                  email:patient.email,
                  name: `Day ${i}`,
                  scheduled_date: new Date(now + i*(3600 * 1000 * 24)),
                })
              }
            }
            dbo.collection("emails").insertMany(emails, (err, res) => {
              if (err) throw err;
              console.log(`Inserted: ${res.insertedCount} rows`);
              db.close();
            });
          });
          });
      }

    );
  });

stream.pipe(csvStream);

