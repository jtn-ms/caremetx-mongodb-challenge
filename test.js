require('dotenv').config()
var mongodb = require('mongodb').MongoClient;

const csvtojson = require("csvtojson");

function IsIdentical(arr1,arr2){
  if (arr1.length !== arr2.length) {
    console.log(`${arr1.length} !== ${arr2.length}`)
    return false;
  }
  for (let i=0;i<arr1.length;i++){
    if (arr1[i].toString()!==arr2[i].toString()) {
      console.log(`${arr1[i]} !== ${arr2[i]}`)
      return false;
    }
  }
  return true;
}

const csv_data = csvtojson()
  .fromFile("data.csv")
  .then(data => {
    var csv_data = data.map((item)=>Object.values(item));
    mongodb.connect(process.env.MONGODB_URL, function(err, db) {
      if (err) throw err;
      var dbo = db.db(process.env.MONGODB_DB_NAME);

      dbo.collection("patients").find().toArray(function(err, patients) {
        if (err) throw err;
        console.log("\n1. Check if file data matches db data");
        var db_data = patients.map((patient)=>{
            delete patient._id;
            return Object.values(patient).join("|");
        })
        if (IsIdentical(csv_data,db_data)) {
          console.log("Passed");
        } else {
          console.log("Failed");
        }
        db.close();
      });
    });
  });

mongodb.connect(process.env.MONGODB_URL, function(err, db) {
  if (err) throw err;
  var dbo = db.db(process.env.MONGODB_DB_NAME);
  dbo.collection("patients").find({ first_name: "" }).toArray(function(err, patients) {
    if (err) throw err;
    console.log("\n2. Print out all patients where the first name is missing")
    console.log(patients.map((patient)=>patient.member_id));
  });

  dbo.collection("patients").find({ consent: "Y", email: "" }).toArray(function(err, patients) {
    if (err) throw err;
    console.log("\n3. Print out all patients where the email is missing and content is Y")
    console.log(patients.map((patient)=>patient.member_id));
    db.close();
  });
});

mongodb.connect(process.env.MONGODB_URL, function(err, db) {
  if (err) throw err;
  var dbo = db.db(process.env.MONGODB_DB_NAME);
  
  dbo.collection("patients").find().toArray(function(err, patients) {
    if (err) throw err;
    console.log("\n4 & 5. Check if any patient without CONSENT as Y exists in Emails");
    var noes = [];
    var yeses = [];
    for (const patient of patients){
      const {email,consent} = patient;
      if (consent === "N") {
        noes.push(email.toString());
      } else if(email.toString().length > 0) {
        yeses.push(email.toString())
      }
    }
    dbo.collection("emails").find().toArray(function(err, items) {
      var emails = Array.from(new Set(items.map((item)=>item.email.toString())));
      if (emails.some((email)=>noes.some((no)=>email===no))){
        console.log("Failed");
      } else if(yeses.length !== emails.length){
        console.log(`Failed ${yeses.length}!==${emails.length}`);
      } else if (items.length != 4*yeses.length){
        console.log(`Failed ${item.length}!==4*${yeses.length}`);
      }
      else {
        console.log("Passed")
      }
      db.close();
    });
  });
});