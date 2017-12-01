var express = require('express');
var app = express(); 
var cors = require('cors');
var bodyParser = require('body-parser');
// var bodyParser = require('body-parser');
env = process.env.NODE_ENV = process.env.NODE_ENV || 'development',
envConfig = require('./env')[env];
var mongodb = require('mongodb').MongoClient;
app.use(bodyParser.urlencoded({
    extended: true
  }));
  app.use(bodyParser.json());
app.use(cors());
app.post("/subscription", function(req,res){
    console.log(req.body);
    
    var subscription = {
        userId: req.body.userId,
        type: req.body.subscriptionType,
        validUpto: req.body.validUpto 
    }
    mongodb.connect(envConfig.db,function(err,db){
        var query = { "userId": subscription.userId };
        var collection = db.collection('subscriptions');
        collection.findOne(query, function(err, existing) {
            if(!existing){
                console.log("new entry");
                collection.insert(subscription,function(err,result){
                    db.close();  
                    res.status(200);  
                });
            }
            else{
                console.log("updating entry");
                collection.updateOne(query,
                {
                    $set: { "type": subscription.type },
                    $set: { "validUpto": subscription.validUpto },
                },
                function(err, result) {
                    console.log(result);
                    db.close();  
                    res.status(200);
                });
            }    
        });        
      });
  });

  var amqp = require('amqplib');
  
  amqp.connect('amqp://admin:admin@ec2-13-127-47-215.ap-south-1.compute.amazonaws.com').then(function(conn) {
    // process.once('SIGINT', function() { conn.close(); });
    return conn.createChannel().then(function(ch) {
      var ok = ch.assertExchange('users', 'fanout', {durable: true});
      ok = ok.then(function() {
        return ch.assertQueue('userCreate',{durable: true});
      });
      ok = ok.then(function(qok) {
        return ch.bindQueue(qok.queue, 'users', '').then(function() {
          return qok.queue;
        });
      });
      ok = ok.then(function(queue) {
        return ch.consume(queue, logMessage, {noAck: false});
      });
      return ok.then(function() {
        console.log('Waiting for events...');
      });
  
      function logMessage(msg) {
        console.log("received");
        var userCreateEvent = JSON.parse(msg.content);
        mongodbConnectionCallBack = function(err,db){
          var query = {"userId" : userCreateEvent.userId };
          var mongoDbcollection = db.collection('subscriptions');
          findOneCallBack = function(err, existingSubscription) {
            if(!existingSubscription){
              var subscription = {
                userId: userCreateEvent.userId,
                type: userCreateEvent.subscriptionType,
                validUpto: userCreateEvent.validUpTo
              }
              subscriptionInsertCallBack = function(err,insertedSubscription){
                if(err)
                  console.log("error in creating subscription");
                else{
                  console.log("new subscription added");
                  console.log(insertedSubscription);
                }
                ch.ack(msg);
              }
              mongoDbcollection.insert(subscription,subscriptionInsertCallBack);
            }
            else{
              console.log("userId conflict");
            }    
          }
          mongoDbcollection.findOne(query, findOneCallBack);
        }
        mongodb.connect(envConfig.db,mongodbConnectionCallBack);
      }
    });
  }).catch(console.warn);

app.listen(envConfig.port, () => {
  console.log(`subscription server running at http://localhost:${envConfig.port}/`);
});