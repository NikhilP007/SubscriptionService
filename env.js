var path = require('path'),
rootPath = path.normalize(__dirname);


module.exports = {
development: {
    rootPath : rootPath,
    db: 'mongodb://ec2-52-66-145-228.ap-south-1.compute.amazonaws.com/subscribeApp',
    port: process.env.PORT || 8080
},
production: {}
};