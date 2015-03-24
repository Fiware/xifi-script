/************************************************************************/
// Copyright 2014 CREATE-NET, Via alla Cascata, 56, 38123 Trento, Italy //
// This file is part of Xifi projject                                   //
// author attybro                                                       //
//                                                                      //
// This is an API tester for the XIFI monitoring APIs                   //
/************************************************************************/

var OAuth = require('oauth');
var http = require('http');
var APIpath='';
var OAuth2 = OAuth.OAuth2;

//create a key secret using the portal or ask in order to obtain the ConsumerKey and ConsumerSecret
var ConsumerKey='<KEY>';
var ConsumerSecret =  '<SECRET>';
var APIip  ="<IP>";
var APIport="<PORT>";

var oauth2 = new OAuth2(ConsumerKey, ConsumerSecret, 'https://account.lab.fiware.org/',  'oauth/authorize', 'oauth2/token',  null);

function testAPI(){
  if (process.argv.length==5){
    if (process.argv[2] && process.argv[3] && process.argv[4]){
      APIpath=process.argv[4];
      oauth2.getOAuthAccessToken( '', { 'grant_type':'password', 'username':process.argv[2], 'password':process.argv[3] }, manageCred);
    }
  }
  else{
    console.log("Not enough arugments. Usage:\nnode testAPI.js <username> <password> <url_path>");
  }
}

function manageCred(e, access_token, refresh_token, results){
  console.log(access_token);
  makeRequest(access_token)
}

var makeRequest=function(token){
  var bearer=new Buffer(token).toString("base64");
  var options={
               hostname:APIip,
               port:APIport,
               path:APIpath,
               method:"GET",
               headers:{'Authorization':'Bearer '+bearer, 'accept':'application/json'}
  };
  http.get(options,function(res2){
    res2.setEncoding('utf8');
    res2.on('data',function(data){
       console.log(data);
     });
  });
}

testAPI();

