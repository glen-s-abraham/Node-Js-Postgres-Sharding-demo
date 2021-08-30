const express = require('express');
const {Client} = require('pg')
const HashRing = require('hashring');
const crypto = require('crypto'); 

const hr = new HashRing();
hr.add('5432');
hr.add('5433');
hr.add('5434');

const app = express();

const clients = {
    "5432" : new Client({
        "host" : "192.168.1.7",
        "port" : "5432",
        "user" : "postgres",
        "password" : "password",
        "database" : "postgres",
    }),
    "5433" : new Client({
        "host" : "192.168.1.7",
        "port" : "5433",
        "user" : "postgres",
        "password" : "password",
        "database" : "postgres",
    }),
    "5434" : new Client({
        "host" : "192.168.1.7",
        "port" : "5434",
        "user" : "postgres",
        "password" : "password",
        "database" : "postgres",
    }),
}

connect();
async function connect()
{
    clients['5432'].connect();
    clients['5433'].connect();
    clients['5434'].connect();
}

app.get('/',(req,res)=>{
    const urlId = req.query.urlid;
    const server = hr.get(urlId);
    console.log(server);
    const queryString = `SELECT * FROM url_table WHERE url_id = '${urlId}'`; 
    clients[server].query(queryString).then(result=>{
        if(result.rowCount>0){
            res.send(result.rows);
        }
    })
});

app.post('/',(req,res)=>{
    const url = req.query.url;
    const hash = crypto.createHash("sha256").update(url).digest("base64");
    const urlId = hash.substr(0, 5);
    server = hr.get(urlId);
    const queryString = `INSERT INTO url_table (url, url_id) VALUES('${url}', '${urlId}')`;
    console.log(queryString);
    clients[server].query(queryString);
    res.send({
        "hash":urlId,
        "url": url,
        "server": server
    });
});

app.listen(8000, ()=>console.log("listening on port 8000"));