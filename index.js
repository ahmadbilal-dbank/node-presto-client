const express = require("express");
const PrestoDriver = require('./presto-driver');

const prestod = new PrestoDriver({ user: 'root', source:  'nodejs-client'});
const app = express();
const port = 8082;

app.get("/", (req, res) => {
  res.send("Hello world!");
});

app.get("/presto", async (req, res) => {
  // res.send(await prestod.testConnection( `SHOW CATALOGS`));
  // res.send(await prestod.testConnection( `SHOW SCHEMAS FROM tpcds`));
  // res.send(await prestod.testConnection( `SHOW TABLES FROM tpcds.sf1`));
  // res.send(await prestod.testConnection( `SHOW TABLES FROM tpch.tiny LIKE ?`, [`%p%`]));
  // res.send(await prestod.testConnection( `SHOW COLUMNS FROM customer`));

  // res.send(await prestod.testConnection( `ALTER TABLE IF EXISTS customer ADD COLUMN IF NOT EXISTS age int`));
  // Error: This connector does not support adding columns

  // res.send(await prestod.testConnection( `SELECT COUNT(*) FROM customer`));
  // res.send(await prestod.testConnection( `SELECT * FROM customer LIMIT 10`));
  
  // res.send(await prestod.testConnection( `UPDATE customer SET c_first_name = ? WHERE c_customer_id = ?`, ['Upper Mall Lahore', 'AAAAAAAABAAAAAAA']));
  // Error: Update is not valid input

  // res.send(await prestod.testConnection( `insert into customer (custkey, name, address, nationkey, phone, acctbal, mktsegment, comment) values (112123, \'Customer#new\', \'lahore\', 15, \'25-989-741-2988\', 711.56, \'BUILDING\', \'dummy comment\')`));
 
 
  res.send(await prestod.testConnection( `insert into customer (c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk) values (112123, \'Customer#new\', 980124, 15)`));

  // res.send(await prestod.testConnection( `CREATE TABLE IF NOT EXISTS ordersv2 (
  //   orderkey bigint,
  //   orderstatus varchar,
  //   totalprice double,
  //   orderdate date
  // )`));

  // CREATE TABLE IF NOT EXISTS ordersv2 (
  //   orderkey bigint,
  //   orderstatus varchar,
  //   totalprice double,
  //   orderdate date
  // )
  


  // res.send(await prestod.testConnection(`CREATE TABLE IF NOT EXISTS ordersv2 (
  //   ? bigint,
  //   ? varchar,
  //   ? double COMMENT ?,
  //   ? date
  // )
  // COMMENT  ?`, ['orderKey', 'orderstatus', 'totalprice', 'Price in cents.', 'orderdate', 'A table to keep track of orders.']));

});


app.listen(port, (req, res) => {
  console.log(`Presto Client listening at http://localhost:${port}`);
});




