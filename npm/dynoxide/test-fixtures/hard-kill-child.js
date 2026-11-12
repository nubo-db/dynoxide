"use strict";

// Starts a real server with start(), prints its port, and waits to be killed.
// Given a database path, it first writes an item there, so the test can check
// the write survives the kill.
const http = require("http");
const { start } = require("..");

function call(endpoint, target, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/x-amz-json-1.0",
        "x-amz-target": `DynamoDB_20120810.${target}`,
        "x-amz-date": "20260101T000000Z",
        authorization:
          "AWS4-HMAC-SHA256 Credential=dynoxide/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=0",
      },
    }, (res) => {
      res.resume();
      res.on("end", () => (res.statusCode === 200 ? resolve() : reject(new Error(`${target}: ${res.statusCode}`))));
    });
    req.on("error", reject);
    req.end(data);
  });
}

async function main() {
  const dbPath = process.argv[2];
  const server = await start(dbPath ? { dbPath } : {});
  if (dbPath) {
    await call(server.endpoint, "CreateTable", {
      TableName: "kept",
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
      AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
      BillingMode: "PAY_PER_REQUEST",
    });
    await call(server.endpoint, "PutItem", { TableName: "kept", Item: { pk: { S: "after-kill" } } });
  }
  console.log(JSON.stringify({ port: server.port }));
  setInterval(() => {}, 1000);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
