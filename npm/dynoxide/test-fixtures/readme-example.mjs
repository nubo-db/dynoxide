import { start } from "dynoxide";
import { DynamoDBClient, ListTablesCommand } from "@aws-sdk/client-dynamodb";

const server = await start();
const client = new DynamoDBClient({
  endpoint: server.endpoint,
  region: server.region,
  credentials: server.credentials,
});

const { TableNames } = await client.send(new ListTablesCommand({}));
console.log(TableNames);

await server.stop();
