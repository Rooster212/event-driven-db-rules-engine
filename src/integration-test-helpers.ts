import {
  CreateTableCommand,
  DeleteTableCommand,
  DeleteTableCommandOutput,
  DynamoDBClient,
} from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

interface DB {
  name: string;
  client: DynamoDBDocumentClient;
  delete: () => Promise<DeleteTableCommandOutput>;
}

const randomTableName = () =>
  `eventdb_test_${new Date().getTime()}_${Math.floor(Math.random() * 1000)}`;

const createLocalTable = async (): Promise<DB> => {
  const options = {
    region: "eu-west-1",
    endpoint: "http://localhost:8000",
    credentials: {
      accessKeyId: "5dyqqr",
      secretAccessKey: "fqm4vf",
    },
  };

  const ddb = new DynamoDBClient(options);

  const tableName = randomTableName();
  const createTableCommand = new CreateTableCommand({
    KeySchema: [
      {
        KeyType: "HASH",
        AttributeName: "_id",
      },
      {
        KeyType: "RANGE",
        AttributeName: "_rng",
      },
    ],
    TableName: tableName,
    AttributeDefinitions: [
      {
        AttributeName: "_id",
        AttributeType: "S",
      },
      {
        AttributeName: "_rng",
        AttributeType: "S",
      },
    ],
    BillingMode: "PAY_PER_REQUEST",
  });
  await ddb.send(createTableCommand);

  const deleteTableFunc = async () => {
    const deleteTableCommand = new DeleteTableCommand({ TableName: tableName });
    return await ddb.send(deleteTableCommand);
  };

  return {
    name: tableName,
    client: DynamoDBDocumentClient.from(ddb, {
      marshallOptions: {
        convertEmptyValues: true,
      },
    }),
    delete: deleteTableFunc,
  };
};

export { DB as TestDB, createLocalTable as TestCreateLocalTable };
