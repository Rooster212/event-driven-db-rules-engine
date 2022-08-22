import { Stack, StackProps } from "aws-cdk-lib";
import { AttributeType, BillingMode, StreamViewType, Table } from "aws-cdk-lib/aws-dynamodb";
import { EventBus, Rule } from "aws-cdk-lib/aws-events";
import { Runtime, StartingPosition } from "aws-cdk-lib/aws-lambda";
import { DynamoEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { RetentionDays } from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";
import { ClearCycleDynamoDBStreamEventHandler } from "../../stream/dynamoDBStreamEventHandler";

export class ExampleDynamoDBEventDrivenArchitectureStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const dynamoDBTable = new Table(this, "example-dynamo-db-table", {
      tableName: `my-example-dynamo-db-table-${this.account}`,
      partitionKey: {
        name: "_id",
        type: AttributeType.STRING,
      },
      sortKey: {
        name: "_seq",
        type: AttributeType.STRING,
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      stream: StreamViewType.NEW_AND_OLD_IMAGES,
    });

    const eventBus = new EventBus(this, "example-event-bus");

    const dynamoDBStreamHandler = new ClearCycleDynamoDBStreamEventHandler(
      this,
      "example-clear-cycle-dynamodb-stream-event-handler",
      {
        eventSource: "my-event-source",
        eventBusName: eventBus.eventBusName,
      },
    );

    // This allows the stream handler to write to the event bus
    eventBus.grantPutEventsTo(dynamoDBStreamHandler);

    // Note how the stream handler is not given direct access to the DynamoDB table

    // Now, we setup the trigger for the Lambda from the DynamoDB table
    dynamoDBStreamHandler.addEventSource(
      new DynamoEventSource(dynamoDBTable, {
        // This ensures we always read the most recent data from the shard
        startingPosition: StartingPosition.LATEST,
        // A batch size of 1 ensures that the stream handler is invoked once per record
        batchSize: 1,
      }),
    );

    // Now, if you wanted to setup a Lambda that listened for events published to the event bus,
    // you could do so like this (replacing source/detail type with your real event types)
    new Rule(this, "example-event-bus-rule", {
      eventBus: eventBus,
      enabled: true,
      targets: [
        /* this is where you would add your Lambda */
        // new eventsTarget.LambdaFunction(myLambdaFunction)
      ],
      ruleName: "example-event-bus-rule",
      eventPattern: {
        source: ["my-published-event-source"],
        detailType: ["my-published-event-detail-type"],
      },
    });
  }
}
