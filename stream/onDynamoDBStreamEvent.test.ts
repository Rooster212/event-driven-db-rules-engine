import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";
import { DynamoDBStreamEvent } from "aws-lambda";
import {
  createOnDynamoDBStreamHandler,
  OnDynamoDBStreamEventLogFunc,
} from "./onDynamoDBStreamEvent";
import { mockClient } from "aws-sdk-client-mock";
import "aws-sdk-client-mock-jest";

const testDynamoDBStreamEventInvalid: DynamoDBStreamEvent = {
  Records: [
    {
      eventID: "1",
      eventName: "INSERT",
      eventVersion: "1.0",
      eventSource: "aws:dynamodb",
      awsRegion: "us-east-1",
      dynamodb: {
        Keys: {
          Id: {
            N: "101",
          },
        },
        NewImage: {
          Message: {
            S: "New item!",
          },
          Id: {
            N: "101",
          },
        },
        SequenceNumber: "111",
        SizeBytes: 26,
        StreamViewType: "NEW_AND_OLD_IMAGES",
      },
      eventSourceARN: "stream-ARN",
    },
    {
      eventID: "2",
      eventName: "MODIFY",
      eventVersion: "1.0",
      eventSource: "aws:dynamodb",
      awsRegion: "us-east-1",
      dynamodb: {
        Keys: {
          Id: {
            N: "101",
          },
        },
        NewImage: {
          Message: {
            S: "This item has changed",
          },
          Id: {
            N: "101",
          },
        },
        OldImage: {
          Message: {
            S: "New item!",
          },
          Id: {
            N: "101",
          },
        },
        SequenceNumber: "222",
        SizeBytes: 59,
        StreamViewType: "NEW_AND_OLD_IMAGES",
      },
      eventSourceARN: "stream-ARN",
    },
  ],
};

const testDynamoDBStreamEventValid: DynamoDBStreamEvent = {
  Records: [
    {
      eventID: "1",
      eventName: "INSERT",
      eventVersion: "1.0",
      eventSource: "aws:dynamodb",
      awsRegion: "us-east-1",
      dynamodb: {
        Keys: {
          _id: {
            S: "ACCOUNT/abc123abc123",
          },
          _rng: {
            S: "OUTBOUND/AccountCreated",
          },
        },
        NewImage: {
          _id: {
            S: "ACCOUNT/abc123abc123",
          },
          _rng: {
            S: "OUTBOUND/AccountCreated",
          },
          _facet: {
            S: "ACCOUNT",
          },
          _typ: {
            S: "AccountCreated",
          },
          _ts: {
            N: "1234567890",
          },
          _date: {
            S: new Date().toISOString(),
          },
          _seq: {
            N: "1",
          },
          accountId: {
            S: "abc123abc123",
          },
          balance: {
            N: "51235",
          },
        },
        SequenceNumber: "111",
        SizeBytes: 26,
        StreamViewType: "NEW_AND_OLD_IMAGES",
      },
      eventSourceARN: "stream-ARN",
    },
  ],
};

const mockEventBridgeClient = mockClient(EventBridgeClient);

describe("onDynamoDBStreamEvent", () => {
  beforeEach(() => {
    mockEventBridgeClient.resetHistory();
    process.env.CONFIGURED_EVENT_SOURCE = "abc123";
    process.env.PUBLISH_TO_EVENT_BUS_NAME = "my-event-bus";
  });

  afterAll(() => {
    delete process.env.CONFIGURED_EVENT_SOURCE;
    delete process.env.PUBLISH_TO_EVENT_BUS_NAME;
  });

  it("successfully writes the expected events", async () => {
    mockEventBridgeClient.on(PutEventsCommand).resolves({
      Entries: [
        {
          EventId: "0",
        },
        {
          EventId: "1",
        },
      ],
    });

    const testLogger: OnDynamoDBStreamEventLogFunc = jest.fn();
    const handler = createOnDynamoDBStreamHandler(testLogger);

    await handler(testDynamoDBStreamEventValid);

    expect(testLogger).toBeCalledWith("processing records", { count: 1 });
    expect(mockEventBridgeClient).toHaveReceivedCommandTimes(PutEventsCommand, 1);
    expect(mockEventBridgeClient).toHaveReceivedCommandWith(PutEventsCommand, {
      Entries: [
        {
          EventBusName: "my-event-bus",
          Source: "abc123",
          DetailType: "AccountCreated",
          Detail: JSON.stringify({
            accountId: "abc123abc123",
            balance: 51235,
          }),
        },
      ],
    });
  });

  it("processes no records if the records are not valid", async () => {
    mockEventBridgeClient.on(PutEventsCommand).rejects();

    const testLogger: OnDynamoDBStreamEventLogFunc = jest.fn();
    const handler = createOnDynamoDBStreamHandler(testLogger);

    await handler(testDynamoDBStreamEventInvalid);

    expect(testLogger).toBeCalledWith("processing records", { count: 2 });
    expect(mockEventBridgeClient).toHaveReceivedCommandTimes(PutEventsCommand, 0);
  });

  it("throws an error when an exception occurs sending an event", async () => {
    const testLogger: OnDynamoDBStreamEventLogFunc = jest.fn();

    mockEventBridgeClient.on(PutEventsCommand).rejects(new Error("woops"));
    const handler = createOnDynamoDBStreamHandler(testLogger);

    await expect(handler(testDynamoDBStreamEventValid)).rejects.toThrowError();
    expect(testLogger).toBeCalledWith("processing records", { count: 1 });
    expect(mockEventBridgeClient).toHaveReceivedCommandTimes(PutEventsCommand, 1);
  });

  it("throws an error when errors are returned for sending an event", async () => {
    const testLogger: OnDynamoDBStreamEventLogFunc = jest.fn();

    mockEventBridgeClient.on(PutEventsCommand).resolves({
      Entries: [
        {
          ErrorCode: "1",
          ErrorMessage: "Failed!",
        },
      ],
    });
    const handler = createOnDynamoDBStreamHandler(testLogger);

    await expect(handler(testDynamoDBStreamEventValid)).rejects.toThrowError(new Error("Failed!"));
    expect(testLogger).toBeCalledWith("processing records", { count: 1 });
    expect(mockEventBridgeClient).toHaveReceivedCommandTimes(PutEventsCommand, 1);
  });

  it("throws an error if the configured event source is not set", async () => {
    delete process.env.CONFIGURED_EVENT_SOURCE;

    const testLogger: OnDynamoDBStreamEventLogFunc = jest.fn();
    const handler = createOnDynamoDBStreamHandler(testLogger);

    await expect(handler(testDynamoDBStreamEventValid)).rejects.toThrowError(
      new Error("CONFIGURED_EVENT_SOURCE is not set"),
    );
  });

  it("throws an error if the event bus to publish to is not set", async () => {
    delete process.env.PUBLISH_TO_EVENT_BUS_NAME;

    const testLogger: OnDynamoDBStreamEventLogFunc = jest.fn();
    const handler = createOnDynamoDBStreamHandler(testLogger);

    await expect(handler(testDynamoDBStreamEventValid)).rejects.toThrowError(
      new Error("PUBLISH_TO_EVENT_BUS_NAME is not set"),
    );
  });
});
