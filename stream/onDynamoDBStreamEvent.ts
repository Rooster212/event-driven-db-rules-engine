/* eslint-disable @typescript-eslint/no-explicit-any */
import { AttributeValue } from "@aws-sdk/client-dynamodb";
import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { DynamoDBStreamEvent, DynamoDBRecord } from "aws-lambda";
import { BaseRecord } from "../src/db";

export type OnDynamoDBStreamEvent = (event: DynamoDBStreamEvent) => Promise<void>;

export type OnDynamoDBStreamEventLogFunc = (
  logMessage: string,
  data: Record<string, unknown>,
) => void;

export const createOnDynamoDBStreamHandler = (internalLog = defaultLog): OnDynamoDBStreamEvent => {
  const handler = async (event: DynamoDBStreamEvent): Promise<void> => {
    const eventSource = process.env.CONFIGURED_EVENT_SOURCE;
    const eventBusName = process.env.PUBLISH_TO_EVENT_BUS_NAME;

    if (!eventSource) {
      throw new Error("CONFIGURED_EVENT_SOURCE is not set");
    }
    if (!eventBusName) {
      throw new Error("PUBLISH_TO_EVENT_BUS_NAME is not set");
    }

    internalLog("processing records", { count: event.Records.length });
    for (let i = 0; i < event.Records.length; i++) {
      const r = event.Records[i];
      if (!hasRequiredKeys(r)) {
        continue;
      }

      // we use `any` here so that we don't have to specify a type, as this is a
      // generic implementation - we are just going to publish the entire record
      const record = unmarshall(
        r.dynamodb?.NewImage as Record<string, AttributeValue>,
      ) as unknown as BaseRecord & any;

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { _id, _rng, _facet, _typ, _ts, _date, _seq, ...itm } = record;
      if (_typ && itm) {
        internalLog("publishing outbound event", { id: _id, _typ });
        await publish<string>(eventSource, _typ, itm, eventBusName);
        internalLog("published outbound event", {
          id: r.dynamodb?.NewImage?._id.S,
          _typ,
          count: 1,
        });
      }
    }
    internalLog("processed records", { count: event.Records.length });
  };

  return handler;
};

const hasStringKey = (r: DynamoDBRecord, k: string): boolean => !!r.dynamodb?.NewImage?.[k]?.S;

const hasSortKeyThatStartsWithOutbound = (r: DynamoDBRecord): boolean =>
  (hasStringKey(r, "_rng") && r.dynamodb?.NewImage?._rng?.S?.startsWith("OUTBOUND")) || false;
const hasRequiredKeys = (r: DynamoDBRecord): boolean =>
  hasSortKeyThatStartsWithOutbound(r) && hasStringKey(r, "_facet") && hasStringKey(r, "_typ");

const publish = async <TEvent>(
  source: string,
  detailType: string,
  detail: TEvent,
  eventBusName: string,
) => {
  const eventBus = new EventBridgeClient({});

  const putEventCommand = new PutEventsCommand({
    Entries: [
      {
        EventBusName: eventBusName,
        Source: source,
        DetailType: detailType,
        Detail: typeof detail === "string" ? detail : JSON.stringify(detail),
      },
    ],
  });

  const res = await eventBus.send(putEventCommand);
  const errors: string[] = [];
  res.Entries?.forEach((entry) => {
    if (entry.ErrorMessage) {
      errors.push(entry.ErrorMessage);
      return;
    }
  });
  if (errors.length > 0) {
    throw new Error(errors.join(", "));
  }
};

const defaultLog: OnDynamoDBStreamEventLogFunc = (msg: string, data: Record<string, unknown>) =>
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      level: "INFO",
      msg,
      ...data,
    }),
  );

export const defaultHandler = createOnDynamoDBStreamHandler(defaultLog);
