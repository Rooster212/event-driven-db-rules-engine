import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";
import { DynamoDBStreamEvent, DynamoDBRecord } from "aws-lambda";

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
      const typ = r.dynamodb?.NewImage?._typ.S;
      const itm = r.dynamodb?.NewImage?._itm.S;
      if (typ && itm) {
        internalLog("publishing outbound event", { id: r.dynamodb?.NewImage?._id.S, typ });
        await publish<string>(eventSource, typ, itm, eventBusName);
        internalLog("published outbound event", { id: r.dynamodb?.NewImage?._id.S, typ, count: 1 });
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
  hasSortKeyThatStartsWithOutbound(r) &&
  hasStringKey(r, "_facet") &&
  hasStringKey(r, "_typ") &&
  hasStringKey(r, "_itm");

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
