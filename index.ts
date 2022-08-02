export { GetOutput, ChangeOutput, DB, Facet } from "./src";

export {
  Processor,
  Event,
  Initializer,
  ProcessResult,
  RecordTypeName,
  StateUpdater,
  StateUpdaterInput,
} from "./src/processor";

export {
  InboundRecord,
  OutboundRecord,
  StateRecord,
  BaseRecord,
  EventDB,
  isInboundRecord,
  isOutboundRecord,
  isStateRecord,
  newInboundRecord,
  newOutboundRecord,
  newStateRecord,
} from "./src/db";

export {
  OnDynamoDBStreamEvent,
  OnDynamoDBStreamEventLogFunc,
  createOnDynamoDBStreamHandler,
} from "./stream/onDynamoDBStreamEvent";
