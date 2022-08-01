import { createOnDynamoDBStreamHandler } from "../../stream/onDynamoDBStreamEvent";

export const handler = createOnDynamoDBStreamHandler("myEventSource")