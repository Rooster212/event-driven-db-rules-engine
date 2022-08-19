import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
  TransactWriteCommand,
} from "@aws-sdk/lib-dynamodb";

// A record is written to DynamoDB.
export interface BaseRecord {
  // Identifier of the record group.
  _id: string;
  // Event sort key.
  _rng: string;
  // Facet of the event.
  _facet: string;
  // Type of the event.
  _typ: string;
  // Timestamp of the record.
  _ts: number;
  // ISO date.
  _date: string;
  // Sequence of the record.
  _seq: number;
}

// A StateRecord represents the current state of an item.
export type StateRecord<TState> = BaseRecord & TState;
// InboundRecords represent all of the change events assocated with an item.
export type InboundRecord = BaseRecord;
// OutboundRecords are the events sent to external systems due to item changes.
export type OutboundRecord = BaseRecord;

const facetId = (facet: string, id: string) => `${facet}/${id}`;
const secondaryIndexId = (facet: string, secondaryIndex: string, id: string) =>
  `${facet}/${secondaryIndex}/${id}`;

const newRecord = <TItem>(
  facet: string,
  id: string,
  seq: number,
  rng: string,
  type: string,
  item: TItem,
  time: Date,
): BaseRecord & TItem => ({
  _facet: facet,
  _id: facetId(facet, id),
  _seq: seq,
  _rng: rng,
  _typ: type,
  _ts: time.getTime(),
  _date: time.toISOString(),
  ...item,
});

const isFacet = (facet: string, r: BaseRecord) => r._facet === facet;

// Create a new state record to represent the state of an item.
// facet: the name of the DynamoDB facet.
export const newStateRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  item: T,
  time: Date,
): StateRecord<T> => newRecord(facet, id, seq, "STATE", facet, item, time);

export const isStateRecord = <T>(r: StateRecord<T>): boolean => r._rng === "STATE";

const inboundRecordRangeKey = (type: string, seq: number) => `INBOUND/${type}/${seq}`;

export const newInboundRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  type: string,
  item: T,
  time: Date,
): InboundRecord & T =>
  newRecord(facet, id, seq, inboundRecordRangeKey(type, seq), type, item, time);

export const isInboundRecord = (r: InboundRecord): boolean => r._rng.startsWith("INBOUND");

const outboundRecordRangeKey = (type: string, seq: number, index: number) =>
  `OUTBOUND/${type}/${seq}/${index}`;

export const newOutboundRecord = <T>(
  facet: string,
  id: string,
  seq: number,
  index: number,
  type: string,
  item: T,
  time: Date,
): OutboundRecord & T =>
  newRecord(facet, id, seq, outboundRecordRangeKey(type, seq, index), type, item, time);

export const isOutboundRecord = (r: OutboundRecord): boolean => r._rng.startsWith("OUTBOUND");

const createPutItem = (tableName: string, r: BaseRecord) => ({
  TableName: tableName,
  Item: r,
  ConditionExpression: "attribute_not_exists(#_id)",
  ExpressionAttributeNames: {
    "#_id": "_id",
  },
});

const createPutState = (tableName: string, r: BaseRecord, previousSeq: number) => ({
  TableName: tableName,
  Item: r,
  ConditionExpression: "attribute_not_exists(#_id) OR #_seq = :_seq",
  ExpressionAttributeNames: {
    "#_id": "_id",
    "#_seq": "_seq",
  },
  ExpressionAttributeValues: {
    ":_seq": previousSeq,
  },
});

const createPutSecondaryIndex = (tableName: string, r: BaseRecord) => ({
  TableName: tableName,
  Item: r,
});

export interface DB<TState, TInputEvents, TOutputEvents> {
  getState(id: string): Promise<StateRecord<TState>>;
  putState(
    state: StateRecord<TState>,
    previousSeq: number,
    inbound: Array<InboundRecord & TInputEvents>,
    outbound: Array<OutboundRecord & TOutputEvents>,
    secondaryIndexRecords: Array<StateRecord<TState>>,
  ): Promise<void>;
  queryRecords(id: string): Promise<Array<QueryRecordsResult<TState, TInputEvents, TOutputEvents>>>;
  queryRecordsBySecondaryIndex(
    byIndex: string,
    id: string,
  ): Promise<Array<QueryRecordsResult<TState, TInputEvents, TOutputEvents>>>;
  queryRecordsByRangePrefix(
    rng: string,
    id: string,
  ): Promise<Array<QueryRecordsResult<TState, TInputEvents, TOutputEvents>>>;
  queryRecordsBySecondaryIndexAndRangePrefix(
    rng: string,
    byIndex: string,
    id: string,
  ): Promise<Array<QueryRecordsResult<TState, TInputEvents, TOutputEvents>>>;
}

export type QueryRecordsResult<TState, TInputEvents, TOutputEvents> =
  | StateRecord<TState>
  | (InboundRecord & TInputEvents)
  | (OutboundRecord & TOutputEvents);

export class EventDB<TState, TInputEvents, TOutputEvents>
  implements DB<TState, TInputEvents, TOutputEvents>
{
  readonly client: DynamoDBDocumentClient;
  readonly table: string;
  readonly facet: string;
  constructor(client: DynamoDBDocumentClient, table: string, facet: string) {
    this.client = client;
    this.table = table;
    this.facet = facet;
  }

  async getState(id: string): Promise<StateRecord<TState>> {
    const params = new GetCommand({
      TableName: this.table,
      Key: {
        _id: facetId(this.facet, id),
        _rng: "STATE",
      },
      ConsistentRead: true,
    });
    const result = await this.client.send(params);
    return result.Item as StateRecord<TState>;
  }

  async putState(
    state: StateRecord<TState>,
    previousSeq: number,
    inbound: Array<InboundRecord & TInputEvents> = [],
    outbound: Array<OutboundRecord & TOutputEvents> = [],
    secondaryIndexRecords: Array<StateRecord<TState>> = [],
  ): Promise<void> {
    if (!isStateRecord(state)) {
      throw Error("putState: invalid state record");
    }
    if (!isFacet(this.facet, state)) {
      throw Error(
        `putState: state record has mismatched facet. Expected: "${this.facet}", got: "${state._facet}"`,
      );
    }
    if (inbound.some((d) => !isInboundRecord(d))) {
      throw Error("putState: invalid inbound record");
    }
    if (inbound.some((d) => !isFacet(this.facet, d))) {
      throw Error("putState: invalid facet for inbound record");
    }
    if (outbound.some((e) => !isOutboundRecord(e))) {
      throw Error("putState: invalid outbound record");
    }
    if (outbound.some((e) => !isFacet(this.facet, e))) {
      throw Error("putState: invalid facet for outbound record");
    }
    const outboundCount = secondaryIndexRecords?.length + outbound?.length + inbound?.length + 1;
    if (outboundCount > 25) {
      throw Error(
        `putState: cannot exceed maximum DynamoDB transaction count of 25. The transaction attempted to write ${outboundCount}.`,
      );
    }

    const putItems = [
      ...inbound.map((i) => createPutItem(this.table, i)),
      ...outbound.map((o) => createPutItem(this.table, o)),
      ...secondaryIndexRecords.map((s) => createPutSecondaryIndex(this.table, s)),
      createPutState(this.table, state, previousSeq),
    ].map((putItem) => ({ Put: putItem }));

    const transactWriteCommand = new TransactWriteCommand({
      TransactItems: putItems,
    });

    await this.client.send(transactWriteCommand);
  }

  /**
   * @description getRecords returns all records grouped under the ID
   * @param id the ID of the item.
   * */
  async queryRecords(id: string): Promise<Array<StateRecord<TState>>> {
    const result = await this.client.send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: "#_id = :_id",
        ExpressionAttributeNames: {
          "#_id": "_id",
        },
        ExpressionAttributeValues: {
          ":_id": facetId(this.facet, id),
        },
        ConsistentRead: true,
      }),
    );
    return result.Items as Array<StateRecord<TState>>;
  }

  /**
   * @description getRecords returns all records grouped under the ID.
   * @param byIndex The index to retrieve the item by.
   * @param id The ID of the item to retrieve.
   * */
  async queryRecordsBySecondaryIndex(
    byIndex: string,
    id: string,
  ): Promise<Array<StateRecord<TState>>> {
    const result = await this.client.send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: "#_id = :_id",
        ExpressionAttributeNames: {
          "#_id": "_id",
        },
        ExpressionAttributeValues: {
          ":_id": secondaryIndexId(this.facet, byIndex, id),
        },
        ConsistentRead: true,
      }),
    );
    return result.Items as Array<StateRecord<TState>>;
  }

  /**
   * Retrieves records indexed by primary index, with a sort key prefix.
   * @param rng The sort key prefix to retrieve the item by.
   * @param id The partition key of the item.
   * @returns Array of State records
   */
  async queryRecordsByRangePrefix(rng: string, id: string): Promise<Array<StateRecord<TState>>> {
    const result = await this.client.send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: "#_id = :_id and begins_with(#_rng, :_rng)",
        ExpressionAttributeNames: {
          "#_id": "_id",
          "#_rng": "_rng",
        },
        ExpressionAttributeValues: {
          ":_id": facetId(this.facet, id),
          "#_rng": rng,
        },
        ConsistentRead: true,
      }),
    );
    return result.Items as Array<StateRecord<TState>>;
  }

  /**
   * Retrieves records indexed by secondary index, with a sort key prefix.
   * @param rng The sort key prefix to retrieve the item by.
   * @param byIndex The secondary index to retrieve the item by.
   * @param id The secondary index ID to retrieve the item by.
   * @returns
   */
  async queryRecordsBySecondaryIndexAndRangePrefix(
    rng: string,
    byIndex: string,
    id: string,
  ): Promise<Array<StateRecord<TState>>> {
    const result = await this.client.send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: "#_id = :_id and begins_with(#_rng, :_rng)",
        ExpressionAttributeNames: {
          "#_id": "_id",
          "#_rng": "_rng",
        },
        ExpressionAttributeValues: {
          ":_id": secondaryIndexId(this.facet, byIndex, id),
          "#_rng": rng,
        },
        ConsistentRead: true,
      }),
    );
    return result.Items as Array<StateRecord<TState>>;
  }
}
