import { ExportTableToPointInTimeCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { Facet } from ".";
import { EventDB } from "./db";
import { TestCreateLocalTable, TestDB } from "./integration-test-helpers";
import { Processor, RecordTypeName, StateUpdater, StateUpdaterInput, Event } from "./processor";

// This example handles duplicate Webhooks from a payment provider.

// The order Facet has multiple records.
// State: The current "Order".
// Inbound Events: The account is made up of "OrderCreated" and "PaymentCompleted" records.
// Outbound Event 1: An "OrderComplete" event is emitted when the balance becomes 0.
interface Order {
  id: string;
  balance: number;
  status: OrderStatus;
  items: Array<OrderItem>;
  intentIds: Array<string>;
}
const OrderRecordName = "ORDER";

enum OrderStatus {
  Created = "created",
  Paid = "paid",
  Shipped = "shipped",
}

interface OrderItem {
  desc: string;
  qty: number;
  cost: number;
}

// Inbound events must have a name.
type InboundEvents = OrderCreated | PaymentCompleted;

interface OrderCreated {
  id: string;
  items: Array<OrderItem>;
}
const OrderCreatedRecordName = "ORDER_CREATED";
interface PaymentCompleted {
  intent: PaymentIntent;
}
const PaymentCompletedRecordName = "PAYMENT_COMPLETED";

interface PaymentIntent {
  id: string;
  // ID of the invoice that the customer is paying off.
  invoice: string;
  amount: number;
}

// Outbound events.
type OutboundEvents = OrderPaid;

interface OutboundEvent {
  eventSource: string;
  eventCreatedDate: Date;
  eventVersion: string;
}
interface OrderPaid extends OutboundEvent {
  order: Order;
}
const OrderPaidEventName = "ORDER_PAID";

let testDB: TestDB;
describe("Duplicate message handling example e2e test", () => {
  beforeEach(async () => {
    testDB = await TestCreateLocalTable();
  });

  afterEach(() => {
    if (testDB) {
      testDB.delete();
    }
  });

  it("Runs a duplicate message handling example", async () => {
    const db = new EventDB<Order, InboundEvents, OutboundEvents>(
      DynamoDBDocumentClient.from(testDB.client),
      testDB.name,
      OrderRecordName,
    );

    const duplicatesRecieved = new Array<string>();

    // The rules define how the Order state is updated by incoming events.
    // The function must be pure, it must not carry out IO (e.g. network requests, or disk
    // access), and it should execute quickly. If it does not, it is more likely that in
    // between the transaction starting (reading all the previous events), and completing (updating
    // the state), another event will have been inserted, resulting in the transaction
    // failing and needing to be executed again.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const rules = new Map<
      RecordTypeName,
      StateUpdater<Order, InboundEvents, OutboundEvents, any>
    >();
    // Handle the creation of new orders.
    rules.set(
      OrderCreatedRecordName,
      (input: StateUpdaterInput<Order, InboundEvents, OutboundEvents, OrderCreated>): Order => {
        input.state.id = input.current.id;
        input.state.items = input.current.items;
        // Calculate how much the customer owes.
        input.state.balance = input.current.items.reduce(
          (acccumulator, current) => (acccumulator += current.cost),
          0,
        );
        input.state.status = OrderStatus.Created;
        return input.state;
      },
    );
    rules.set(
      PaymentCompletedRecordName,
      (input: StateUpdaterInput<Order, InboundEvents, OutboundEvents, PaymentCompleted>): Order => {
        // If there's no initial state, then this order doesn't exist.
        if (!input.state) {
          throw new Error(`Order "${input.current.intent.invoice}" doesn't exist.`);
        }

        // Find any previous processing of transactions with this intent ID.
        // If we find one, then this is a duplicate and can safely be ignored.
        // In this case, there's a unique ID on the event, but if not, you might need to hash the events.
        input.state.intentIds = input.state.intentIds ?? new Array<string>();
        const previousPaymentCompletedEvent = input.state.intentIds.find(
          (intendId) => intendId === input.current.intent.id,
        );

        // Exit early, it's a duplicate message.
        if (previousPaymentCompletedEvent) {
          // For test purposes, push the duplicate message ID to an array.
          duplicatesRecieved.push(input.current.intent.id);
          return input.state;
        }

        // Ensure that we track that we've processed this event.
        input.state.intentIds.push(input.current.intent.id);

        // Take the payment off the balance.
        input.state.balance -= input.current.intent.amount;

        // If the customer has finished paying, we can tell other systems to start shipping the order.
        if (input.state.balance === 0) {
          const orderPaidEvent = { order: input.state } as OrderPaid;
          input.state.status = OrderStatus.Paid;
          input.publish(OrderPaidEventName, orderPaidEvent);
        }

        // In real life, you may want to handle the case that the customer has somehow paid too much.
        return input.state;
      },
    );

    // Create the processor that handles events.
    const processor = new Processor<Order, InboundEvents, OutboundEvents>(rules);

    // Can now create an order "Facet" in our DynamoDB table.
    const order = new Facet<Order, InboundEvents, OutboundEvents>(OrderRecordName, db, processor);

    // Let's create a new order.
    const orderId = Math.round(Math.random() * 1000000).toString();

    // We start an order with an order created message.
    await order.append(
      orderId,
      new Event<OrderCreated>(OrderCreatedRecordName, {
        id: orderId,
        items: new Array<OrderItem>({ desc: "Spinet", cost: 150000, qty: 1 } as OrderItem),
      }),
    );

    // If our front-end creates a payment intent with a card processor, we'll receive
    // webhook notifications of the result of that payment intent. Sometimes, those can
    // be sent twice. We don't want to send two notifications to
    // our consumers.

    // We'll receive an event like this from the Webhook.
    const payment1 = {
      intent: {
        amount: 100000,
        invoice: orderId,
        id: "unique_intent_id_1",
      },
    } as PaymentCompleted;

    // Lets try processing it twice.
    await order.append(
      payment1.intent.invoice,
      new Event<PaymentCompleted>(PaymentCompletedRecordName, payment1),
    );
    await order.append(
      payment1.intent.invoice,
      new Event<PaymentCompleted>(PaymentCompletedRecordName, payment1),
    );

    const orderState = await order.get(orderId);
    if (!orderState) {
      fail("Order state is not defined");
    }

    // We should find that the balance is still 50000 ($500), despite paying off 100000 ($1000) twice.
    expect(orderState.item.balance).toBe(50000);

    // Now, let's process another payment and check that the outbound event is only sent once.
    const payment2 = {
      intent: {
        amount: 50000,
        invoice: orderId,
        id: "unique_intent_id_2",
      },
    } as PaymentCompleted;

    // Lets try processing it twice.
    const process1 = await order.append(
      payment2.intent.invoice,
      new Event<PaymentCompleted>(PaymentCompletedRecordName, payment2),
    );
    expect(process1.newOutboundEvents.length).toBe(1);

    const process2 = await order.append(
      payment2.intent.invoice,
      new Event<PaymentCompleted>(PaymentCompletedRecordName, payment2),
    );

    // We expect no outbound events on the second webhook event as we should
    // not publish another event when we receive a duplicate inbound event
    expect(process2.newOutboundEvents.length).toBe(0);

    // We also added duplicate publishes to the duplicatesRecieved array
    // Let's check this array - we expected 2 duplicates
    expect(duplicatesRecieved).toHaveLength(2);

    // We should have one of each duplicate message ID, in the correct order in the array.
    expect(duplicatesRecieved[0]).toBe(payment1.intent.id);
    expect(duplicatesRecieved[1]).toBe(payment2.intent.id);

    // We should find that the balance is now zero, and the state is paid.
    const finalState = await order.db.getState(orderId);
    expect(finalState.balance).toBe(0);
    expect(finalState.status).toBe(OrderStatus.Paid);
  });
});
