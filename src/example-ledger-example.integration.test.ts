import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { Facet, IndexStateFunc } from ".";
import { EventDB, StateRecord } from "./db";
import { TestCreateLocalTable, TestDB } from "./integration-test-helpers";
import { Processor, RecordTypeName, StateUpdater, StateUpdaterInput, Event } from "./processor";

// The account Facet has multiple records.
// State: The current "BankAccount".
// Inbound Events: The account is made up of "Transaction" and "AccountUpdate" records.
// Outbound Event 1: An "AccountOverdrawn" event is emitted when the balance becomes < 0, and this is allowed due to an overdraft.
// Outbound Event 2: A "TransactionFailed" event is emitted when a transaction cannot be completed, due to insufficient funds.
interface BankAccount {
  id: string;
  ownerFirst: string;
  ownerLast: string;
  balance: number;
  minimumBalance: number;
  customerEmail: string;
}
const BankAccountRecordName = "BANK_ACCOUNT";

// Inbound events must have a name.
type InboundEvents = AccountCreation | AccountUpdate | Transaction;

interface AccountCreation {
  id: string;
  customerEmail: string;
}
const AccountCreationRecordName = "ACCOUNT_CREATION";
interface AccountUpdate {
  ownerFirst: string;
  ownerLast: string;
}
const AccountUpdateRecordName = "ACCOUNT_UPDATE";
interface Transaction {
  desc: string;
  amount: number;
}
const TransactionRecordName = "TRANSACTION_ACCEPTED";

// Outbound events don't need to be named, they're named when they're sent, so it's still a good
// idea to set the name.
type OutboundEvents = AccountOverdrawn;

interface AccountOverdrawn {
  accountId: string;
}
const AccountOverdrawnEventName = "accountOverdrawn";

let testDB: TestDB;
describe("Ledger example e2e test", () => {
  beforeEach(async () => {
    testDB = await TestCreateLocalTable();
  });

  afterEach(() => {
    if (testDB) {
      testDB.delete();
    }
  });

  it("Runs a ledger example", async () => {
    testDB = await TestCreateLocalTable();
    const db = new EventDB<BankAccount, InboundEvents, OutboundEvents>(
      DynamoDBDocumentClient.from(testDB.client),
      testDB.name,
      BankAccountRecordName,
    );

    const rules = new Map<
      RecordTypeName,
      StateUpdater<BankAccount, InboundEvents, OutboundEvents, any>
    >();
    rules.set(
      AccountCreationRecordName,
      (
        input: StateUpdaterInput<BankAccount, InboundEvents, OutboundEvents, AccountCreation>,
      ): BankAccount => {
        input.state.id = input.current.id;
        input.state.customerEmail = input.current.customerEmail;
        return input.state;
      },
    );
    rules.set(
      TransactionRecordName,
      (
        input: StateUpdaterInput<BankAccount, InboundEvents, OutboundEvents, Transaction>,
      ): BankAccount => {
        const previousBalance = input.state.balance;
        const newBalance = input.state.balance + input.current.amount;

        // If they don't have sufficient overdraft, cancel the transaction.
        if (newBalance < input.state.minimumBalance) {
          throw new Error("insufficient funds");
        }

        // If this is the transaction that takes the user overdrawn, notify them.
        if (previousBalance >= 0 && newBalance < 0) {
          const overdrawnEvent = { accountId: input.state.id } as AccountOverdrawn;
          input.publish(AccountOverdrawnEventName, overdrawnEvent);
        }

        input.state.balance = newBalance;
        return input.state;
      },
    );
    rules.set(
      AccountUpdateRecordName,
      (
        input: StateUpdaterInput<BankAccount, InboundEvents, OutboundEvents, AccountUpdate>,
      ): BankAccount => {
        input.state.ownerFirst = input.current.ownerFirst;
        input.state.ownerLast = input.current.ownerLast;
        return input.state;
      },
    );

    // New accounts start with a balance of zero.
    const initialAccount = (): BankAccount =>
      ({
        balance: 0,
        minimumBalance: -1000, // Give the user an overdraft.
      } as BankAccount);

    // Create the processor that handles events.
    const processor = new Processor<BankAccount, InboundEvents, OutboundEvents>(
      rules,
      initialAccount,
    );

    const byCustomerEmailIndex = "byCustomerEmail";
    const indexByCustomerEmailFunc: IndexStateFunc<BankAccount> = (
      state: StateRecord<BankAccount>,
    ) => {
      return {
        ...state,
        _id: `${state._facet}/${byCustomerEmailIndex}/${state.customerEmail}`,
      };
    };

    // Can now create a ledger "Facet" in our DynamoDB table.
    const ledger = new Facet<BankAccount, InboundEvents, OutboundEvents>(
      BankAccountRecordName,
      db,
      processor,
      [indexByCustomerEmailFunc],
    );

    // Let's create a new account.
    const accountId = Math.round(Math.random() * 1000000).toString();

    // There is no new data to add.
    await ledger.append(
      accountId,
      new Event<AccountCreation>(AccountCreationRecordName, {
        id: accountId,
        customerEmail: "test@example.com",
      }),
    );

    // Update the name of the owner.
    await ledger.append(
      accountId,
      new Event<AccountUpdate>(AccountUpdateRecordName, {
        ownerFirst: "John",
        ownerLast: "Brown",
      }),
    );

    // Now, let's add a couple of transactions in a single operation.
    const result = await ledger.append(
      accountId,
      new Event<Transaction>(TransactionRecordName, {
        desc: "Transaction A",
        amount: 200,
      }),
      new Event<Transaction>(TransactionRecordName, {
        desc: "Transaction B",
        amount: -300,
      }),
    );
    expect(result.newOutboundEvents).toHaveLength(1);
    expect(result.newOutboundEvents[0].type).toEqual("accountOverdrawn");
    expect(result.newOutboundEvents[0].event).toEqual({ accountId: accountId });

    // Another separate transaction.
    const transactionCResult = await ledger.append(
      accountId,
      new Event<Transaction>(TransactionRecordName, {
        desc: "Transaction C",
        amount: 50,
      }),
    );

    // If we've just read the STATE, we can try appending without doing
    // another database read. If no other records have been written in the
    // meantime, the transaction will succeed.
    await ledger.appendTo(
      accountId,
      transactionCResult.item,
      transactionCResult.seq,
      new Event<Transaction>(TransactionRecordName, {
        desc: "Transaction D",
        amount: 25,
      }),
    );

    // Get the final balance.
    const balance = await ledger.get(accountId);
    if (balance) {
      expect(balance.item.balance).toEqual(-25);
    }

    // Verify the final balance by reading all of the transactions and re-calculating.
    const verifiedBalance = await ledger.recalculate(accountId);
    expect(verifiedBalance.item.balance).toBe(-25);

    // The re-calculation can also take data to modify the result.
    const finalBalance = await ledger.recalculate(
      accountId,
      new Event<Transaction>(TransactionRecordName, {
        desc: "Transaction E",
        amount: 25,
      }),
    );
    expect(finalBalance.item.balance).toBe(0);

    // We also can retrieve the customer account by their email
    const customerAccount = await ledger.query(byCustomerEmailIndex, "test@example.com");
    expect(customerAccount).toHaveLength(1);
    expect(customerAccount[0].item.balance).toEqual(0);
    expect(customerAccount[0].record._id).toEqual("BANK_ACCOUNT/byCustomerEmail/test@example.com");
  });
});
