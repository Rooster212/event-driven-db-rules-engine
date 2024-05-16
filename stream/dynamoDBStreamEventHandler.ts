import { IEventBus } from "aws-cdk-lib/aws-events";
import { Architecture, Runtime, RuntimeFamily } from "aws-cdk-lib/aws-lambda";
import { NodejsFunction, NodejsFunctionProps } from "aws-cdk-lib/aws-lambda-nodejs";
import { RetentionDays } from "aws-cdk-lib/aws-logs";

import { Construct } from "constructs";
import fs from "fs";

export interface DynamoDBStreamEventHandlerProps
  extends Omit<NodejsFunctionProps, "architecture" | "runtime" | "code" | "handler"> {
  /**
   * The event source that the events published should be associated with.
   */
  eventSource: string;

  /**
   * The target event bus for events. The Lambda created in the construct
   * will be granted PutEvents permission for this event bus.
   */
  targetEventBus: IEventBus;

  /**
   * The target runtime for the Lambda function.
   * @default Runtime.NODEJS_LATEST
   */
  targetRuntime?: Runtime;

  /**
   * The target architecture for the Lambda function.
   * @default Architecture.ARM_64
   */
  targetArchitecture?: Architecture;
}

export class DynamoDBStreamEventHandler extends NodejsFunction {
  /**
   * Creates a Lambda function that will write outbound events from the stream to the event bus.
   * @param scope The scope in which to create the handler.
   * @param id the ID of the construct.
   * @param props The properties to pass. See {@link DynamoDBStreamEventHandlerProps}.
   */
  constructor(scope: Construct, id: string, props: DynamoDBStreamEventHandlerProps) {
    if (props.targetRuntime && props.targetRuntime?.family !== RuntimeFamily.NODEJS) {
      throw new Error("The target runtime must be Node.js");
    }

    super(scope, id, {
      bundling: {
        minify: true,
        sourceMap: true,
      },
      ...props,
      architecture: props.targetArchitecture ?? Architecture.ARM_64,
      runtime: props.targetRuntime ?? Runtime.NODEJS_LATEST,
      // I've done it like this so that it works when published as well as from TypeScript
      entry: fs.existsSync(`${__dirname}/onDynamoDBStreamEvent.ts`)
        ? `${__dirname}/onDynamoDBStreamEvent.ts`
        : `${__dirname}/onDynamoDBStreamEvent.js`,
      handler: "defaultHandler",
      logRetention: props.logRetention ? props.logRetention : RetentionDays.ONE_WEEK,
      environment: {
        ...props.environment,
        CONFIGURED_EVENT_SOURCE: props.eventSource,
        PUBLISH_TO_EVENT_BUS_NAME: props.targetEventBus.eventBusName,
      },
    });

    props.targetEventBus.grantPutEventsTo(this);
  }
}
