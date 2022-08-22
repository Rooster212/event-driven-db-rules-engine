import { Architecture, Runtime } from "aws-cdk-lib/aws-lambda";
import { NodejsFunction, NodejsFunctionProps } from "aws-cdk-lib/aws-lambda-nodejs";
import { RetentionDays } from "aws-cdk-lib/aws-logs";

import { Construct } from "constructs";
import fs from "fs";

export interface ClearCycleDynamoDBStreamEventHandlerProps
  extends Omit<NodejsFunctionProps, "architecture" | "runtime" | "code" | "handler"> {
  eventSource: string;
  eventBusName: string;
}

export class ClearCycleDynamoDBStreamEventHandler extends NodejsFunction {
  constructor(scope: Construct, id: string, props: ClearCycleDynamoDBStreamEventHandlerProps) {
    super(scope, id, {
      bundling: {
        minify: true,
        sourceMap: true,
      },
      ...props,
      architecture: Architecture.ARM_64,
      runtime: Runtime.NODEJS_16_X,
      // I've done it like this so that it works when published as well as from TypeScript
      entry: fs.existsSync(`${__dirname}/onDynamoDBStreamEvent.ts`)
        ? `${__dirname}/onDynamoDBStreamEvent.ts`
        : `${__dirname}/onDynamoDBStreamEvent.js`,
      handler: "defaultHandler",
      logRetention: props.logRetention ? props.logRetention : RetentionDays.ONE_WEEK,
      environment: {
        ...props.environment,
        CONFIGURED_EVENT_SOURCE: props.eventSource,
        PUBLISH_TO_EVENT_BUS_NAME: props.eventBusName,
      },
    });
  }
}
