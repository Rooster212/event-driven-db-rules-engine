import { App } from "aws-cdk-lib";
import { ExampleDynamoDBEventDrivenArchitectureStack } from "./cdk";

describe("example CDK", () => {
  it("synth's a stack successfully", () => {
    const app = new App();
    const stack = new ExampleDynamoDBEventDrivenArchitectureStack(app, "example-cdk");
    const synthResult = app.synth();

    expect(synthResult).not.toBeUndefined()
  })
})