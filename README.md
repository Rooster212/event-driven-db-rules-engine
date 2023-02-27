# Event Sourced Database in DynamoDB / TypeScript

A library for building event sourced database systems with DynamoDB.

There are examples in the `examples` folder - to run the tests successfully you should run `npm run dynamodb` first, which will run DynamoDB locally in Docker.

## Original Documentation

Forked from original implementation here: https://github.com/a-h/hde. Many thanks to Adrian.

See blog posts:

- https://adrianhesketh.com/2020/08/28/event-sourced-dynamodb-design-with-typescript-part-1/
- https://adrianhesketh.com/2020/08/28/event-sourced-dynamodb-design-with-typescript-part-2/

## Using a package from the GitHub Package registry for NPM

There is full documentation [on the GitHub website here](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry#installing-a-package). Using the package hosted on GitHub requires setting up an `.npmrc` file for the project's scope in the consuming project that looks like this:

```bash
@rooster212:registry=https://npm.pkg.github.com
```

You also have to login to the GitHub package registry (as this is a public package, it should work for anyone).

```bash
npm login --scope=@rooster212 --registry=https://npm.pkg.github.com
```

If you use MFA on your GitHub account, you will need to setup a Personal Access Token in your profile settings and use that in place of your password when prompted in the `npm login` step.
