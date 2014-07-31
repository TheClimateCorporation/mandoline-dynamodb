# mandoline-dynamodb

DynamoDB backend for Mandoline.

## Usage

        [io.mandoline/mandoline-dynamodb "0.1.1"]

## About the tests


Most of the tests in this project need to interact with dynamodb.  Sometimes
running these tests with the real dynamodb can be very slow, so we have
provided and option to run these tests against a local instance of dynamodb.
This local instance is not *exactly* the same as the real thing, but is a close
approximation.

If you create a test with the `deftest*` macro it will actually create two
tests for you, one marked with the `^:local` selector and one marked with the
`^:integration`. The one marked with `^:local` will try to connect to the local
instance of dynamodb instead of the real thing.

To run just the tests that interact with the real dynamodb: `lein test
:integration`. You will need the environment variables AWS_ACCESS_KEY_ID
AWS_SECRET_KEY in your environment.

To run just the tests that interact with the local dynamodb, you will need to
be running the local dynamodb process. Then run: `lein test :local`

###  How to run the local dynamodb process

1. Get the jar file from:
   http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_2014-01-08.tar.gz
2. cd to dynamodb_local_XXXX-XX-XX dir
3. Run the process with: `java -Djava.library.path=DynamoDBLocal_lib -jar
   DynamoDBLocal.jar --inMemory`

By default, when `lein test` is run, all tests except the ones marked with
`^:integration` will be run (i.e. unit tests and `^:local` tests). Keep in mind
using `deftest*` means that you have actually made two tests. Make sure that
you use this macro to make all new dynamodb tests so that both kinds of tests
will be generated.
