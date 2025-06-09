# Conduit Connector for <!-- readmegen:name -->Box<!-- /readmegen:name -->

A [Conduit](https://conduit.io) destination connector for Box.com.

<!-- readmegen:description -->
## Destination

The Box Destination takes a Conduit record and uploads it to the remote Box directory.

### Create, Update and Snapshot Operations

The Box destination connector uploads the records in 3 different ways.

* For a file which is ≤ 4MB, it uploads the single record file using a single
`POST /files/content`.
* For a file which is ≥ 4MB and ≤ 20MB, it assembles the file in memory. Once
the file is fully assembled, it uploads it using a single
`POST /files/content` request.
* For a file which is > 20MB, it uploads the file using chunk upload endpoint.
It first creates a new session for chunk upload which gives session id and
part size in response. Using this session id and part size the records are
then uploaded. It prepares the parts by keeping them in memory and upload the
parts one by one using chunk upload endpoint.

### Delete Operation

An OpenCDC record with the `delete` operation is processed so that the file
that's found in the `opencdc.file.name` metadata field is deleted.

## Generating an Access Token

The destination connector requires a token so it can authenticate with the
Box.com HTTP API. To generate it, please follow the steps below.

### Step 1: Access the Box Developer Console

1. Navigate to [https://app.box.com/developers/console](https://app.box.com/developers/console).
2. Sign in using your Box.com credentials.

### Step 2: Create a New App

1. In the Box Developer Console, click **Create Platform App**.
2. Choose **Custom App** as the app type.
3. Select **User Authentication (OAuth 2.0)** as the authentication method.
4. Enter your app details:
   - **App Name**: Use a descriptive name (e.g., *Conduit Box Connector Prod*).
   - **Description**: Provide a brief explanation of your app's purpose.
   - **Purpose**: Describe the app's purpose. This field is informational only and does not affect connector functionality.
5. Click **Create App**.

### Step 3: Configure App Settings

1. On your app's configuration page, go to the **Configuration** tab.
2. Under **Application Scopes**, enable:
   - **Read all files and folders stored in Box**

### Step 4: Obtain an Access Token

1. In the **Developer Token** section, click **Generate Developer Token**.
2. Copy the generated token for use.

### Token Management

You can store the access token in one of the following ways:
- As a plain string in a configuration file
- As an environment variable
<!-- /readmegen:description -->

## Source

A source connector pulls data from an external resource and pushes it to
downstream resources via Conduit.

### Configuration

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    type: source
    status: running
    connectors:
      - id: example
        plugin: "box"
        settings:
```
<!-- /readmegen:source.parameters.yaml -->

## Destination

A destination connector pushes data from upstream resources to an external
resource via Conduit.

### Configuration

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    type: destination
    status: running
    connectors:
      - id: example
        plugin: "box"
        settings:
          # Token used to authenticate API access.
          # Type: string
          # Required: yes
          token: ""
          # ID of the Box directory to read/write files. Default is 0 for the
          # root directory.
          # Type: string
          # Required: no
          parentID: "0"
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

## Development

- To install the required tools, run `make install-tools`.
- To generate code (mocks, re-generate `connector.yaml`, update the README,
  etc.), run `make generate`.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests. Run `make test-integration` to run
the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the
required resource locally.

## How to release?

The release is done in two steps:

- Bump the version in [connector.yaml](/connector.yaml). This can be done
  with [bump_version.sh](/scripts/bump_version.sh) script, e.g.
  `scripts/bump_version.sh 2.3.4` (`2.3.4` is the new version and needs to be a
  valid semantic version). This will also automatically create a PR for the
  change.
- Tag the connector, which will kick off a release. This can be done
  with [tag.sh](/scripts/tag.sh).
