# Blob

A Blob sink Firehose (`SINK_TYPE`=`blob`) requires the following variables to be set along with Generic ones

### `SINK_BLOB_STORAGE_TYPE`

Defines the types of blob storage the destination remote file system the file will be uploaded. Currently supported blob storages are `GCS` (Google Cloud Storage), `S3` (Amazon S3), and `OSS` (Alibaba Cloud Object Storage Service).

- Example value: `GCS`, `S3`, or `OSS`
- Type: `required`

[... existing content ...]

### OSS Configuration

When using OSS as the storage type (`SINK_BLOB_STORAGE_TYPE=OSS`), the following configurations are required:

### `SINK_BLOB_OSS_ENDPOINT`

The OSS endpoint URL.

- Example value: `oss-cn-hangzhou.aliyuncs.com`
- Type: `required`

### `SINK_BLOB_OSS_ACCESS_KEY_ID`

The access key ID for OSS authentication.

- Type: `required`

### `SINK_BLOB_OSS_ACCESS_KEY_SECRET`

The access key secret for OSS authentication.

- Type: `required`

### `SINK_BLOB_OSS_BUCKET_NAME`

The name of the OSS bucket.

- Type: `required`

### `SINK_BLOB_OSS_DIRECTORY_PREFIX`

The directory prefix for OSS objects.

- Type: `required`

### `SINK_BLOB_OSS_REGION`

The OSS region.

- Example value: `cn-hangzhou`
- Type: `required`

### `SINK_BLOB_OSS_MAX_CONNECTIONS`

Maximum number of allowed open HTTP connections.

- Example value: `1024`
- Type: `optional`
- Default value: `1024`

### `SINK_BLOB_OSS_SOCKET_TIMEOUT`

Socket timeout in milliseconds.

- Example value: `50000`
- Type: `optional`
- Default value: `50000`

### `SINK_BLOB_OSS_CONNECTION_TIMEOUT`

Connection timeout in milliseconds.

- Example value: `50000`
- Type: `optional`
- Default value: `50000`

### `SINK_BLOB_OSS_MAX_ERROR_RETRY`

Maximum number of retry attempts for failed requests.

- Example value: `3`
- Type: `optional`
- Default value: `3`
