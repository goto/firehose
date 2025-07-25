# Filters

Following variables need to be set to enable JSON/JEXL filters.

## `FILTER_ENGINE`

Defines whether to use `JSON` Schema-based filters or `JEXL`-based filters or `NO_OP` \(i.e. no filtering\)

* Example value: `JSON`
* Type: `optional`
* Default value`: NO_OP`

## `FILTER_JSON_ESB_MESSAGE_TYPE`

Defines the format type of the input ESB messages, i.e. JSON/Protobuf. This field is required only for JSON filters.

* Example value: `JSON`
* Type: `optional`

## `FILTER_SCHEMA_PROTO_CLASS`

The fully qualified name of the proto schema so that the key/message in Kafka could be parsed.

* Example value: `com.gojek.esb.driverlocation.DriverLocationLogKey`
* Type: `optional`

## `FILTER_DATA_SOURCE`

`key`/`message`/`none`depending on where to apply filter

* Example value: `key`
* Type: `optional`
* Default value`: none`

## `FILTER_JEXL_EXPRESSION`

JEXL filter expression

* Example value: `driverLocationLogKey.getVehicleType()=="BIKE"`
* Type: `optional`

## `FILTER_JSON_SCHEMA`

JSON Schema string containing the filter rules to be applied.

* Example value: `{"properties":{"order_number":{"const":"1253"}}}`
* Type: `optional`

## `FILTER_TIMESTAMP_PROTO_FIELD_NAME`

Field name containing the timestamp for timestamp-based filtering.

* Example value: `event_timestamp`
* Type: `optional`
* Default value: `event_timestamp`

## `FILTER_TIMESTAMP_PAST_WINDOW_SECONDS`

Time window in seconds for past timestamps. Messages with timestamps older than (current_time - past_window) will be filtered out.

* Example value: `86400` (1 day)
* Type: `optional`
* Default value: `604800` (7 days)

## `FILTER_TIMESTAMP_FUTURE_WINDOW_SECONDS`

Time window in seconds for future timestamps. Messages with timestamps newer than (current_time + future_window) will be filtered out.

* Example value: `3600` (1 hour)
* Type: `optional`
* Default value: `604800` (7 days)

## `FILTER_DROP_DESERIALIZATION_ERROR`

Whether to drop messages with protobuf deserialization errors instead of failing the entire batch. Applies to all filter types (JSON, JEXL, Timestamp) when processing protobuf messages.

* Example value: `true`
* Type: `optional`
* Default value: `false`

