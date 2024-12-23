"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[909],{3905:function(e,t,a){a.d(t,{Zo:function(){return u},kt:function(){return m}});var n=a(7294);function i(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function s(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){i(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,i=function(e,t){if(null==e)return{};var a,n,i={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(i[a]=e[a]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(i[a]=e[a])}return i}var l=n.createContext({}),p=function(e){var t=n.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):s(s({},t),e)),a},u=function(e){var t=p(e.components);return n.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},h=n.forwardRef((function(e,t){var a=e.components,i=e.mdxType,r=e.originalType,l=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),h=p(a),m=i,k=h["".concat(l,".").concat(m)]||h[m]||d[m]||r;return a?n.createElement(k,s(s({ref:t},u),{},{components:a})):n.createElement(k,s({ref:t},u))}));function m(e,t){var a=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=a.length,s=new Array(r);s[0]=h;var o={};for(var l in t)hasOwnProperty.call(t,l)&&(o[l]=t[l]);o.originalType=e,o.mdxType="string"==typeof e?e:i,s[1]=o;for(var p=2;p<r;p++)s[p]=a[p];return n.createElement.apply(null,s)}return n.createElement.apply(null,a)}h.displayName="MDXCreateElement"},4016:function(e,t,a){a.r(t),a.d(t,{assets:function(){return u},contentTitle:function(){return l},default:function(){return m},frontMatter:function(){return o},metadata:function(){return p},toc:function(){return d}});var n=a(7462),i=a(3366),r=(a(7294),a(3905)),s=["components"],o={},l="Creating Firehose",p={unversionedId:"guides/create_firehose",id:"guides/create_firehose",title:"Creating Firehose",description:"This page contains how-to guides for creating Firehose with different sinks along with their features.",source:"@site/docs/guides/create_firehose.md",sourceDirName:"guides",slug:"/guides/create_firehose",permalink:"/firehose/guides/create_firehose",draft:!1,editUrl:"https://github.com/goto/firehose/edit/master/docs/docs/guides/create_firehose.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Introduction",permalink:"/firehose/"},next:{title:"JSON-based Filters",permalink:"/firehose/guides/json-based-filters"}},u={},d=[{value:"Create a Log Sink",id:"create-a-log-sink",level:2},{value:"Define generic configurations",id:"define-generic-configurations",level:2},{value:"Create an HTTP Sink",id:"create-an-http-sink",level:2},{value:"Supported methods",id:"supported-methods",level:3},{value:"Authentication",id:"authentication",level:3},{value:"Retries",id:"retries",level:3},{value:"Templating",id:"templating",level:3},{value:"Create a JDBC sink",id:"create-a-jdbc-sink",level:2},{value:"Create an InfluxDB sink",id:"create-an-influxdb-sink",level:2},{value:"Create a Redis sink",id:"create-a-redis-sink",level:2},{value:"Create an Elasticsearch sink",id:"create-an-elasticsearch-sink",level:2},{value:"Create a GRPC sink",id:"create-a-grpc-sink",level:2},{value:"Create an MongoDB sink",id:"create-an-mongodb-sink",level:2},{value:"Create a Blob sink",id:"create-a-blob-sink",level:2},{value:"Create a Bigquery sink",id:"create-a-bigquery-sink",level:2},{value:"Create a Bigtable sink",id:"create-a-bigtable-sink",level:2},{value:"Create an HTTPV2 Sink",id:"create-an-httpv2-sink",level:2},{value:"Create a MaxCompute sink",id:"create-a-maxcompute-sink",level:2}],h={toc:d};function m(e){var t=e.components,a=(0,i.Z)(e,s);return(0,r.kt)("wrapper",(0,n.Z)({},h,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"creating-firehose"},"Creating Firehose"),(0,r.kt)("p",null,"This page contains how-to guides for creating Firehose with different sinks along with their features."),(0,r.kt)("h2",{id:"create-a-log-sink"},"Create a Log Sink"),(0,r.kt)("p",null,"Firehose provides a log sink to make it easy to consume messages in ",(0,r.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Standard_streams#Standard_output_%28stdout%29"},"standard output"),". A log sink firehose requires the following ",(0,r.kt)("a",{parentName:"p",href:"/firehose/advance/generic"},"variables")," to be set. Firehose log sink can work in key as well as message parsing mode configured through ",(0,r.kt)("a",{parentName:"p",href:"/firehose/advance/generic#kafka_record_parser_mode"},(0,r.kt)("inlineCode",{parentName:"a"},"KAFKA_RECORD_PARSER_MODE"))),(0,r.kt)("p",null,"An example log sink configurations:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"SOURCE_KAFKA_BROKERS=localhost:9092\nSOURCE_KAFKA_TOPIC=test-topic\nKAFKA_RECOED_CONSUMER_GROUP_ID=sample-group-id\nKAFKA_RECORD_PARSER_MODE=message\nSINK_TYPE=log\nINPUT_SCHEMA_DATA_TYPE=protobuf\nINPUT_SCHEMA_PROTO_CLASS=com.tests.TestMessage\n")),(0,r.kt)("p",null,"Sample output of a Firehose log sink:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"2021-03-29T08:43:05,998Z [pool-2-thread-1] INFO  com.gotocompany.firehose.Consumer- Execution successful for 1 records\n2021-03-29T08:43:06,246Z [pool-2-thread-1] INFO  com.gotocompany.firehose.Consumer - Pulled 1 messages\n2021-03-29T08:43:06,246Z [pool-2-thread-1] INFO  com.gotocompany.firehose.sink.log.LogSink -\n================= DATA =======================\nsample_field: 81179979\nsample_field_2: 9897987987\nevent_timestamp {\n  seconds: 1617007385\n  nanos: 964581040\n}\n")),(0,r.kt)("h2",{id:"define-generic-configurations"},"Define generic configurations"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"These are the configurations that remain common across all the Sink Types."),(0,r.kt)("li",{parentName:"ul"},"You don\u2019t need to modify them necessarily, It is recommended to use them with the default values. More details ",(0,r.kt)("a",{parentName:"li",href:"../advance/generic#standard"},"here"),".")),(0,r.kt)("h2",{id:"create-an-http-sink"},"Create an HTTP Sink"),(0,r.kt)("p",null,"Firehose ",(0,r.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol"},"HTTP")," sink allows users to read data from Kafka and write to an HTTP endpoint. it requires the following ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/http-sink#http-sink"},"variables")," to be set. You need to create your own HTTP endpoint so that the Firehose can send data to it."),(0,r.kt)("p",null,"Note: HTTP sink type is deprecated from Firehose version 0.8.11 onwards. Please consider using HTTPV2 sink type instead."),(0,r.kt)("h3",{id:"supported-methods"},"Supported methods"),(0,r.kt)("p",null,"Firehose supports ",(0,r.kt)("inlineCode",{parentName:"p"},"PUT")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"POST")," verbs in its HTTP sink. The method can be configured using ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/http-sink#sink_http_request_method"},(0,r.kt)("inlineCode",{parentName:"a"},"SINK_HTTP_REQUEST_METHOD")),"."),(0,r.kt)("h3",{id:"authentication"},"Authentication"),(0,r.kt)("p",null,"Firehose HTTP sink supports ",(0,r.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/OAuth"},"OAuth")," authentication. OAuth can be enabled for the HTTP sink by setting ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/http-sink#sink_http_oauth2_enable"},(0,r.kt)("inlineCode",{parentName:"a"},"SINK_HTTP_OAUTH2_ENABLE"))),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-text"},"SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL: https://sample-oauth.my-api.com/oauth2/token  # OAuth2 Token Endpoint.\nSINK_HTTP_OAUTH2_CLIENT_NAME: client-name  # OAuth2 identifier issued to the client.\nSINK_HTTP_OAUTH2_CLIENT_SECRET: client-secret # OAuth2 secret issued for the client.\nSINK_HTTP_OAUTH2_SCOPE: User:read, sys:info  # Space-delimited scope overrides.\n")),(0,r.kt)("h3",{id:"retries"},"Retries"),(0,r.kt)("p",null,"Firehose allows for retrying to sink messages in case of failure of HTTP service. The HTTP error code ranges to retry can be configured with ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/http-sink#sink_http_retry_status_code_ranges"},(0,r.kt)("inlineCode",{parentName:"a"},"SINK_HTTP_RETRY_STATUS_CODE_RANGES")),". HTTP request timeout can be configured with ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/http-sink#sink_http_request_timeout_ms"},(0,r.kt)("inlineCode",{parentName:"a"},"SINK_HTTP_REQUEST_TIMEOUT_MS"))),(0,r.kt)("h3",{id:"templating"},"Templating"),(0,r.kt)("p",null,"Firehose HTTP sink supports payload templating using ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/http-sink#sink_http_json_body_template"},(0,r.kt)("inlineCode",{parentName:"a"},"SINK_HTTP_JSON_BODY_TEMPLATE"))," configuration. It uses ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/json-path/JsonPath"},"JsonPath")," for creating Templates which is a DSL for basic JSON parsing. Playground for this: ",(0,r.kt)("a",{parentName:"p",href:"https://jsonpath.com/"},"https://jsonpath.com/"),", where users can play around with a given JSON to extract out the elements as required and validate the ",(0,r.kt)("inlineCode",{parentName:"p"},"jsonpath"),". The template works only when the output data format ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/http-sink#sink_http_data_format"},(0,r.kt)("inlineCode",{parentName:"a"},"SINK_HTTP_DATA_FORMAT"))," is JSON."),(0,r.kt)("p",null,(0,r.kt)("em",{parentName:"p"},(0,r.kt)("strong",{parentName:"em"},"Creating Templates:"))),(0,r.kt)("p",null,"This is really simple. Find the paths you need to extract using the JSON path. Create a valid JSON template with the static field names + the paths that need to extract. ","(","Paths name starts with $.",")",". Firehose will simply replace the paths with the actual data in the path of the message accordingly. Paths can also be used on keys, but be careful that the element in the key must be a string data type."),(0,r.kt)("p",null,"One sample configuration","(","On XYZ proto",")"," : ",(0,r.kt)("inlineCode",{parentName:"p"},'{"test":"$.routes[0]", "$.order_number" : "xxx"}')," If you want to dump the entire JSON as it is in the backend, use ",(0,r.kt)("inlineCode",{parentName:"p"},'"$._all_"')," as a path."),(0,r.kt)("p",null,"Limitations:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Works when the input DATA TYPE is a protobuf, not a JSON."),(0,r.kt)("li",{parentName:"ul"},"Supports only on messages, not keys."),(0,r.kt)("li",{parentName:"ul"},"validation on the level of valid JSON template. But after data has been replaced the resulting string may or may not be a valid JSON. Users must do proper testing/validation from the service side."),(0,r.kt)("li",{parentName:"ul"},"If selecting fields from complex data types like repeated/messages/map of proto, the user must do filtering based first as selecting a field that does not exist would fail.")),(0,r.kt)("h2",{id:"create-a-jdbc-sink"},"Create a JDBC sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Supports only PostgresDB as of now."),(0,r.kt)("li",{parentName:"ul"},"Data read from Kafka is written to the PostgresDB database and it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/jdbc-sink#jdbc-sink"},"variables")," to be set.")),(0,r.kt)("p",null,(0,r.kt)("em",{parentName:"p"},(0,r.kt)("strong",{parentName:"em"},"Note: Schema ","(","Table, Columns, and Any Constraints",")"," being used in firehose configuration must exist in the Database already."))),(0,r.kt)("h2",{id:"create-an-influxdb-sink"},"Create an InfluxDB sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Data read from Kafka is written to the InfluxDB time-series database and it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/influxdb-sink#influx-sink"},"variables")," to be set.")),(0,r.kt)("p",null,(0,r.kt)("em",{parentName:"p"},(0,r.kt)("strong",{parentName:"em"},"Note:"))," ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/influxdb-sink#sink_influx_db_name"},(0,r.kt)("em",{parentName:"a"},(0,r.kt)("strong",{parentName:"em"},"DATABASE")))," ",(0,r.kt)("em",{parentName:"p"},(0,r.kt)("strong",{parentName:"em"},"and"))," ",(0,r.kt)("a",{parentName:"p",href:"/firehose/sinks/influxdb-sink#sink_influx_retention_policy"},(0,r.kt)("em",{parentName:"a"},(0,r.kt)("strong",{parentName:"em"},"RETENTION POLICY")))," ",(0,r.kt)("em",{parentName:"p"},(0,r.kt)("strong",{parentName:"em"},"being used in firehose configuration must exist already in the Influx, It\u2019s outside the scope of a firehose and won\u2019t be generated automatically."))),(0,r.kt)("h2",{id:"create-a-redis-sink"},"Create a Redis sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/redis-sink"},"variables")," to be set."),(0,r.kt)("li",{parentName:"ul"},"Redis sink can be created in 2 different modes based on the value of ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/redis-sink#sink_redis_data_type"},(0,r.kt)("inlineCode",{parentName:"a"},"SINK_REDIS_DATA_TYPE")),": HashSet or List",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"Hashset"),": For each message, an entry of the format ",(0,r.kt)("inlineCode",{parentName:"li"},"key : field : value")," is generated and pushed to Redis. field and value are generated on the basis of the config ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/redis-sink#-input_schema_proto_to_column_mapping-2"},(0,r.kt)("inlineCode",{parentName:"a"},"INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"List"),": For each message entry of the format ",(0,r.kt)("inlineCode",{parentName:"li"},"key : value")," is generated and pushed to Redis. Value is fetched for the proto index provided in the config ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/redis-sink#sink_redis_list_data_proto_index"},(0,r.kt)("inlineCode",{parentName:"a"},"SINK_REDIS_LIST_DATA_PROTO_INDEX"))))),(0,r.kt)("li",{parentName:"ul"},"The ",(0,r.kt)("inlineCode",{parentName:"li"},"key")," is picked up from a field in the message itself."),(0,r.kt)("li",{parentName:"ul"},"Redis sink also supports different ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/redis-sink#sink_redis_deployment_type"},"Deployment Types")," ",(0,r.kt)("inlineCode",{parentName:"li"},"Standalone")," and ",(0,r.kt)("inlineCode",{parentName:"li"},"Cluster"),"."),(0,r.kt)("li",{parentName:"ul"},"Limitation: Firehose Redis sink only supports HashSet and List entries as of now.")),(0,r.kt)("h2",{id:"create-an-elasticsearch-sink"},"Create an Elasticsearch sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/elasticsearch-sink"},"variables")," to be set."),(0,r.kt)("li",{parentName:"ul"},"In the Elasticsearch sink, each message is converted into a document in the specified index with the Document type and ID as specified by the user."),(0,r.kt)("li",{parentName:"ul"},"Elasticsearch sink supports reading messages in both JSON and Protobuf formats."),(0,r.kt)("li",{parentName:"ul"},"Using ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/elasticsearch-sink#sink_es_routing_key_name"},"Routing Key")," one can route documents to a particular shard in Elasticsearch.")),(0,r.kt)("h2",{id:"create-a-grpc-sink"},"Create a GRPC sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Data read from Kafka is written to a GRPC endpoint and it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/grpc-sink"},"variables")," to be set."),(0,r.kt)("li",{parentName:"ul"},"You need to create your own GRPC endpoint so that the Firehose can send data to it. The response proto should have a field \u201csuccess\u201d with value as true or false.")),(0,r.kt)("h2",{id:"create-an-mongodb-sink"},"Create an MongoDB sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/mongo-sink"},"variables")," to be set."),(0,r.kt)("li",{parentName:"ul"},"In the MongoDB sink, each message is converted into a BSON Document and then inserted/updated/upserted into the specified Mongo Collection"),(0,r.kt)("li",{parentName:"ul"},"MongoDB sink supports reading messages in both JSON and Protobuf formats.")),(0,r.kt)("h2",{id:"create-a-blob-sink"},"Create a Blob sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/blob-sink"},"variables")," to be set."),(0,r.kt)("li",{parentName:"ul"},"Only support google cloud storage for now."),(0,r.kt)("li",{parentName:"ul"},"Only support writing protobuf message to apache parquet file format for now."),(0,r.kt)("li",{parentName:"ul"},"The protobuf message need to have a ",(0,r.kt)("inlineCode",{parentName:"li"},"google.protobuf.Timestamp")," field as partitioning timestamp, ",(0,r.kt)("inlineCode",{parentName:"li"},"event_timestamp")," field is usually being used."),(0,r.kt)("li",{parentName:"ul"},"Google cloud credential with some google cloud storage permission is required to run this sink.")),(0,r.kt)("h2",{id:"create-a-bigquery-sink"},"Create a Bigquery sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/bigquery-sink"},"variables")," to be set."),(0,r.kt)("li",{parentName:"ul"},"For INPUT_SCHEMA_DATA_TYPE = protobuf, this sink will generate bigquery schema from protobuf message schema and update bigquery table with the latest generated schema."),(0,r.kt)("li",{parentName:"ul"},"The protobuf message of a ",(0,r.kt)("inlineCode",{parentName:"li"},"google.protobuf.Timestamp")," field might be needed when table partitioning is enabled."),(0,r.kt)("li",{parentName:"ul"},"For INPUT_SCHEMA_DATA_TYPE = json, this sink will generate bigquery schema by infering incoming json. In future we will add support for json schema as well coming from stencil."),(0,r.kt)("li",{parentName:"ul"},"The timestamp column is needed incase of partition table. It can be generated at the time of ingestion by setting the config. Please refer to config ",(0,r.kt)("inlineCode",{parentName:"li"},"SINK_BIGQUERY_ADD_EVENT_TIMESTAMP_ENABLE")," in ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/goto/depot/blob/main/docs/reference/configuration/bigquery-sink.md#sink_bigquery_add_event_timestamp_enable"},"depot bigquery sink config section")),(0,r.kt)("li",{parentName:"ul"},"Google cloud credential with some bigquery permission is required to run this sink.")),(0,r.kt)("h2",{id:"create-a-bigtable-sink"},"Create a Bigtable sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"it requires the following environment  ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/goto/depot/blob/main/docs/reference/configuration/bigtable.md"},"variables")," ,which are required by Depot library, to be set along with the generic firehose variables.")),(0,r.kt)("h2",{id:"create-an-httpv2-sink"},"Create an HTTPV2 Sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"HttpV2 Sink is implemented in Firehose using the Http sink connector implementation in Depot library. For details on all the features supported by HttpV2 Sink, please refer the Depot documentation ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/goto/depot/blob/main/docs/sinks/http.md"},"here"),"."),(0,r.kt)("li",{parentName:"ul"},"it requires the following environment  ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/goto/depot/blob/main/docs/reference/configuration/http.md"},"variables")," ,which are required by Depot library, to be set along with the generic firehose variables.")),(0,r.kt)("p",null,"If you'd like to connect to a sink which is not yet supported, you can create a new sink by following the ",(0,r.kt)("a",{parentName:"p",href:"/firehose/contribute/contribution"},"contribution guidelines")),(0,r.kt)("h2",{id:"create-a-maxcompute-sink"},"Create a MaxCompute sink"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"it requires the following ",(0,r.kt)("a",{parentName:"li",href:"/firehose/sinks/maxcompute-sink"},"variables")," to be set. Please follow the Configuration section in the MaxCompute Sink documentation for more details."),(0,r.kt)("li",{parentName:"ul"},"As of now it only supports INPUT_SCHEMA_DATA_TYPE = protobuf. Schema creation and update is inferred from protobuf schema."),(0,r.kt)("li",{parentName:"ul"},"The protobuf message of a ",(0,r.kt)("inlineCode",{parentName:"li"},"google.protobuf.Timestamp")," field might be needed when table partitioning is enabled."),(0,r.kt)("li",{parentName:"ul"},"INPUT_SCHEMA_DATA_TYPE = json will be supported in future."),(0,r.kt)("li",{parentName:"ul"},"Schema/Dataset need to be created in advance in MaxCompute."),(0,r.kt)("li",{parentName:"ul"},"Service account requires ODPS and Tunnel Service permissions.")))}m.isMDXComponent=!0}}]);