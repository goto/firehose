"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[294],{3905:function(e,t,a){a.d(t,{Zo:function(){return p},kt:function(){return g}});var r=a(7294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},i=Object.keys(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var u=r.createContext({}),s=function(e){var t=r.useContext(u),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},p=function(e){var t=s(e.components);return r.createElement(u.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,i=e.originalType,u=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),m=s(a),g=n,c=m["".concat(u,".").concat(g)]||m[g]||d[g]||i;return a?r.createElement(c,l(l({ref:t},p),{},{components:a})):r.createElement(c,l({ref:t},p))}));function g(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=a.length,l=new Array(i);l[0]=m;var o={};for(var u in t)hasOwnProperty.call(t,u)&&(o[u]=t[u]);o.originalType=e,o.mdxType="string"==typeof e?e:n,l[1]=o;for(var s=2;s<i;s++)l[s]=a[s];return r.createElement.apply(null,l)}return r.createElement.apply(null,a)}m.displayName="MDXCreateElement"},3996:function(e,t,a){a.r(t),a.d(t,{assets:function(){return p},contentTitle:function(){return u},default:function(){return g},frontMatter:function(){return o},metadata:function(){return s},toc:function(){return d}});var r=a(7462),n=a(3366),i=(a(7294),a(3905)),l=["components"],o={},u="Bigquery Sink",s={unversionedId:"sink/bigquery",id:"sink/bigquery",title:"Bigquery Sink",description:"Bigquery Sink has several responsibilities, first creation of bigquery table and dataset when they are not exist, second update the bigquery table schema based on the latest protobuf schema, third translate protobuf messages into bigquery records and insert them to bigquery tables.",source:"@site/docs/sink/bigquery.md",sourceDirName:"sink",slug:"/sink/bigquery",permalink:"/firehose/sink/bigquery",draft:!1,editUrl:"https://github.com/odpf/firehose/edit/master/docs/docs/sink/bigquery.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Bigquery Sink",permalink:"/firehose/sinks/bigquery-sink"},next:{title:"Configuration",permalink:"/firehose/reference/configuration"}},p={},d=[{value:"Asynchronous Consumer Mode",id:"asynchronous-consumer-mode",level:2},{value:"At Least Once Guarantee",id:"at-least-once-guarantee",level:2},{value:"Bigquery Table Schema Update",id:"bigquery-table-schema-update",level:2},{value:"Protobuf - Bigquery Table Type Mapping",id:"protobuf---bigquery-table-type-mapping",level:2},{value:"Partitioning",id:"partitioning",level:2},{value:"Kafka Metadata",id:"kafka-metadata",level:2},{value:"Errors Handling",id:"errors-handling",level:2},{value:"Google Cloud Bigquery IAM Permission",id:"google-cloud-bigquery-iam-permission",level:2}],m={toc:d};function g(e){var t=e.components,a=(0,n.Z)(e,l);return(0,i.kt)("wrapper",(0,r.Z)({},m,a,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"bigquery-sink"},"Bigquery Sink"),(0,i.kt)("p",null,"Bigquery Sink has several responsibilities, first creation of bigquery table and dataset when they are not exist, second update the bigquery table schema based on the latest protobuf schema, third translate protobuf messages into bigquery records and insert them to bigquery tables.\nBigquery utilise Bigquery ",(0,i.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/streaming-data-into-bigquery"},"Streaming API")," to insert record into bigquery tables."),(0,i.kt)("h2",{id:"asynchronous-consumer-mode"},"Asynchronous Consumer Mode"),(0,i.kt)("p",null,"Bigquery Streaming API limits size of payload sent for each insert operations. The limitation reduces the amount of message allowed to be inserted when the message size is big.\nThis will reduce the throughput of bigquery sink. To increase the throughput, firehose provide kafka consumer asynchronous mode.\nIn asynchronous mode sink operation is executed asynchronously, so multiple sink task can be scheduled and run concurrently.\nThroughput can be increased by increasing the number of sink pool."),(0,i.kt)("h2",{id:"at-least-once-guarantee"},"At Least Once Guarantee"),(0,i.kt)("p",null,"Because of asynchronous consumer mode and the possibility of retry on the insert operation. There is no guarantee of the message order that successfully sent to the sink.\nThat also happened with commit offset, the there is no order of the offset number of the processed messages.\nFirehose collect all the offset sort them and only commit the latest continuous offset.\nThis will ensure all the offset being committed after messages successfully processed even when some messages are being re processed by retry handler or when the insert operation took a long time."),(0,i.kt)("h2",{id:"bigquery-table-schema-update"},"Bigquery Table Schema Update"),(0,i.kt)("p",null,"Bigquery Sink update the bigquery table schema on separate table update operation. Bigquery utilise ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/odpf/stencil"},"Stencil")," to parse protobuf messages generate schema and update bigquery tables with the latest schema.\nThe stencil client periodically reload the descriptor cache. Table schema update happened after the descriptor caches uploaded.\nBecause firehose is horizontally scalable multiple firehose consumer might be running.\nBecause there is no coordination strategy between consumers the schema update will be triggered by all consumers. "),(0,i.kt)("h2",{id:"protobuf---bigquery-table-type-mapping"},"Protobuf - Bigquery Table Type Mapping"),(0,i.kt)("p",null,"Here are type conversion between protobuf type and bigquery type : "),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Protobuf Type"),(0,i.kt)("th",{parentName:"tr",align:null},"Bigquery Type"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"bytes"),(0,i.kt)("td",{parentName:"tr",align:null},"BYTES")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"string"),(0,i.kt)("td",{parentName:"tr",align:null},"STRING")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"enum"),(0,i.kt)("td",{parentName:"tr",align:null},"STRING")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"float"),(0,i.kt)("td",{parentName:"tr",align:null},"FLOAT")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"double"),(0,i.kt)("td",{parentName:"tr",align:null},"FLOAT")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"bool"),(0,i.kt)("td",{parentName:"tr",align:null},"BOOLEAN")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"int64, uint64, int32, uint32, fixed64, fixed32, sfixed64, sfixed32, sint64, sint32"),(0,i.kt)("td",{parentName:"tr",align:null},"INTEGER")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"message"),(0,i.kt)("td",{parentName:"tr",align:null},"RECORD")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},".google.protobuf.Timestamp"),(0,i.kt)("td",{parentName:"tr",align:null},"TIMESTAMP")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},".google.protobuf.Struct"),(0,i.kt)("td",{parentName:"tr",align:null},"STRING (Json Serialised)")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},".google.protobuf.Duration"),(0,i.kt)("td",{parentName:"tr",align:null},"RECORD")))),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Protobuf Modifier"),(0,i.kt)("th",{parentName:"tr",align:null},"Bigquery Modifier"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"repeated"),(0,i.kt)("td",{parentName:"tr",align:null},"REPEATED")))),(0,i.kt)("h2",{id:"partitioning"},"Partitioning"),(0,i.kt)("p",null,"Bigquery Sink supports creation of table with partition configuration. Currently, Bigquery Sink only supports time based partitioning.\nTo have time based partitioning protobuf ",(0,i.kt)("inlineCode",{parentName:"p"},"Timestamp")," as field is needed on the protobuf message. The protobuf field will be used as partitioning column on table creation.\nThe time partitioning type that is currently supported is ",(0,i.kt)("inlineCode",{parentName:"p"},"DAY")," partitioning."),(0,i.kt)("h2",{id:"kafka-metadata"},"Kafka Metadata"),(0,i.kt)("p",null,"For data quality checking purpose sometimes kafka metadata need to be added on the record. When ",(0,i.kt)("inlineCode",{parentName:"p"},"SINK_BIGQUERY_METADATA_NAMESPACE")," is configured kafka metadata column will be added, here is the list of kafka metadata column to be added :"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Fully Qualified Column Name"),(0,i.kt)("th",{parentName:"tr",align:null},"Type"),(0,i.kt)("th",{parentName:"tr",align:null},"Modifier"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"metadata_column"),(0,i.kt)("td",{parentName:"tr",align:null},"RECORD"),(0,i.kt)("td",{parentName:"tr",align:null},"NULLABLE")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"metadata_column.message_partition"),(0,i.kt)("td",{parentName:"tr",align:null},"INTEGER"),(0,i.kt)("td",{parentName:"tr",align:null},"NULLABLE")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"metadata_column.message_offset"),(0,i.kt)("td",{parentName:"tr",align:null},"INTEGER"),(0,i.kt)("td",{parentName:"tr",align:null},"NULLABLE")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"metadata_column.message_topic"),(0,i.kt)("td",{parentName:"tr",align:null},"STRING"),(0,i.kt)("td",{parentName:"tr",align:null},"NULLABLE")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"metadata_column.message_timestamp"),(0,i.kt)("td",{parentName:"tr",align:null},"TIMESTAMP"),(0,i.kt)("td",{parentName:"tr",align:null},"NULLABLE")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"metadata_column.load_time"),(0,i.kt)("td",{parentName:"tr",align:null},"TIMESTAMP"),(0,i.kt)("td",{parentName:"tr",align:null},"NULLABLE")))),(0,i.kt)("h2",{id:"errors-handling"},"Errors Handling"),(0,i.kt)("p",null,"Firehose consumer parse errors from table insertion, translate the error into generic error types and attach them for each message that failed to be inserted to bigquery.\nUsers can configure how to handle each generic error types accordingly.\nHere is mapping of the error translation to generic firehose error types : "),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Error Name"),(0,i.kt)("th",{parentName:"tr",align:null},"Generic Error Type"),(0,i.kt)("th",{parentName:"tr",align:null},"Description"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Stopped Error"),(0,i.kt)("td",{parentName:"tr",align:null},"SINK_5XX_ERROR"),(0,i.kt)("td",{parentName:"tr",align:null},"Error on a row insertion that happened because insert job is cancelled because other record is invalid although current record is valid")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Out of bounds Error"),(0,i.kt)("td",{parentName:"tr",align:null},"SINK_4XX_ERROR"),(0,i.kt)("td",{parentName:"tr",align:null},"Error on a row insertion the partitioned column has a date value less than 5 years and more than 1 year in the future")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Invalid schema Error"),(0,i.kt)("td",{parentName:"tr",align:null},"SINK_4XX_ERROR"),(0,i.kt)("td",{parentName:"tr",align:null},"Error on a row insertion when there is a new field that is not exist on the table or when there is required field on the table")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Other Error"),(0,i.kt)("td",{parentName:"tr",align:null},"SINK_UNKNOWN_ERROR"),(0,i.kt)("td",{parentName:"tr",align:null},"Uncategorized error")))),(0,i.kt)("h2",{id:"google-cloud-bigquery-iam-permission"},"Google Cloud Bigquery IAM Permission"),(0,i.kt)("p",null,"Several IAM permission is required for bigquery sink to run properly,"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Create and update Dataset",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"bigquery.tables.create"),(0,i.kt)("li",{parentName:"ul"},"bigquery.tables.get"),(0,i.kt)("li",{parentName:"ul"},"bigquery.tables.update"))),(0,i.kt)("li",{parentName:"ul"},"Create and update Table",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"bigquery.datasets.create"),(0,i.kt)("li",{parentName:"ul"},"bigquery.datasets.get"),(0,i.kt)("li",{parentName:"ul"},"bigquery.datasets.update"))),(0,i.kt)("li",{parentName:"ul"},"Stream insert to Table",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"bigquery.tables.updateData")))),(0,i.kt)("p",null,"Further documentation on bigquery IAM permission ",(0,i.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/streaming-data-into-bigquery"},"here"),"."))}g.isMDXComponent=!0}}]);