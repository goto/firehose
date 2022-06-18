"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[592],{3905:function(e,t,n){n.d(t,{Zo:function(){return d},kt:function(){return m}});var i=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,i)}return n}function r(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,i,a=function(e,t){if(null==e)return{};var n,i,a={},l=Object.keys(e);for(i=0;i<l.length;i++)n=l[i],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(i=0;i<l.length;i++)n=l[i],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var u=i.createContext({}),p=function(e){var t=i.useContext(u),n=t;return e&&(n="function"==typeof e?e(t):r(r({},t),e)),n},d=function(e){var t=p(e.components);return i.createElement(u.Provider,{value:t},e.children)},s={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},c=i.forwardRef((function(e,t){var n=e.components,a=e.mdxType,l=e.originalType,u=e.parentName,d=o(e,["components","mdxType","originalType","parentName"]),c=p(n),m=a,_=c["".concat(u,".").concat(m)]||c[m]||s[m]||l;return n?i.createElement(_,r(r({ref:t},d),{},{components:n})):i.createElement(_,r({ref:t},d))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var l=n.length,r=new Array(l);r[0]=c;var o={};for(var u in t)hasOwnProperty.call(t,u)&&(o[u]=t[u]);o.originalType=e,o.mdxType="string"==typeof e?e:a,r[1]=o;for(var p=2;p<l;p++)r[p]=n[p];return i.createElement.apply(null,r)}return i.createElement.apply(null,n)}c.displayName="MDXCreateElement"},2338:function(e,t,n){n.r(t),n.d(t,{assets:function(){return d},contentTitle:function(){return u},default:function(){return m},frontMatter:function(){return o},metadata:function(){return p},toc:function(){return s}});var i=n(7462),a=n(3366),l=(n(7294),n(3905)),r=["components"],o={},u="Bigquery Sink",p={unversionedId:"sinks/bigquery-sink",id:"sinks/bigquery-sink",title:"Bigquery Sink",description:"A Bigquery sink Firehose \\(SINK_TYPE=bigquery\\) requires the following variables to be set along with Generic ones",source:"@site/docs/sinks/bigquery-sink.md",sourceDirName:"sinks",slug:"/sinks/bigquery-sink",permalink:"/firehose/sinks/bigquery-sink",draft:!1,editUrl:"https://github.com/odpf/firehose/edit/master/docs/docs/sinks/bigquery-sink.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Blob Sink",permalink:"/firehose/sinks/blob-sink"},next:{title:"Bigquery Sink",permalink:"/firehose/sink/bigquery"}},d={},s=[{value:"<code>SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID</code>",id:"sink_bigquery_google_cloud_project_id",level:2},{value:"<code>SINK_BIGQUERY_TABLE_NAME</code>",id:"sink_bigquery_table_name",level:2},{value:"<code>SINK_BIGQUERY_DATASET_NAME</code>",id:"sink_bigquery_dataset_name",level:2},{value:"<code>SINK_BIGQUERY_DATASET_LABELS</code>",id:"sink_bigquery_dataset_labels",level:2},{value:"<code>SINK_BIGQUERY_TABLE_LABELS</code>",id:"sink_bigquery_table_labels",level:2},{value:"<code>SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE</code>",id:"sink_bigquery_table_partitioning_enable",level:2},{value:"<code>SINK_BIGQUERY_TABLE_PARTITION_KEY</code>",id:"sink_bigquery_table_partition_key",level:2},{value:"<code>SINK_BIGQUERY_ROW_INSERT_ID_ENABLE</code>",id:"sink_bigquery_row_insert_id_enable",level:2},{value:"<code>SINK_BIGQUERY_CREDENTIAL_PATH</code>",id:"sink_bigquery_credential_path",level:2},{value:"<code>SINK_BIGQUERY_METADATA_NAMESPACE</code>",id:"sink_bigquery_metadata_namespace",level:2},{value:"<code>SINK_BIGQUERY_DATASET_LOCATION</code>",id:"sink_bigquery_dataset_location",level:2},{value:"<code>SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS</code>",id:"sink_bigquery_table_partition_expiry_ms",level:2},{value:"<code>SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS</code>",id:"sink_bigquery_client_read_timeout_ms",level:2},{value:"<code>SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS</code>",id:"sink_bigquery_client_connect_timeout_ms",level:2}],c={toc:s};function m(e){var t=e.components,n=(0,a.Z)(e,r);return(0,l.kt)("wrapper",(0,i.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,l.kt)("h1",{id:"bigquery-sink"},"Bigquery Sink"),(0,l.kt)("p",null,"A Bigquery sink Firehose ","(",(0,l.kt)("inlineCode",{parentName:"p"},"SINK_TYPE"),"=",(0,l.kt)("inlineCode",{parentName:"p"},"bigquery"),")"," requires the following variables to be set along with Generic ones"),(0,l.kt)("h2",{id:"sink_bigquery_google_cloud_project_id"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID")),(0,l.kt)("p",null,"Contains information of google cloud project id location of the bigquery table where the records need to be inserted. Further documentation on google cloud ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/resource-manager/docs/creating-managing-projects"},"project id"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"gcp-project-id")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"required"))),(0,l.kt)("h2",{id:"sink_bigquery_table_name"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_TABLE_NAME")),(0,l.kt)("p",null,"The name of bigquery table. Here is further documentation of bigquery ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/docs/tables"},"table naming"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"user_profile")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"required"))),(0,l.kt)("h2",{id:"sink_bigquery_dataset_name"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_DATASET_NAME")),(0,l.kt)("p",null,"The name of dataset that contains the bigquery table. Here is further documentation of bigquery ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/docs/datasets"},"dataset naming"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"customer")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"required"))),(0,l.kt)("h2",{id:"sink_bigquery_dataset_labels"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_DATASET_LABELS")),(0,l.kt)("p",null,"Labels of a bigquery dataset, key-value information separated by comma attached to the bigquery dataset. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/docs/labels-intro"},"labels"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"owner=data-engineering,granurality=daily")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"optional"))),(0,l.kt)("h2",{id:"sink_bigquery_table_labels"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_TABLE_LABELS")),(0,l.kt)("p",null,"Labels of a bigquery table, key-value information separated by comma attached to the bigquery table. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/docs/labels-intro"},"labels"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"owner=data-engineering,granurality=daily")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"optional"))),(0,l.kt)("h2",{id:"sink_bigquery_table_partitioning_enable"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE")),(0,l.kt)("p",null,"Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the bigquery table.\nBigquery table partitioning config can only be set once, on the table creation and the partitioning cannot be disabled once created. Changing this value of this config later will cause error when firehose trying to update the bigquery table.\nHere is further documentation of bigquery ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/docs/partitioned-tables"},"table partitioning"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"true")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"required")),(0,l.kt)("li",{parentName:"ul"},"Default value: ",(0,l.kt)("inlineCode",{parentName:"li"},"false"))),(0,l.kt)("h2",{id:"sink_bigquery_table_partition_key"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_TABLE_PARTITION_KEY")),(0,l.kt)("p",null,"Define protobuf/bigquery field name that will be used for bigquery table partitioning. only protobuf ",(0,l.kt)("inlineCode",{parentName:"p"},"Timestamp")," field, that later converted into bigquery ",(0,l.kt)("inlineCode",{parentName:"p"},"Timestamp")," column that is supported as partitioning key.\nCurrently, this sink only support ",(0,l.kt)("inlineCode",{parentName:"p"},"DAY")," time partitioning type.\nHere is further documentation of bigquery ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/docs/creating-partitioned-tables#console"},"column time partitioning"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"event_timestamp")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"required"))),(0,l.kt)("h2",{id:"sink_bigquery_row_insert_id_enable"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_ROW_INSERT_ID_ENABLE")),(0,l.kt)("p",null,"This config enables adding of ID row intended for deduplication when inserting new records into bigquery.\nHere is further documentation of bigquery streaming insert ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/streaming-data-into-bigquery"},"deduplication"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"false")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"required")),(0,l.kt)("li",{parentName:"ul"},"Default value: ",(0,l.kt)("inlineCode",{parentName:"li"},"true"))),(0,l.kt)("h2",{id:"sink_bigquery_credential_path"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_CREDENTIAL_PATH")),(0,l.kt)("p",null,"Full path of google cloud credentials file. Here is further documentation of google cloud authentication and ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/docs/authentication/getting-started"},"credentials"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"/.secret/google-cloud-credentials.json")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"required"))),(0,l.kt)("h2",{id:"sink_bigquery_metadata_namespace"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_METADATA_NAMESPACE")),(0,l.kt)("p",null,"The name of column that will be added alongside of the existing bigquery column that generated from protobuf, that column contains struct of kafka metadata of the inserted record.\nWhen this config is not configured the metadata column will not be added to the table."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"kafka_metadata")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"optional"))),(0,l.kt)("h2",{id:"sink_bigquery_dataset_location"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_DATASET_LOCATION")),(0,l.kt)("p",null,"The geographic region name of location of bigquery dataset. Further documentation on bigquery dataset ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/docs/locations#dataset_location"},"location"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"us-central1")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"optional")),(0,l.kt)("li",{parentName:"ul"},"Default value: ",(0,l.kt)("inlineCode",{parentName:"li"},"asia-southeast1"))),(0,l.kt)("h2",{id:"sink_bigquery_table_partition_expiry_ms"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS")),(0,l.kt)("p",null,"The duration of bigquery table partitioning expiration in milliseconds. Fill this config with ",(0,l.kt)("inlineCode",{parentName:"p"},"-1")," will disable the table partition expiration. Further documentation on bigquery table partition ",(0,l.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration"},"expiration"),"."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"2592000000")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"optional")),(0,l.kt)("li",{parentName:"ul"},"Default value: ",(0,l.kt)("inlineCode",{parentName:"li"},"-1"))),(0,l.kt)("h2",{id:"sink_bigquery_client_read_timeout_ms"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS")),(0,l.kt)("p",null,"The duration of bigquery client http read timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000)."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"20000")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"optional")),(0,l.kt)("li",{parentName:"ul"},"Default value: ",(0,l.kt)("inlineCode",{parentName:"li"},"-1"))),(0,l.kt)("h2",{id:"sink_bigquery_client_connect_timeout_ms"},(0,l.kt)("inlineCode",{parentName:"h2"},"SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS")),(0,l.kt)("p",null,"The duration of bigquery client http connection timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000)."),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"Example value: ",(0,l.kt)("inlineCode",{parentName:"li"},"20000")),(0,l.kt)("li",{parentName:"ul"},"Type: ",(0,l.kt)("inlineCode",{parentName:"li"},"optional")),(0,l.kt)("li",{parentName:"ul"},"Default value: ",(0,l.kt)("inlineCode",{parentName:"li"},"-1"))))}m.isMDXComponent=!0}}]);