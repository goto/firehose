"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[518],{3905:function(t,e,a){a.d(e,{Zo:function(){return m},kt:function(){return c}});var n=a(7294);function r(t,e,a){return e in t?Object.defineProperty(t,e,{value:a,enumerable:!0,configurable:!0,writable:!0}):t[e]=a,t}function o(t,e){var a=Object.keys(t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(t);e&&(n=n.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),a.push.apply(a,n)}return a}function i(t){for(var e=1;e<arguments.length;e++){var a=null!=arguments[e]?arguments[e]:{};e%2?o(Object(a),!0).forEach((function(e){r(t,e,a[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(a,e))}))}return t}function l(t,e){if(null==t)return{};var a,n,r=function(t,e){if(null==t)return{};var a,n,r={},o=Object.keys(t);for(n=0;n<o.length;n++)a=o[n],e.indexOf(a)>=0||(r[a]=t[a]);return r}(t,e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(t);for(n=0;n<o.length;n++)a=o[n],e.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(t,a)&&(r[a]=t[a])}return r}var p=n.createContext({}),u=function(t){var e=n.useContext(p),a=e;return t&&(a="function"==typeof t?t(e):i(i({},e),t)),a},m=function(t){var e=u(t.components);return n.createElement(p.Provider,{value:e},t.children)},d={inlineCode:"code",wrapper:function(t){var e=t.children;return n.createElement(n.Fragment,{},e)}},s=n.forwardRef((function(t,e){var a=t.components,r=t.mdxType,o=t.originalType,p=t.parentName,m=l(t,["components","mdxType","originalType","parentName"]),s=u(a),c=r,k=s["".concat(p,".").concat(c)]||s[c]||d[c]||o;return a?n.createElement(k,i(i({ref:e},m),{},{components:a})):n.createElement(k,i({ref:e},m))}));function c(t,e){var a=arguments,r=e&&e.mdxType;if("string"==typeof t||r){var o=a.length,i=new Array(o);i[0]=s;var l={};for(var p in e)hasOwnProperty.call(e,p)&&(l[p]=e[p]);l.originalType=t,l.mdxType="string"==typeof t?t:r,i[1]=l;for(var u=2;u<o;u++)i[u]=a[u];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}s.displayName="MDXCreateElement"},7175:function(t,e,a){a.r(e),a.d(e,{assets:function(){return m},contentTitle:function(){return p},default:function(){return c},frontMatter:function(){return l},metadata:function(){return u},toc:function(){return d}});var n=a(7462),r=a(3366),o=(a(7294),a(3905)),i=["components"],l={},p="MaxCompute sink",u={unversionedId:"sinks/maxcompute-sink",id:"sinks/maxcompute-sink",title:"MaxCompute sink",description:"Datatype Protobuf",source:"@site/docs/sinks/maxcompute-sink.md",sourceDirName:"sinks",slug:"/sinks/maxcompute-sink",permalink:"/firehose/sinks/maxcompute-sink",draft:!1,editUrl:"https://github.com/goto/firehose/edit/master/docs/docs/sinks/maxcompute-sink.md",tags:[],version:"current",frontMatter:{}},m={},d=[{value:"Datatype Protobuf",id:"datatype-protobuf",level:3},{value:"MaxCompute Table Schema Update",id:"maxcompute-table-schema-update",level:2},{value:"Protobuf",id:"protobuf",level:3},{value:"Supported Protobuf - MaxCompute Table Type Mapping",id:"supported-protobuf---maxcompute-table-type-mapping",level:4},{value:"Partitioning",id:"partitioning",level:2},{value:"Clustering",id:"clustering",level:2},{value:"Metadata",id:"metadata",level:2}],s={toc:d};function c(t){var e=t.components,a=(0,r.Z)(t,i);return(0,o.kt)("wrapper",(0,n.Z)({},s,a,{components:e,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"maxcompute-sink"},"MaxCompute sink"),(0,o.kt)("h3",{id:"datatype-protobuf"},"Datatype Protobuf"),(0,o.kt)("p",null,"MaxCompute sink has several responsibilities, including :"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Creation of MaxCompute table if it does not exist."),(0,o.kt)("li",{parentName:"ol"},"Updating the MaxCompute table schema based on the latest protobuf schema."),(0,o.kt)("li",{parentName:"ol"},"Translating protobuf messages into MaxCompute compatible records and inserting them into MaxCompute tables.")),(0,o.kt)("h2",{id:"maxcompute-table-schema-update"},"MaxCompute Table Schema Update"),(0,o.kt)("h3",{id:"protobuf"},"Protobuf"),(0,o.kt)("p",null,"MaxCompute Sink update the MaxCompute table schema on separate table update operation. MaxCompute\nutilise ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/goto/stencil"},"Stencil")," to parse protobuf messages generate schema and update MaxCompute\ntables with the latest schema.\nThe stencil client periodically reload the descriptor cache. Table schema update happened after the descriptor caches\nuploaded."),(0,o.kt)("h4",{id:"supported-protobuf---maxcompute-table-type-mapping"},"Supported Protobuf - MaxCompute Table Type Mapping"),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Protobuf Type"),(0,o.kt)("th",{parentName:"tr",align:null},"MaxCompute Type"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"bytes"),(0,o.kt)("td",{parentName:"tr",align:null},"BINARY")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"string"),(0,o.kt)("td",{parentName:"tr",align:null},"STRING")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"enum"),(0,o.kt)("td",{parentName:"tr",align:null},"STRING")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"float"),(0,o.kt)("td",{parentName:"tr",align:null},"FLOAT")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"double"),(0,o.kt)("td",{parentName:"tr",align:null},"DOUBLE")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"bool"),(0,o.kt)("td",{parentName:"tr",align:null},"BOOLEAN")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"int64, uint64, fixed64, sfixed64, sint64"),(0,o.kt)("td",{parentName:"tr",align:null},"BIGINT")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"int32, uint32, fixed32, sfixed32, sint32"),(0,o.kt)("td",{parentName:"tr",align:null},"INT")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"message"),(0,o.kt)("td",{parentName:"tr",align:null},"STRUCT")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},".google.protobuf.Timestamp"),(0,o.kt)("td",{parentName:"tr",align:null},"TIMESTAMP_NTZ")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},".google.protobuf.Struct"),(0,o.kt)("td",{parentName:"tr",align:null},"STRING (Json Serialised)")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},".google.protobuf.Duration"),(0,o.kt)("td",{parentName:"tr",align:null},"STRUCT")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"map<k,v>"),(0,o.kt)("td",{parentName:"tr",align:null},"ARRAY<STRUCT<key:k, value:v>>")))),(0,o.kt)("h2",{id:"partitioning"},"Partitioning"),(0,o.kt)("p",null,"MaxCompute Sink supports creation of table with partition configuration. Currently, MaxCompute Sink supports primitive field(STRING, TINYINT, SMALLINT, BIGINT)\nand timestamp field based partitioning. Timestamp based partitioning strategy introduces a pseudo-partition column with the value of the timestamp field truncated to the nearest start of day."),(0,o.kt)("h2",{id:"clustering"},"Clustering"),(0,o.kt)("p",null,"MaxCompute Sink currently does not support clustering."),(0,o.kt)("h2",{id:"metadata"},"Metadata"),(0,o.kt)("p",null,"For data quality checking purposes, sometimes some metadata need to be added on the record.\nif ",(0,o.kt)("inlineCode",{parentName:"p"},"SINK_MAXCOMPUTE_ADD_METADATA_ENABLED")," is true then the metadata will be added.\n",(0,o.kt)("inlineCode",{parentName:"p"},"SINK_MAXCOMPUTE_METADATA_NAMESPACE")," is used for another namespace to add columns\nif namespace is empty, the metadata columns will be added in the root level.\n",(0,o.kt)("inlineCode",{parentName:"p"},"SINK_MAXCOMPUTE_METADATA_COLUMNS_TYPES")," is set with kafka metadata column and their type,\nAn example of metadata columns that can be added for kafka records."))}c.isMDXComponent=!0}}]);