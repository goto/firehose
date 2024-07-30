"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[915],{3905:function(e,t,n){n.d(t,{Zo:function(){return c},kt:function(){return u}});var i=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,i)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,i,r=function(e,t){if(null==e)return{};var n,i,r={},a=Object.keys(e);for(i=0;i<a.length;i++)n=a[i],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(i=0;i<a.length;i++)n=a[i],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var p=i.createContext({}),s=function(e){var t=i.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},c=function(e){var t=s(e.components);return i.createElement(p.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},m=i.forwardRef((function(e,t){var n=e.components,r=e.mdxType,a=e.originalType,p=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),m=s(n),u=r,k=m["".concat(p,".").concat(u)]||m[u]||d[u]||a;return n?i.createElement(k,o(o({ref:t},c),{},{components:n})):i.createElement(k,o({ref:t},c))}));function u(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var a=n.length,o=new Array(a);o[0]=m;var l={};for(var p in t)hasOwnProperty.call(t,p)&&(l[p]=t[p]);l.originalType=e,l.mdxType="string"==typeof e?e:r,o[1]=l;for(var s=2;s<a;s++)o[s]=n[s];return i.createElement.apply(null,o)}return i.createElement.apply(null,n)}m.displayName="MDXCreateElement"},2510:function(e,t,n){n.r(t),n.d(t,{assets:function(){return c},contentTitle:function(){return p},default:function(){return u},frontMatter:function(){return l},metadata:function(){return s},toc:function(){return d}});var i=n(7462),r=n(3366),a=(n(7294),n(3905)),o=["components"],l={},p="GRPC",s={unversionedId:"sinks/grpc-sink",id:"sinks/grpc-sink",title:"GRPC",description:"gRPC is a modern open source high performance Remote Procedure Call framework that can run in any environment.",source:"@site/docs/sinks/grpc-sink.md",sourceDirName:"sinks",slug:"/sinks/grpc-sink",permalink:"/firehose/sinks/grpc-sink",draft:!1,editUrl:"https://github.com/goto/firehose/edit/master/docs/docs/sinks/grpc-sink.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"HTTP",permalink:"/firehose/sinks/http-sink"},next:{title:"JDBC",permalink:"/firehose/sinks/jdbc-sink"}},c={},d=[{value:"<code>SINK_GRPC_SERVICE_HOST</code>",id:"sink_grpc_service_host",level:3},{value:"<code>SINK_GRPC_SERVICE_PORT</code>",id:"sink_grpc_service_port",level:3},{value:"<code>SINK_GRPC_METHOD_URL</code>",id:"sink_grpc_method_url",level:3},{value:"<code>SINK_GRPC_METADATA</code>",id:"sink_grpc_metadata",level:3},{value:"<code>SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS</code>",id:"sink_grpc_response_schema_proto_class",level:3},{value:"<code>SINK_GRPC_ARG_KEEPALIVE_TIME_MS</code>",id:"sink_grpc_arg_keepalive_time_ms",level:3},{value:"<code>SINK_GRPC_ARG_KEEPALIVE_TIMEOUT_MS</code>",id:"sink_grpc_arg_keepalive_timeout_ms",level:3},{value:"<code>SINK_GRPC_ARG_DEADLINE_MS</code>",id:"sink_grpc_arg_deadline_ms",level:3},{value:"<code>SINK_GRPC_TLS_ENABLE</code>",id:"sink_grpc_tls_enable",level:3},{value:"<code>SINK_GRPC_ROOT_CA</code>",id:"sink_grpc_root_ca",level:3},{value:"<code>SINK_GRPC_RESPONSE_RETRY_CEL_EXPRESSION</code>",id:"sink_grpc_response_retry_cel_expression",level:3},{value:"<code>SINK_GRPC_RESPONSE_RETRY_ERROR_TYPE</code>",id:"sink_grpc_response_retry_error_type",level:3}],m={toc:d};function u(e){var t=e.components,n=(0,r.Z)(e,o);return(0,a.kt)("wrapper",(0,i.Z)({},m,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"grpc"},"GRPC"),(0,a.kt)("p",null,(0,a.kt)("a",{parentName:"p",href:"https://grpc.io/"},"gRPC")," is a modern open source high performance Remote Procedure Call framework that can run in any environment."),(0,a.kt)("p",null,"A GRPC sink Firehose ","(",(0,a.kt)("inlineCode",{parentName:"p"},"SINK_TYPE"),"=",(0,a.kt)("inlineCode",{parentName:"p"},"grpc"),")"," requires the following variables to be set along with Generic ones"),(0,a.kt)("h3",{id:"sink_grpc_service_host"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_SERVICE_HOST")),(0,a.kt)("p",null,"Defines the host of the GRPC service."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"http://grpc-service.sample.io")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"sink_grpc_service_port"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_SERVICE_PORT")),(0,a.kt)("p",null,"Defines the port of the GRPC service."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"8500")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"sink_grpc_method_url"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_METHOD_URL")),(0,a.kt)("p",null,"Defines the URL of the GRPC method that needs to be called."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"com.tests.SampleServer/SomeMethod")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"sink_grpc_metadata"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_METADATA")),(0,a.kt)("p",null,"Defines the GRPC additional static and dynamic Metadata that allows clients to provide information to server that is associated with an RPC.\nDynamic metadata is populated by using CEL expression applied to the input payload. CEL expression should be flagged by '$' and use fully qualified package name.\nConfig format is CSV key-value pair, separated by colon. String, numeric, boolean are the dynamic values supported. Refer to official CEL documentation ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/google/cel-spec"},"https://github.com/google/cel-spec"),"."),(0,a.kt)("p",null,"Note - final metadata will be generated with merging metadata and the kafka record header. "),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"p"},"authorization:token,dlq:true,$com.goto.company.GenericPayload.field:staticvalue,$com.goto.company.GenericPayload.field_two:$(com.goto.company.GenericPayload.id + '' + com.goto.company.GenericPayload.code)"))),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"Type: ",(0,a.kt)("inlineCode",{parentName:"p"},"optional"))),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"Use case :\nExample Proto"),(0,a.kt)("pre",{parentName:"li"},(0,a.kt)("code",{parentName:"pre"},'  package com.goto.company\n\n  message GenericPayload {\n    string field = "field_name";\n    string field_two = "field_two";\n    string id = "123";\n    int code = 400;\n  }\n')),(0,a.kt)("p",{parentName:"li"},"Example config : ",(0,a.kt)("inlineCode",{parentName:"p"},"$com.goto.company.GenericPayload.field: $(com.goto.company.GenericPayload.field_two + '_' + string(com.goto.company.GenericPayload.code))"),"\nThis would result in : ",(0,a.kt)("inlineCode",{parentName:"p"},"field_name:field_two_400")))),(0,a.kt)("h3",{id:"sink_grpc_response_schema_proto_class"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS")),(0,a.kt)("p",null,"Defines the Proto which would be the response of the GRPC Method."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"com.tests.SampleGrpcResponse")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"sink_grpc_arg_keepalive_time_ms"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_ARG_KEEPALIVE_TIME_MS")),(0,a.kt)("p",null,"The keepalive ping is a way to check if a channel is currently working by sending HTTP2 pings over the transport. It is sent periodically, and if the ping is not acknowledged by the peer within a certain timeout period, the transport is disconnected. Other keepalive configurations are described ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/grpc/grpc/blob/master/doc/keepalive.md"},"here"),"."),(0,a.kt)("p",null,"Defines the period (in milliseconds) after which a keepalive ping is sent on the transport. If smaller than 10000, 10000 will be used instead."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"60000")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional")),(0,a.kt)("li",{parentName:"ul"},"Default value: ",(0,a.kt)("inlineCode",{parentName:"li"},"infinite"))),(0,a.kt)("h3",{id:"sink_grpc_arg_keepalive_timeout_ms"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_ARG_KEEPALIVE_TIMEOUT_MS")),(0,a.kt)("p",null,"Defines the amount of time (in milliseconds) the sender of the keepalive ping waits for an acknowledgement. If it does not receive an acknowledgment within this time, it will close the connection."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"5000")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional")),(0,a.kt)("li",{parentName:"ul"},"Default value: ",(0,a.kt)("inlineCode",{parentName:"li"},"20000"))),(0,a.kt)("h3",{id:"sink_grpc_arg_deadline_ms"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_ARG_DEADLINE_MS")),(0,a.kt)("p",null,"Defines the amount of time (in milliseconds) gRPC clients are willing to wait for an RPC to complete before the RPC is terminated with the error ",(0,a.kt)("a",{parentName:"p",href:"https://grpc.io/docs/guides/deadlines/#:~:text=By%20default%2C%20gRPC%20does%20not,realistic%20deadline%20in%20your%20clients."},"DEADLINE_EXCEEDED")),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"1000")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional"))),(0,a.kt)("h3",{id:"sink_grpc_tls_enable"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_TLS_ENABLE")),(0,a.kt)("p",null,"Indicates whether the sink needs to be connected over TLS. If set to true, the Firehose should establish a TLS connection with the SINK_GRPC_SERVICE_HOST."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"true")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional")),(0,a.kt)("li",{parentName:"ul"},"Default value: ",(0,a.kt)("inlineCode",{parentName:"li"},"false"))),(0,a.kt)("h3",{id:"sink_grpc_root_ca"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_ROOT_CA")),(0,a.kt)("p",null,"The CA certificates for the domain *.gojek.gcp."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"base64 encoded string")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required if SINK_GRPC_TLS_ENABLE is set to true."))),(0,a.kt)("h3",{id:"sink_grpc_response_retry_cel_expression"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_RESPONSE_RETRY_CEL_EXPRESSION")),(0,a.kt)("p",null,"Defines the CEL(Common Expression Language) expression used to evaluate whether gRPC sink call should be retried or not based on the gRPC response.\nThe given CEL expression should evaluate to a boolean value. If the expression evaluates to true, the unsuccessful gRPC sink call will be retried, otherwise it won't.\nCurrently, we support all standard CEL macro including: has, all, exists, exists_one, map, map_filter, filter\nFor more information about CEL please refer to this documentation : ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/google/cel-spec/blob/master/doc/langdef.md"},"https://github.com/google/cel-spec/blob/master/doc/langdef.md")),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"p"},"com.gotocompany.generic.GrpcResponse.success == false && com.gotocompany.generic.GenericResponse.errors.exists(e, int(e.code) >= 400 && int(e.code) <= 500)"))),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"Type: ",(0,a.kt)("inlineCode",{parentName:"p"},"optional"))),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"Default value: ``")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("p",{parentName:"li"},"Use cases :\nExample response proto :"),(0,a.kt)("pre",{parentName:"li"},(0,a.kt)("code",{parentName:"pre"},'syntax = "proto3";\npackage com.gotocompany.generic;\n\n  GenericResponse {\n      bool success = 1;\n      repeated Error errors = 2;\n  }\n\n  Error {\n      string code = 1;\n      string reason = 2;\n  }\n')),(0,a.kt)("p",{parentName:"li"},"Example retry rule : "),(0,a.kt)("ul",{parentName:"li"},(0,a.kt)("li",{parentName:"ul"},"Retry on specific error code : ",(0,a.kt)("inlineCode",{parentName:"li"},'com.gotocompany.generic.GenericResponse.errors.exists(e, e.code == "400")')),(0,a.kt)("li",{parentName:"ul"},"Retry on specific error code range : ",(0,a.kt)("inlineCode",{parentName:"li"},"com.gotocompany.generic.GenericResponse.errors.exists(e, int(e.code) >= 400 && int(e.code) <= 500)")),(0,a.kt)("li",{parentName:"ul"},"Retry on error codes outside from specific error codes : ",(0,a.kt)("inlineCode",{parentName:"li"},"com.gotocompany.generic.GenericResponse.errors.exists(e, !(int(e.code) in [400, 500, 600]))")),(0,a.kt)("li",{parentName:"ul"},"Disable retry on all cases : ",(0,a.kt)("inlineCode",{parentName:"li"},"false")),(0,a.kt)("li",{parentName:"ul"},"Retry on all error codes : ",(0,a.kt)("inlineCode",{parentName:"li"},"true"))))),(0,a.kt)("h3",{id:"sink_grpc_response_retry_error_type"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_GRPC_RESPONSE_RETRY_ERROR_TYPE")),(0,a.kt)("p",null,"Defines the ErrorType to assign for a retryable error. This is used in conjunction with ",(0,a.kt)("inlineCode",{parentName:"p"},"SINK_GRPC_RESPONSE_RETRY_CEL_EXPRESSION")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"ERROR_TYPES_FOR_RETRY"),".\nValue must be defined in com.gotocompany.depot.error.ErrorType"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"SINK_RETRYABLE_ERROR")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional")),(0,a.kt)("li",{parentName:"ul"},"Default Value: ",(0,a.kt)("inlineCode",{parentName:"li"},"DEFAULT_ERROR"))))}u.isMDXComponent=!0}}]);