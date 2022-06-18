"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[855],{3905:function(e,n,t){t.d(n,{Zo:function(){return c},kt:function(){return m}});var r=t(7294);function o(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function i(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);n&&(r=r.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,r)}return t}function l(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?i(Object(t),!0).forEach((function(n){o(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):i(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function a(e,n){if(null==e)return{};var t,r,o=function(e,n){if(null==e)return{};var t,r,o={},i=Object.keys(e);for(r=0;r<i.length;r++)t=i[r],n.indexOf(t)>=0||(o[t]=e[t]);return o}(e,n);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)t=i[r],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var u=r.createContext({}),p=function(e){var n=r.useContext(u),t=n;return e&&(t="function"==typeof e?e(n):l(l({},n),e)),t},c=function(e){var n=p(e.components);return r.createElement(u.Provider,{value:n},e.children)},s={inlineCode:"code",wrapper:function(e){var n=e.children;return r.createElement(r.Fragment,{},n)}},f=r.forwardRef((function(e,n){var t=e.components,o=e.mdxType,i=e.originalType,u=e.parentName,c=a(e,["components","mdxType","originalType","parentName"]),f=p(t),m=o,d=f["".concat(u,".").concat(m)]||f[m]||s[m]||i;return t?r.createElement(d,l(l({ref:n},c),{},{components:t})):r.createElement(d,l({ref:n},c))}));function m(e,n){var t=arguments,o=n&&n.mdxType;if("string"==typeof e||o){var i=t.length,l=new Array(i);l[0]=f;var a={};for(var u in n)hasOwnProperty.call(n,u)&&(a[u]=n[u]);a.originalType=e,a.mdxType="string"==typeof e?e:o,l[1]=a;for(var p=2;p<i;p++)l[p]=t[p];return r.createElement.apply(null,l)}return r.createElement.apply(null,t)}f.displayName="MDXCreateElement"},3865:function(e,n,t){t.r(n),t.d(n,{assets:function(){return c},contentTitle:function(){return u},default:function(){return m},frontMatter:function(){return a},metadata:function(){return p},toc:function(){return s}});var r=t(7462),o=t(3366),i=(t(7294),t(3905)),l=["components"],a={},u="Sink Pool",p={unversionedId:"sinks/sink-pool",id:"sinks/sink-pool",title:"Sink Pool",description:"SINKPOOLNUM_THREADS",source:"@site/docs/sinks/sink-pool.md",sourceDirName:"sinks",slug:"/sinks/sink-pool",permalink:"/firehose/sinks/sink-pool",draft:!1,editUrl:"https://github.com/odpf/firehose/edit/master/docs/docs/sinks/sink-pool.md",tags:[],version:"current",frontMatter:{}},c={},s=[{value:"<code>SINK_POOL_NUM_THREADS</code>",id:"sink_pool_num_threads",level:2},{value:"<code>SINK_POOL_QUEUE_POLL_TIMEOUT_MS</code>",id:"sink_pool_queue_poll_timeout_ms",level:2}],f={toc:s};function m(e){var n=e.components,t=(0,o.Z)(e,l);return(0,i.kt)("wrapper",(0,r.Z)({},f,t,{components:n,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"sink-pool"},"Sink Pool"),(0,i.kt)("h2",{id:"sink_pool_num_threads"},(0,i.kt)("inlineCode",{parentName:"h2"},"SINK_POOL_NUM_THREADS")),(0,i.kt)("p",null,"Number of sinks in the pool to process messages in parallel."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"10")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"1"))),(0,i.kt)("h2",{id:"sink_pool_queue_poll_timeout_ms"},(0,i.kt)("inlineCode",{parentName:"h2"},"SINK_POOL_QUEUE_POLL_TIMEOUT_MS")),(0,i.kt)("p",null,"Poll timeout when the worker queue is full."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"1")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"1000"))))}m.isMDXComponent=!0}}]);