"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[979],{3905:function(e,t,n){n.d(t,{Zo:function(){return l},kt:function(){return p}});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var s=r.createContext({}),u=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},l=function(e){var t=u(e.components);return r.createElement(s.Provider,{value:t},e.children)},f={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,i=e.originalType,s=e.parentName,l=c(e,["components","mdxType","originalType","parentName"]),d=u(n),p=o,m=d["".concat(s,".").concat(p)]||d[p]||f[p]||i;return n?r.createElement(m,a(a({ref:t},l),{},{components:n})):r.createElement(m,a({ref:t},l))}));function p(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var i=n.length,a=new Array(i);a[0]=d;var c={};for(var s in t)hasOwnProperty.call(t,s)&&(c[s]=t[s]);c.originalType=e,c.mdxType="string"==typeof e?e:o,a[1]=c;for(var u=2;u<i;u++)a[u]=n[u];return r.createElement.apply(null,a)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},8045:function(e,t,n){n.r(t),n.d(t,{assets:function(){return l},contentTitle:function(){return s},default:function(){return p},frontMatter:function(){return c},metadata:function(){return u},toc:function(){return f}});var r=n(7462),o=n(3366),i=(n(7294),n(3905)),a=["components"],c={},s="Troubleshooting",u={unversionedId:"guides/manage",id:"guides/manage",title:"Troubleshooting",description:"Consumer Lag",source:"@site/docs/guides/manage.md",sourceDirName:"guides",slug:"/guides/manage",permalink:"/firehose/guides/manage",draft:!1,editUrl:"https://github.com/odpf/firehose/edit/master/docs/docs/guides/manage.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Deployment",permalink:"/firehose/guides/deployment"},next:{title:"Overview",permalink:"/firehose/concepts/overview"}},l={},f=[{value:"Consumer Lag",id:"consumer-lag",level:2},{value:"The caveat to the aforementioned remedies:",id:"the-caveat-to-the-aforementioned-remedies",level:3}],d={toc:f};function p(e){var t=e.components,n=(0,o.Z)(e,a);return(0,i.kt)("wrapper",(0,r.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"troubleshooting"},"Troubleshooting"),(0,i.kt)("h2",{id:"consumer-lag"},"Consumer Lag"),(0,i.kt)("p",null,"When it comes to decreasing the topic lag, it often helps to have the environment variable - ",(0,i.kt)("a",{parentName:"p",href:"../reference/configuration/#source_kafka_consumer_config_max_poll_records"},(0,i.kt)("inlineCode",{parentName:"a"},"SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS"))," config to be increased from the default of 500 to something higher."),(0,i.kt)("p",null,"Additionally, you can increase the workers in the Firehose which will effectively multiply the number of records being processed by Firehose. However, please be mindful of the caveat mentioned below."),(0,i.kt)("h3",{id:"the-caveat-to-the-aforementioned-remedies"},"The caveat to the aforementioned remedies:"),(0,i.kt)("p",null,"Be mindful of the fact that your sink also needs to be able to process this higher volume of data being pushed to it. Because if it is not, then this will only compound the problem of increasing lag."),(0,i.kt)("p",null,"Alternatively, if your underlying sink is not able to handle increased ","(","or default",")"," volume of data being pushed to it, adding some sort of a filter condition in the Firehose to ignore unnecessary messages in the topic would help you bring down the volume of data being processed by the sink."))}p.isMDXComponent=!0}}]);