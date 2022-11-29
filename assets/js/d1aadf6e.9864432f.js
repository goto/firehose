"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[402],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return b}});var r=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,r,i=function(e,t){if(null==e)return{};var n,r,i={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=r.createContext({}),u=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},p=function(e){var t=u(e.components);return r.createElement(s.Provider,{value:t},e.children)},l={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},f=r.forwardRef((function(e,t){var n=e.components,i=e.mdxType,o=e.originalType,s=e.parentName,p=c(e,["components","mdxType","originalType","parentName"]),f=u(n),b=i,m=f["".concat(s,".").concat(b)]||f[b]||l[b]||o;return n?r.createElement(m,a(a({ref:t},p),{},{components:n})):r.createElement(m,a({ref:t},p))}));function b(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var o=n.length,a=new Array(o);a[0]=f;var c={};for(var s in t)hasOwnProperty.call(t,s)&&(c[s]=t[s]);c.originalType=e,c.mdxType="string"==typeof e?e:i,a[1]=c;for(var u=2;u<o;u++)a[u]=n[u];return r.createElement.apply(null,a)}return r.createElement.apply(null,n)}f.displayName="MDXCreateElement"},2592:function(e,t,n){n.r(t),n.d(t,{assets:function(){return p},contentTitle:function(){return s},default:function(){return b},frontMatter:function(){return c},metadata:function(){return u},toc:function(){return l}});var r=n(7462),i=n(3366),o=(n(7294),n(3905)),a=["components"],c={},s="Bigtable Sink",u={unversionedId:"sinks/bigtable-sink",id:"sinks/bigtable-sink",title:"Bigtable Sink",description:"Bigtable Sink is implemented in Firehose using the Bigtable sink connector implementation in ODPF Depot. You can check out ODPF Depot Github repository here.",source:"@site/docs/sinks/bigtable-sink.md",sourceDirName:"sinks",slug:"/sinks/bigtable-sink",permalink:"/firehose/sinks/bigtable-sink",draft:!1,editUrl:"https://github.com/odpf/firehose/edit/master/docs/docs/sinks/bigtable-sink.md",tags:[],version:"current",frontMatter:{}},p={},l=[{value:"Configuration",id:"configuration",level:3}],f={toc:l};function b(e){var t=e.components,n=(0,i.Z)(e,a);return(0,o.kt)("wrapper",(0,r.Z)({},f,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"bigtable-sink"},"Bigtable Sink"),(0,o.kt)("p",null,"Bigtable Sink is implemented in Firehose using the Bigtable sink connector implementation in ODPF Depot. You can check out ODPF Depot Github repository ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/depot"},"here"),"."),(0,o.kt)("h3",{id:"configuration"},"Configuration"),(0,o.kt)("p",null,"For Bigtable sink in Firehose we need to set first (",(0,o.kt)("inlineCode",{parentName:"p"},"SINK_TYPE"),"=",(0,o.kt)("inlineCode",{parentName:"p"},"bigtable"),"). There are some generic configs which are common across different sink types which need to be set which are mentioned in ",(0,o.kt)("a",{parentName:"p",href:"/firehose/advance/generic"},"generic.md"),". Bigtable sink specific configs are mentioned in ODPF Depot repository. You can check out the Bigtable Sink configs ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/odpf/depot/blob/main/docs/reference/configuration/bigtable.md"},"here")))}b.isMDXComponent=!0}}]);