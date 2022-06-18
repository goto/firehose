"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[586],{3905:function(e,t,a){a.d(t,{Zo:function(){return p},kt:function(){return h}});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},l=Object.keys(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),d=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},p=function(e){var t=d(e.components);return n.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},c=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,l=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),c=d(a),h=r,m=c["".concat(s,".").concat(h)]||c[h]||u[h]||l;return a?n.createElement(m,i(i({ref:t},p),{},{components:a})):n.createElement(m,i({ref:t},p))}));function h(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=a.length,i=new Array(l);i[0]=c;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:r,i[1]=o;for(var d=2;d<l;d++)i[d]=a[d];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}c.displayName="MDXCreateElement"},5230:function(e,t,a){a.r(t),a.d(t,{assets:function(){return p},contentTitle:function(){return s},default:function(){return h},frontMatter:function(){return o},metadata:function(){return d},toc:function(){return u}});var n=a(7462),r=a(3366),l=(a(7294),a(3905)),i=["components"],o={},s="JEXL-based Filters",d={unversionedId:"guides/jexl-based-filters",id:"guides/jexl-based-filters",title:"JEXL-based Filters",description:"To enable JEXL-based filtering, you need to set the Firehose environment variable FILTERENGINE=JEXL and provide the required JEXL filter expression to the variableFILTERJEXL_EXPRESSION .",source:"@site/docs/guides/jexl-based-filters.md",sourceDirName:"guides",slug:"/guides/jexl-based-filters",permalink:"/firehose/guides/jexl-based-filters",draft:!1,editUrl:"https://github.com/odpf/firehose/edit/master/docs/docs/guides/jexl-based-filters.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"JSON-based Filters",permalink:"/firehose/guides/json-based-filters"},next:{title:"Deployment",permalink:"/firehose/guides/deployment"}},p={},u=[{value:"JEXL Filter Expression",id:"jexl-filter-expression",level:2},{value:"Rules to write expressions:",id:"rules-to-write-expressions",level:3},{value:"Syntax",id:"syntax",level:2},{value:"Literals",id:"literals",level:3},{value:"Operators",id:"operators",level:3},{value:"<strong>Examples</strong>",id:"examples",level:2}],c={toc:u};function h(e){var t=e.components,a=(0,r.Z)(e,i);return(0,l.kt)("wrapper",(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,l.kt)("h1",{id:"jexl-based-filters"},"JEXL-based Filters"),(0,l.kt)("p",null,"To enable JEXL-based filtering, you need to set the Firehose environment variable ",(0,l.kt)("inlineCode",{parentName:"p"},"FILTER_ENGINE=JEXL")," and provide the required JEXL filter expression to the variable",(0,l.kt)("inlineCode",{parentName:"p"},"FILTER_JEXL_EXPRESSION .")),(0,l.kt)("h2",{id:"jexl-filter-expression"},"JEXL Filter Expression"),(0,l.kt)("p",null,"Filter expressions are JEXL expressions used to filter messages just after reading from Kafka and before sending to Sink."),(0,l.kt)("h3",{id:"rules-to-write-expressions"},"Rules to write expressions:"),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},"All the expressions are like a piece of Java code. Follow rules for every data type, as like writing a Java code. Parenthesis ",(0,l.kt)("inlineCode",{parentName:"li"},"()"),"can be used to combine multiple arithmetic or logical expressions into a single JEXL expression, which evaluates to a boolean value ie. ",(0,l.kt)("inlineCode",{parentName:"li"},"true")," or ",(0,l.kt)("inlineCode",{parentName:"li"},"false")),(0,l.kt)("li",{parentName:"ul"},"Start with the object reference of the schema proto class of the key/message on which you wish to apply the filter. Make sure to change the first letter of the proto class to lower case. eg - ",(0,l.kt)("inlineCode",{parentName:"li"},"sampleLogMessage")," ","(","if ",(0,l.kt)("inlineCode",{parentName:"li"},"FILTER_SCHEMA_PROTO_CLASS=com.xyz.SampleLogMessage")," ",")"),(0,l.kt)("li",{parentName:"ul"},"Access a particular field by calling the getter method on the proto object. The name of the getter method will be the field name, changed to camel-case, with all underscore ","("," ",(0,l.kt)("inlineCode",{parentName:"li"},"_"),")"," characters removed, and prefixed by the string ",(0,l.kt)("inlineCode",{parentName:"li"},"get")," eg - if the field name is ",(0,l.kt)("inlineCode",{parentName:"li"},"vehicle_type")," , then the getter method name would be ",(0,l.kt)("inlineCode",{parentName:"li"},"getVehicleType()")),(0,l.kt)("li",{parentName:"ul"},"Access nested fields using linked invocations of the getter methods, ",(0,l.kt)("inlineCode",{parentName:"li"},".")," and repeatedly call the getter method for the every level of nested field. eg - ",(0,l.kt)("inlineCode",{parentName:"li"},"sampleLogKey.getEventTimestamp().getSeconds()")),(0,l.kt)("li",{parentName:"ul"},"You can combine multiple fields of the key/message protobuf in a single JEXL expression and perform any arithmetic or logical operations between them. e.g - ",(0,l.kt)("inlineCode",{parentName:"li"},"sampleKey.getTime().getSeconds() * 1000 + sampleKey.getTime().getMillis() > 22809"))),(0,l.kt)("h2",{id:"syntax"},"Syntax"),(0,l.kt)("h3",{id:"literals"},"Literals"),(0,l.kt)("p",null,"Integer Literals\n1 or more digits from 0 to 9, eg 42."),(0,l.kt)("p",null,"Float Literals\n1 or more digits from 0 to 9, followed by a decimal point and then one or more digits from 0 to 9, optionally followed by f or F, eg 42.0 or 42.0f."),(0,l.kt)("p",null,"Long Literals\n1 or more digits from 0 to 9 suffixed with l or L , eg 42l."),(0,l.kt)("p",null,"Double Literals\n1 or more digits from 0 to 9, followed by a decimal point and then one or more digits from 0 to 9 suffixed with d or D , eg 42.0d. A special literal NaN can be used to denote Double.NaN constant"),(0,l.kt)("p",null,"String literals\nCan start and end with either ' or \" delimiters, e.g. \"Hello world\" and 'Hello world' are equivalent.\nThe escape character is \\ (backslash). Unicode characters can be used in string literals;\nUnicode escape sequences consist of:\na backslash '\\'\na 'u'\n4 hexadecimal digits (","[0-9]",",","[A-H]",",","[a-h]",").\nSuch sequences represent the UTF-16 encoding of a Unicode character, for example, 'a' is equivalent to '\\u0061'."),(0,l.kt)("p",null,"Regular expression (regex) literals\nStart with ~/ and ends with / delimiters, e.g. ~/ABC.","*","/\nThe escape character is \\ (backslash); it only escapes the string delimiter \\ (slash)"),(0,l.kt)("p",null,"Boolean literals\nThe literals true and false can be used, e.g. val1 == true"),(0,l.kt)("p",null,"Null literal\nThe null value is represented as in java using the literal null, e.g. val1 == null"),(0,l.kt)("p",null,"Array literal\nA ","[ followed by zero or more expressions separated by , and ending with ]",", e.g. ",'[ 1, 2, "three" ]',"\nThis syntax creates an ",(0,l.kt)("inlineCode",{parentName:"p"},"Object[]"),"."),(0,l.kt)("p",null,"Empty array literal can be specified as [] with result of creating Object[]\nJEXL will attempt to strongly type the array; if all entries are of the same class or if all entries are Number instance, the array literal will be an MyClass[] in the former case, a Number[] in the latter case.\nFurthermore, if all entries in the array literal are of the same class and that class has an equivalent primitive type, the array returned will be a primitive array. e.g. ","[1, 2, 3]"," will be interpreted as int[]."),(0,l.kt)("p",null,"List literal\nA ","[ followed by zero or more expressions separated by , and ending with ,...]",", e.g. ",'[ 1, 2, "three",...]',"\nThis syntax creates an ",(0,l.kt)("inlineCode",{parentName:"p"},"ArrayList<Object>"),".\nEmpty list literal can be specified as ","[...]"),(0,l.kt)("p",null,'Set literal\nA { followed by zero or more expressions separated by , and ending with }, e.g. { "one" , 2, "more"}\nThis syntax creates a ',(0,l.kt)("inlineCode",{parentName:"p"},"HashSet<Object>"),".\nEmpty set literal can be specified as {}"),(0,l.kt)("p",null,(0,l.kt)("strong",{parentName:"p"},"Map literal"),'\nA { followed by zero or more sets of key : value pairs separated by , and ending with }, e.g. { "one" : 1, "two" : 2, "three" : 3, "more": "many more" }\nThis syntax creates a ',(0,l.kt)("inlineCode",{parentName:"p"},"HashMap<Object,Object>"),".\nEmpty map literal can be specified as {:}"),(0,l.kt)("h3",{id:"operators"},"Operators"),(0,l.kt)("p",null,"In addition to the common arithmetic and logical operations, the following operators are also available."),(0,l.kt)("p",null,"Boolean and\nThe usual && operator can be used as well as the word and, e.g. cond1 and cond2 and cond1 && cond2 are equivalent.\nNote that this operator can not be overloaded"),(0,l.kt)("p",null,"Boolean or\nThe usual || operator can be used as well as the word or, e.g. cond1 or cond2 and cond1 || cond2 are equivalent.\nNote that this operator can not be overloaded"),(0,l.kt)("p",null,"Boolean not\nThe usual ! operator can be used as well as the word not, e.g. !cond1 and not cond1 are equivalent.\nNote that this operator can not be overloaded"),(0,l.kt)("p",null,"Bitwise and\nThe usual & operator is used, e.g. 33 & 4, 0010 0001 & 0000 0100 = 0."),(0,l.kt)("p",null,"Bitwise or\nThe usual | operator is used, e.g. 33 | 4, 0010 0001 | 0000 0100 = 0010 0101 = 37."),(0,l.kt)("p",null,"Bitwise xor\nThe usual ^ operator is used, e.g. 33 ^ 4, 0010 0001 ^ 0000 0100 = 0010 0100 = 37."),(0,l.kt)("p",null,"Bitwise complement\nThe usual ~ operator is used, e.g. ~33, ~0010 0001 = 1101 1110 = -34."),(0,l.kt)("p",null,"Ternary conditional ?:\nThe usual ternary conditional operator condition ? if_true : if_false operator can be used as well as the abbreviation value ?: if_false which returns the value if its evaluation is defined, non-null and non-false, e.g. val1 ? val1 : val2 and val1 ?: val2 are equivalent.\nNOTE: The condition will evaluate to false when it refers to an undefined variable or null for all JexlEngine flag combinations. This allows explicit syntactic leniency and treats the condition 'if undefined or null or false' the same way in all cases.\nNote that this operator can not be overloaded"),(0,l.kt)("p",null,'Null coalescing operator ??\nThe null coalescing operator returns the result of its first operand if it is defined and is not null.\nWhen xandyare null or undefined, x ?? \'unknown or null x\' evaluates as \'unknown or null x\' y ?? "default" evaluates as "default".\nWhen var x = 42 and var y = "forty-two",x??"other" evaluates as 42 and y??"other" evaluates as "forty-two".\nNOTE: this operator does not behave like the ternary conditional since it does not coerce the first argument to a boolean to evaluate the condition. When var x = false and var y = 0,x??true evaluates as false and y??1 evaluates as 0.\nNote that this operator can not be overloaded'),(0,l.kt)("p",null,"Equality\nThe usual == operator can be used as well as the abbreviation eq. For example val1 == val2 and val1 eq val2 are equivalent.\nnull is only ever equal to null, that is if you compare null to any non-null value, the result is false.\nEquality uses the java equals method"),(0,l.kt)("p",null,'In or Match=~\nThe syntactically Perl inspired =~ operator can be used to check that a string matches a regular expression (expressed either a Java String or a java.util.regex.Pattern). For example "abcdef" =~ "abc.',"*",' returns true. It also checks whether any collection, set or map (on keys) contains a value or not; in that case, it behaves as an "in" operator. Note that arrays and user classes exposing a public \'contains\' method will allow their instances to behave as right-hand side operands of this operator. "a" =~ ','["a","b","c","d","e",f"]'," returns true."),(0,l.kt)("p",null,'Not-In or Not-Match!~\nThe syntactically Perl inspired !~ operator can be used to check that a string does not match a regular expression (expressed either a Java String or a java.util.regex.Pattern). For example "abcdef" !~ "abc.',"*",' returns false. It also checks whether any collection, set or map (on keys) does not contain a value; in that case, it behaves as "not in" operator. Note that arrays and user classes exposing a public \'contains\' method will allow their instances to behave as right-hand side operands of this operator. "a" !~ ','["a","b","c","d","e",f"]'," returns true."),(0,l.kt)("p",null,'Starts With=^\nThe =^ operator is a short-hand for the \'startsWith\' method. For example, "abcdef" =^ "abc" returns true. Note that through duck-typing, user classes exposing a public \'startsWith\' method will allow their instances to behave as left-hand side operands of this operator.\nNot Starts With!^\nThis is the negation of the \'starts with\' operator. a !^ "abc" is equivalent to !(a =^ "abc")'),(0,l.kt)("p",null,"Ends With=$\nThe =$ operator is a short-hand for the 'endsWith' method. For example, \"abcdef\" =$ \"def\" returns true. Note that through duck-typing, user classes exposing an 'endsWith' method will allow their instances to behave as left-hand side operands of this operator."),(0,l.kt)("p",null,'Not Ends With!$\nThis is the negation of the \'ends with\' operator. a !$ "abc" is equivalent to !(a =$ "abc")'),(0,l.kt)("h2",{id:"examples"},(0,l.kt)("strong",{parentName:"h2"},"Examples")),(0,l.kt)("p",null,"Sample proto message:"),(0,l.kt)("pre",null,(0,l.kt)("code",{parentName:"pre",className:"language-text"},'===================KEY==========================\ndriver_id: "abcde12345"\nvehicle_type: BIKE\nevent_timestamp {\n  seconds: 186178\n  nanos: 323080\n}\ndriver_status: UNAVAILABLE\n\n================= MESSAGE=======================\ndriver_id: "abcde12345"\nvehicle_type: BIKE\nevent_timestamp {\n  seconds: 186178\n  nanos: 323080\n}\ndriver_status: UNAVAILABLE\napp_version: "1.0.0"\ndriver_location {\n  latitude: 0.6487193703651428\n  longitude: 0.791822075843811\n  altitude_in_meters: 0.9949166178703308\n  accuracy_in_meters: 0.39277541637420654\n  speed_in_meters_per_second: 0.28804516792297363\n}\ngcm_key: "abc123"\n')),(0,l.kt)("p",null,(0,l.kt)("em",{parentName:"p"},(0,l.kt)("strong",{parentName:"em"},"Key")),"-",(0,l.kt)("em",{parentName:"p"},(0,l.kt)("strong",{parentName:"em"},"based filter expressions examples:"))),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},'sampleLogKey.getDriverId()=="abcde12345"')),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},'sampleLogKey.getVehicleType()=="BIKE"')),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sampleLogKey.getEventTimestamp().getSeconds()==186178")),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},'sampleLogKey.getDriverId()=="abcde12345"&&sampleLogKey.getVehicleType=="BIKE"')," ","(","multiple conditions example 1",")"),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},'sampleLogKey.getVehicleType()=="BIKE"||sampleLogKey.getEventTimestamp().getSeconds()==186178')," ","(","multiple conditions example 2",")")),(0,l.kt)("p",null,(0,l.kt)("em",{parentName:"p"},(0,l.kt)("strong",{parentName:"em"},"Message -based filter expressions examples:"))),(0,l.kt)("ul",null,(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},'sampleLogMessage.getGcmKey()=="abc123"')),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},'sampleLogMessage.getDriverId()=="abcde12345"&&sampleLogMessage.getDriverLocation().getLatitude()>0.6487193703651428')),(0,l.kt)("li",{parentName:"ul"},(0,l.kt)("inlineCode",{parentName:"li"},"sampleLogMessage.getDriverLocation().getAltitudeInMeters>0.9949166178703308"))),(0,l.kt)("p",null,(0,l.kt)("em",{parentName:"p"},(0,l.kt)("strong",{parentName:"em"},"Note: Use ",(0,l.kt)("inlineCode",{parentName:"strong"},"log")," sink for testing the applied filtering"))))}h.isMDXComponent=!0}}]);