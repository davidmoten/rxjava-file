rxjava-file
===========

Status: *pre-alpha*

Requires Java 7.

Observable utilities for files:
* tail a text file using Java 7 and later NIO ```WatchService``` events
* stream ```WatchEvent```s from a ```WatchService```

Continuous integration with Jenkins for this project is [here](https://xuml-tools.ci.cloudbees.com/). <a href="https://xuml-tools.ci.cloudbees.com/"><img  src="http://web-static-cloudfront.s3.amazonaws.com/images/badges/BuiltOnDEV.png"/></a>

Maven site reports are [here](http://davidmoten.github.io/rxjava-file/index.html) including [javadoc](http://davidmoten.github.io/rxjava-file/apidocs/index.html).

Getting started
----------------
Add this maven dependency to your pom.xml:
```xml
<dependency>
  <groupId>com.github.davidmoten</groupId>
  <artifactId>rxjava-file</artifactId>
  <version>0.1-SNAPSHOT</version>
</dependency>
```

How to build
----------------

```bash
git clone https://github.com/davidmoten/rxjava-file
cd rxjava-file
maven clean install 
```



