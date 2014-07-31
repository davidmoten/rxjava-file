Release Notes
---------------
###Version 0.3-SNAPSHOT
* #3 add onWatchStart action to tailer to make tests non-deterministic

###Version 0.2
* update rxjava to 0.19.6
* minor api changes to ```FileObservable.tailFile``` and ```FileObservable.tailTextFile```
* #1 FileObservable.tailTextFile from end of file does not work properly
* add builder ```FileObservable.tailer()```
* #2 FileObservable.tailFile should detect Delete events

###Version 0.1 ([Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.davidmoten%7Crxjava-file%7C0.1%7Cjar))
* initial release

