Compare ingest performance of Fedora 3 and Fedora 4
=======================================================================================

The suite consists of two main classes responsible for running the benchmark on fcrepo3/fcrepo4. 

Fedora 3
--------
Tool to run an ingest benchmark against Fedora 3

### Usage

```
BenchToolFC3 <fedora-uri> <user> <pass> <num-objects> <datatstream-size>
```

##### Example
Login with the user `fedoraAdmin` and the passwd `changeme` and ingest 1000 Objects each with one datastream of 1024kb size 

```
#> java -cp bench-tool-${VERSION}-jar-with-dependencies.jar org.fcrepo.bench.BenchToolFC3 http://localhost:8080/fedora fedoraAdmin changeme 1000 1024 
```


Fedora 4
--------
Tool to run an ingest benchmark against Fedora 4

### Usage

``` 
BenchToolFC4 <fedora-uri> <num-objects> <datatstream-size> <num-threads>
```

#### Example
Ingest 1000 Objects each with one datastream of 1024kb size using a max of 15 threads 

```
#> java -cp bench-tool-${VERSION}-jar-with-dependencies.jar org.fcrepo.bench.BenchToolFC4 http://localhost:8080/fcrepo 1000 1024 15 
```

Results
-------
Both main classes will generate a file called `ingest.log` containing the request durations in milliseconds which can be turned easily into a graph by using e.g. gnuplot

```
gnuplot> plot "ingest.log" title "FCRepo3 Ingest" with lines
```