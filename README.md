BenchTool for Fedora 3/4
=======================================================================================
This is a simple ingest benchmark to compare performance of Fedora 3 and Fedora 4. 
The suite consists of two main classes responsible for running the benchmarks. 

Fedora 3
--------
Tool to run an ingest benchmark against Fedora 3

### Usage

```
BenchToolFC3 <fedora-uri> <user> <pass> <num-objects> <datastream-size> [ingest|read|update|delete]
```

##### Example
Login with the user `fedoraAdmin` and the passwd `changeme` and ingest 1000 Objects each with one datastream of 1024kb size 

```
#> java -cp bench-tool-${VERSION}-jar-with-dependencies.jar org.fcrepo.bench.BenchToolFC3 http://localhost:8080/fedora fedoraAdmin changeme 1000 1024 ingest
```

Login with the user `fedoraAdmin` and the passwd `changeme` and update 1000 Objects each with one datastream of 1024kb size 

```
#> java -cp bench-tool-${VERSION}-jar-with-dependencies.jar org.fcrepo.bench.BenchToolFC3 http://localhost:8080/fedora fedoraAdmin changeme 1000 1024 update
```

Fedora 4
--------
Tool to run an ingest benchmark against Fedora 4

### Usage

``` 
BenchToolFC4 <fedora-uri> <num-objects> <datastream-size> <num-threads> [ingest|read|update|delete]
```

#### Example
Ingest 1000 Objects each with one datastream of 1024kb size using a max of 15 threads 

```
#> java -cp bench-tool-${VERSION}-jar-with-dependencies.jar org.fcrepo.bench.BenchToolFC4 http://localhost:8080/fcrepo 1000 1024 15 
```

Delete 1000 Objects with a single thread

```
#> java -cp bench-tool-${VERSION}-jar-with-dependencies.jar org.fcrepo.bench.BenchToolFC4 http://localhost:8080/fcrepo 1000 1024 1 delete
```

Results
-------
Both main classes will generate a file called `ingest.log` containing the request durations in milliseconds which can be turned easily into a graph by using e.g. gnuplot

```
gnuplot> plot "ingest.log" title "FCRepo3 Ingest" with lines
```
