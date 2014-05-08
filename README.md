BenchTool for Fedora 3/4
=======================================================================================
This is a simple ingest/retrieve/update/delete benchmark to compare performance of Fedora 3 and Fedora 4. 
The suite consists of a jar file which can be run directly. The Fedora version will be automatically discovered depending on the given URL.
The url should include the context path of the webapp. For example `http://localhost:8080/fcrepo` for a Fedora version deployed at the context path `fcrepo`. 


### Usage

```
usage: BenchTool
 -a,--action <action>                        The action to perform. Can be
                                             one of ingest, read, update
                                             or delete. [default=ingest]
 -f,--fedora-url <fedora-url>                The URL of the Fedora
                                             instance. The url must
                                             include the context path of
                                             the webapp.
                                             [default=http://localhost:8080]
 -h,--help                                   print the help screen
 -l,--log <log>                              The log file to which the
                                             durations will get written.
                                             [default=durations.log]
 -n,--num-actions <num-actions>              The number of actions
                                             performed. [default=1]
 -p,--password <password>                    The user's password
 -pt,--prep-tx <boolean>                     Whether to perform
                                             preparation and tear down
                                             steps as transactions for
                                             supporting Fedora versions.
                                             Boolean. [default=true]
 -s,--size <size>                            The size of the individual
                                             binaries used. Sizes with a
                                             k,m,g or t postfix will be
                                             interpreted as kilo-, mega-,
                                             giga- and terabyte
                                             [default=1024]
 -t,--num-threads <num-threads>              The number of threads used
                                             for performing all actions.
                                             [default=1]
 -ta,--tx-num-actions <num-actions-per-tx>   Maximum number of actions to
                                             perform per transaction.
                                             Values <= 0 indicate
                                             unlimited actions.
                                             [default=0]
 -tp,--tx-parallel <num-parallel-tx>         Number of transactions to
                                             perform simultaneously.
                                             [default=1]
 -tx,--tx-mode <tx-mode>                     The transaction mode, can be
                                             one of none, commit or
                                             rollback. [default=none]
 -u,--user <user>                            The fedora user name
 -g,--no-purge <boolean>                     Whether or not to purge the
                                             created objects after the run.
                                             [default=true]
 -p,--property <boolean>                     Perform action ingest, read, 
                                             update, or delete on a 
                                             property
                                             [default=true]
```

Fedora 3
--------

##### Example
Login with the user `fedoraAdmin` and the passwd `changeme` and ingest 1000 Objects each with one datastream of 1024kb size 
```
#> java -jar target/bench-tool-${VERSION}-jar-with-dependencies.jar -f http://localhost:8080/fedora -u fedoraAdmin -p changeme -s 1048576 -n 1000 
```

Login with the user `fedoraAdmin` and the passwd `changeme` and update 1000 Objects each with one datastream of 1024kb size 

```
#> java -jar target/bench-tool-${VERSION}-jar-with-dependencies.jar -f http://localhost:8080/fedora -u fedoraAdmin -p changeme -s 1048576 -n 1000 -a update
```

Fedora 4
--------

#### Example
Ingest 1000 Objects each with one datastream of 1024kb size using a max of 15 threads 

```
#> java -jar target/bench-tool-${VERSION}-jar-with-dependencies.jar -f http://localhost:8080/fcrepo -s 1048576 -n 1000 -t 15 
```

Delete 1000 Objects with a single thread

```
#> java -jar target/bench-tool-${VERSION}-jar-with-dependencies.jar -f http://localhost:8080/fcrepo -s 1048576 -n 1000 -t 15 -a delete 
```

Results
-------
The durations file can be easily turned into a graph using gnuplot.  It supports a variety of output formats.

#### Example
```
gnuplot> set term svg
gnuplot> set output "durations.svg"
gnuplot> plot "durations.log" title "Duration" with lines
gnuplot> exit
```
or
```
gnuplot> set term png
gnuplot> set output "durations.png"
gnuplot> plot "durations.log" title "Duration" with lines
gnuplot> exit
```
