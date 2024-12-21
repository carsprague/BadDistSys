# BadDistSys

Soon to be a fully-functional distributed system. For now, a concatenation of miscellaneous Go libraries.

## MP4

In this MP we implemented a stream processing framework with exactly-once semantics.

##### Running Commands
Use these commands to run the stream.

`app1 localfile outputfile parameter` runs a filter operation (stateless)

`app2 localfile outputfile parameter` runs a count operation (stateful)

You can also replace 'app' with 'test' to run the script which will kill 2 nodes after 1.5 seconds. This simulates failures and demonstrates that our system is fault tolerant.


## MP3

**We ended up deciding to use the recommended soltuions. Run using `go run daemon/daemon.go`**

In this MP we implemented a distributed filesystem that can handle failures. Each file is duplicated a number of times to achieve this. The user can create, get, and append to the files. The underlying structure of this filesystem is a ring, which uses a modified version of Go's consistent hashing library (https://pkg.go.dev/github.com/nobound/go-consistent).

##### Running Comands
Use these commands to interact with the filesystem.

`create localfile hydfsfile` creates the file in the system

`get hydfsfile localfile` gets the file locally

`append localfile hydfsfile` appends to the file

`multiappend hydfsfile vm1:file1 vm2:file2 ... ` performs multiple appends

`merge hydfsfile` merges appends to get one consistent file

`ls hydfsfile` prints the nodes that store the file

`list_mem_ids` lists the nodes on the ring

`store` lists the locally stored files


## MP2

**We ended up deciding to use the recommended soltuions. Run using `go run daemon/daemon.go`**

This MP was about implementing a failure detector and distributed membership list using the SWIM PingAck (+ suspicion) protocol. This protocol guarantees that we detect a failure in $2n-1$ time, but is susceptible to occasional false-positives.

#### Initialization (on a VM)
##### Server (on a VM)

```
go run server_main.go
```
This runs the server on the machine. The server opens up both TCP and UDP ports to handle both log and PingAck traffic. **Note that VM1 should be initialized first as it acts as the introducer.** Any other node can join after and it will be introduced into the system.

##### Running Comands
There are a couple commands that make it quite a bit easier to see the status of the membership list. You can just run these on the terminal that the server is running in, ignore all of the incoming and outgoing messages you see...

`list_mem` lists the membership list

`list_self` lists the local DNS, incarnation timestamp, and status

`leave` broadcasts to the system that it is leaving

`enable/disable_sus` toggles suspicion mode

`status_sus` prints the status

## MP1
This is mostly bootstrapping for the rest of the system. Each machine (up to 10) should be able to run grep on all other machines, meaning that each machine acts as both a client and server. We assume that machines can and will fail, **but will never come back online.**

#### Initialization
##### Server (on a VM)

```
go run server_main.go
```
This runs the server on the machine. Note that this should be done on all desired machines (including the client machine) before running the client.

##### Client (on a VM)

```
go run client_main.go
```
This runs the client. The client will attempt to connect to all 10 machines (including itself) via the DNS of the 10 virtual machines given to our group. **If a connection fails, the client will assume the machine is dead forever and will no longer make requests to it.**

##### Running Commands
Our system uses Go's `os/exec` library, which means we can run generic linux commands like `ls`. However, for demo we are only testing for `grep` functionality. **We require the user to enter grep commands in the format below so that our program aggregates the output data correctly** (explained below). Note that this also implies that **there must be a logs directory.** If there isn't one already, create one now.
```
grep -c "string" logs/machine.*.log && grep [flags] "string" logs/machine.*.log
```
If you follow this format, you can expect an output like this. Something to note about `grep`, mistyping a file name or any other typo will result in an error code which will outputted to the screen. **If the output looks wrong, double check the input was formatted correctly,** as only then can we guarantee an output like this:
```
baddistsys> grep -c "http" logs/machine.*.log && grep -i "http" logs/machine.*.log
2024/09/15 14:04:07 ocurrances on machine 7: 268084
2024/09/15 14:04:09 ocurrances on machine 10: 265524
2024/09/15 14:04:10 ocurrances on machine 6: 268894
2024/09/15 14:04:12 ocurrances on machine 5: 271205
2024/09/15 14:04:12 ocurrances on machine 4: 270917
2024/09/15 14:04:13 ocurrances on machine 2: 267938
2024/09/15 14:04:13 ocurrances on machine 8: 274522
2024/09/15 14:04:13 ocurrances on machine 1: 283553
2024/09/15 14:04:13 ocurrances on machine 3: 268804
2024/09/15 14:04:14 ocurrances on machine 9: 269822
2024/09/15 14:04:14 total occurances across all machines: 2709263
2024/09/15 14:04:14 elapsed time: 8.278046936s
```
Our aggregation system assumes that the line counts will be returned first by the server before any line matches. This makes the parsing logic quite simple.
##### Output
The output from any machine will be stored locally on the client in a log file. Check in `logs/m#.YYYYMMDD_HHMMSS.log` to find the full output from any given machine. Note that if you entered a `grep` command in the format above, you will find the line counts right at the start.

#### Testing
As of now we only have a couple unit tests. We cover some basic client/server initialization and communication, as well as a distributed grep command accross multiple machines.
##### Usage
Run `go tests` in the `/test` folder (must be on a VM). 
