# Shared Memory Example in gRPC Python

## Check Our Guide First

For knowing the basics of gRPC Python and the context around the helloworld example, please checkout our gRPC Python [Quick Start guide](https://grpc.io/docs/languages/python/quickstart).

## Overview

This example demonstrates how gRPC Python can utilize the gRPC Name Resolution mechanism to specify shared memory addresses for clients and servers. The gRPC Name Resolution mechanism is documented at https://github.com/grpc/grpc/blob/master/doc/naming.md.

Specifically, this example will bind the server to the following shared memory addresses, and use clients to connect to them:

* `shmem:path`: setting the shared memory segment path relatively or absolutely
* `shmem://absolute_path`: setting the absolute path of the shared memory segment

## Prerequisite

The Python interpreter should have `grpcio` and `protobuf` installed.

## Running The Example

Starting the server:

```
$ python3 greeter_server.py
INFO:root:Server listening on: shmem:helloworld.shm
INFO:root:Server listening on: shmem:///tmp/helloworld.shm
...
```

```
$ python3 async_greeter_server.py
INFO:root:Server listening on: shmem:helloworld.shm
INFO:root:Server listening on: shmem:///tmp/helloworld.shm
...
```

Connecting with a client:

```
$ python3 greeter_client.py
INFO:root:Received: Hello to shmem:helloworld.shm!
INFO:root:Received: Hello to shmem:///tmp/helloworld.shm!
```

```
$ python3 async_greeter_client.py
INFO:root:Received: Hello to shmem:helloworld.shm!
INFO:root:Received: Hello to shmem:///tmp/helloworld.shm!
```