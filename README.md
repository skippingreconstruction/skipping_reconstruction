# skipping_reconstruction
This repo includes the code to produce the skipping- and eager-reconstruction plans of a query.

## To compile
1. Install the `protobuf` library
2. Build the engines
```
cd substrait_producer
Modify Makefile to link protobuf
make
```
The engine to produce the skipping reconstruction plan is in folder `engine` and the engine to produce the eager plans is in folder `baselines`. Read the file `configuration.cpp` to configure the input parameters. 
