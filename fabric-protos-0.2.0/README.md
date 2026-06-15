# Hyperledger Fabric gRPC Service Definitions

This repository contains the [grpc] service and [protocol buffer][protobuf] definitions for the Hyperledger Fabric project.
Tools like `protoc` can transform these definitions into code that can be used by clients and libraries to interact with Fabric.

## Building and testing

Note: there are currently two build processes- an Azure devops based build for Go bindings and a newer GitHub Actions based build for Java and Node.js bindings.
The intention is to move everything to the new GitHub Actions build, which uses the Makefile described below.

### Build using make

The following Makefile targets are available:

- `make lint` - run [Buf] linter and breaking change detection
- `make javabindings` - run Java bindings code generation
- `make nodebindings` - run Node.js bindings code generation

[Buf]: https://github.com/bufbuild/buf
[grpc]: https://grpc.io/docs/guides/
[protobuf]: https://github.com/protocolbuffers/protobuf/
