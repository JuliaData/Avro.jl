# Avro

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://juliadata.github.io/Avro.jl/stable)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://juliadata.github.io/Avro.jl/dev)
[![CI](https://github.com/JuliaData/Avro.jl/workflows/CI/badge.svg)](https://github.com/JuliaData/Avro.jl/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/JuliaData/Avro.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaData/Avro.jl)

<!-- [![deps](https://juliahub.com/docs/Avro/deps.svg)](https://juliahub.com/ui/Packages/Avro/QnF3w?t=2)
[![version](https://juliahub.com/docs/Avro/version.svg)](https://juliahub.com/ui/Packages/Avro/QnF3w)
[![pkgeval](https://juliahub.com/docs/Avro/pkgeval.svg)](https://juliahub.com/ui/Packages/Avro/QnF3w) -->

This is a pure Julia implementation of the [Apache Avro](http://avro.apache.org/docs/current/index.html) data standard. It provides convenient APIs for reading/writing data directly in the avro format, or as schema-included object container files.

## Installation

The package can be installed by typing in the following in a Julia REPL:

```julia
julia> using Pkg; Pkg.add("Avro")
```

### Implementation status

It currently supports:

  * All primitive types
  * All nested/complex types
  * Logical types listed in the spec (Decimal, UUID, Date, Time, Timestamps, Duration)
  * Binary encoding/decoding
  * Reading/writing object container files via the Tables.jl interface
  * Supports the xz, zstd, deflate, and bzip2 compression codecs for object container files

Currently not supported are:

  * JSON encoding/decoding of objects
  * Single object encoding or schema fingerprints
  * Schema resolution
  * Protocol messages, calls, handshakes
  * Snappy compression

### Package motivation

Why use the avro format vs. other data formats? Some benefits include:
  * Very concise binary encoding, especially object container files with compression
  * Very fast reading/writing
  * Objects/data must have well-defined schema
  * One of the few "row-oriented" binary data formats
