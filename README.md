# raystack

[![crates.io](https://img.shields.io/crates/v/raystack.svg)](https://crates.io/crates/raystack)
[![Documentation](https://docs.rs/raystack/badge.svg)](https://docs.rs/raystack)
![](https://github.com/a-mackay/raystack/workflows/.github/workflows/rust.yml/badge.svg)


A SkySpark 3 client library for Rust.

## Documentation
See [docs.rs](https://docs.rs/raystack).

## Features
* SkySpark REST API `eval` operation.
* Partial implementation of the Project Haystack REST API.
    * Most Haystack ops have been implemented.
    * Some Haystack ops (like watches) are unimplemented because I haven't looked into how I can test them.

## Future Plans
* Switch to async/await once the Rust ecosystem matures in a few months.
* Refine the public API.


<br><br>

Many thanks to Steve Eynon for his [post about SkySpark's SCRAM authentication](http://www.alienfactory.co.uk/articles/skyspark-scram-over-sasl).
