# raystack

[![crates.io](https://img.shields.io/crates/v/raystack.svg)](https://crates.io/crates/raystack)
[![Documentation](https://docs.rs/raystack/badge.svg)](https://docs.rs/raystack)
![Build Status](https://github.com/a-mackay/raystack/workflows/build/badge.svg)
![Audit Status](https://github.com/a-mackay/raystack/workflows/audit/badge.svg)


A SkySpark 3 client library for Rust.

## Documentation
See [docs.rs](https://docs.rs/raystack).

## Features
* SkySpark REST API `eval` operation.
* Partial implementation of the Project Haystack REST API.
    * Most Haystack ops have been implemented.
    * Some Haystack ops (like watches) are currently unimplemented (pull requests are welcome).

## Code Statistics
```
-------------------------------------------------------------------------------
 Language            Files        Lines         Code     Comments       Blanks
-------------------------------------------------------------------------------
 Markdown                1           35           35            0            0
 Rust                   12         3935         3020          405          510
 TOML                    2           26           22            0            4
-------------------------------------------------------------------------------
 Total                  15         3996         3077          405          514
-------------------------------------------------------------------------------
```

<br><br>

Many thanks to Steve Eynon for his [post about SkySpark's SCRAM authentication](http://www.alienfactory.co.uk/articles/skyspark-scram-over-sasl).
