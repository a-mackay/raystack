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
===============================================================================
 Language            Files        Lines         Code     Comments       Blanks
===============================================================================
 Markdown                1           38            0           30            8
 TOML                    2           31           25            0            6
-------------------------------------------------------------------------------
 Rust                    9         3205         2704           31          470
 |- Markdown             7          329           64          241           24
 (Total)                           3534         2768          272          494
===============================================================================
 Total                  12         3603         2793          302          508
===============================================================================
```

<br><br>

Many thanks to Steve Eynon for his [post about SkySpark's SCRAM authentication](http://www.alienfactory.co.uk/articles/skyspark-scram-over-sasl).
