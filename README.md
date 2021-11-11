# raystack

[![crates.io](https://img.shields.io/crates/v/raystack.svg)](https://crates.io/crates/raystack)
[![Documentation](https://docs.rs/raystack/badge.svg)](https://docs.rs/raystack)
![Build Status](https://github.com/a-mackay/raystack/workflows/build/badge.svg)
![Audit Status](https://github.com/a-mackay/raystack/workflows/audit/badge.svg)


An asynchronous SkySpark 3 client library for Rust, compatible with SkySpark versions 3.1.1 or higher.

For SkySpark versions before 3.0.28, use version `0.8.*` of this library. Newer versions
of SkySpark use [Hayson encoding](https://github.com/j2inn/hayson), which this library
supports. The older versions of SkySpark used a different JSON encoding which is no
longer supported by this library.

For SkySpark versions 3.0.28 or 3.0.29, use version `0.11.*` of this library, because it uses older Haystack ops for
`formats` and `ops`.

## Documentation
See [docs.rs](https://docs.rs/raystack).

## Features
* SkySpark REST API `eval` operation.
* Partial implementation of the Project Haystack REST API.
    * Most Haystack ops have been implemented.
    * Some Haystack ops (like watches) are currently unimplemented (pull requests are welcome).

## Synchronous raystack

If you don't want to use asynchronous Rust, or are just writing a quick
script, a synchronous version of this library is available, called
[raystack_blocking](https://crates.io/crates/raystack_blocking) ([source code](https://github.com/a-mackay/raystack_blocking))

## Code Statistics
```
===============================================================================
 Language            Files        Lines         Code     Comments       Blanks
===============================================================================
 Markdown                1           43            0           34            9
 TOML                    2           35           29            0            6
-------------------------------------------------------------------------------
 Rust                   11         3559         2996           29          534
 |- Markdown             9          344           69          251           24
 (Total)                           3903         3065          280          558
===============================================================================
 Total                  14         3637         3025           63          549
===============================================================================
```

<br><br>

Many thanks to Steve Eynon for his [post about SkySpark's SCRAM authentication](http://www.alienfactory.co.uk/articles/skyspark-scram-over-sasl).
