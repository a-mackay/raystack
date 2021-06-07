# raystack

[![crates.io](https://img.shields.io/crates/v/raystack.svg)](https://crates.io/crates/raystack)
[![Documentation](https://docs.rs/raystack/badge.svg)](https://docs.rs/raystack)
![Build Status](https://github.com/a-mackay/raystack/workflows/build/badge.svg)
![Audit Status](https://github.com/a-mackay/raystack/workflows/audit/badge.svg)


A SkySpark 3 client library for Rust, compatible with SkySpark versions 3.0.28 or higher.

For SkySpark versions before 3.0.28, use version `0.8.*` of this library. Newer versions
of SkySpark use [Hayson encoding](https://github.com/j2inn/hayson), which this library
supports. The older versions of SkySpark used a different JSON encoding which is no
longer supported by this library.

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
 Markdown                1           43            0           34            9
 TOML                    2           31           25            0            6
-------------------------------------------------------------------------------
 Rust                   11         3531         2956           30          545
 |- Markdown             9          348           64          260           24
 (Total)                           3879         3020          290          569
===============================================================================
 Total                  14         3605         2981           64          560
===============================================================================
```

<br><br>

Many thanks to Steve Eynon for his [post about SkySpark's SCRAM authentication](http://www.alienfactory.co.uk/articles/skyspark-scram-over-sasl).
