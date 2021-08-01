# mtf-rs
Low level parser of `Microsoft Tape Format` files.

This only provides low level functionality, parsing the various `DBLK`s and `Stream`s and does not aim to provide higher level functionality such as unpacking of common `mtf` files created by backup tools.

Instead this library is meant as a building block for such tools. A basic example for this is the `MTFPageProvider` provided by this crate, which can be used together with [mdf-rs](https://github.com/rroohhh/mdf-rs) to parse Microsoft SQL Server backups directly (without unpacking the `.BAK` file).
