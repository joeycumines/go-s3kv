# go-s3kv

Package s3kv provides simple effective key value storage for s3 or other services via 
path hashing.

Very early development, but it will be v1 complete by next week.

## Why

AWS docs strongly recommend avoiding sequential paths is it kills the shard performance
in high concurrent read write scenarios, recommending random-ish prefixes.

## How

A simple solution would be to prefix your path with a hash. This package provides 
easy consistent ways to do this.

E.g. given `logs/some-service/2018-01-01.log`, you might use this library to map output
to something like `a9d/logs/some-service/2018-01-01.log`.


# TODO: real readme
