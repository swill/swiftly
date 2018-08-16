SWIFTLY
-------

Swiftly is a utility tool for syncing a directory, namely a static website, into a bucket in OpenStack Swift.

Swiftly has been designed to publish static website to Swift so they can be served to the public.  I have been using `hugo` as my static site generator, but it should work with any html based static website.

Swiftly includes the following features:
- Create the swift bucket if it does not already exist.
- Remove files from the swift bucket if no longer present in the directory being synced.
- Replace existing files if they have changed.
- Set the `web-index` and `web-error` settings so files can be served without extensions.
- Make the container being synced to public so a website can be served from it.
- Syncs multiple files at once (check the `--concurrent` flag for details).

BUILD
=====

**Local Install**
```
go install
```

**Local Build**
```
go build
```

**Cross Compile**
Requires the `gox` package which is available here: https://github.com/mitchellh/gox
```
./_build.sh
```

USAGE
=====

**View Help**
```
swiftly -h
```

**Usage Example**
```
swiftly -dir=<public_dir> -identity=<tenant>:<user> -password=<password> -domain=<domain>
```
*Note: `-domain` will be the name of the swift bucket the contents of `-dir` will be synced to.*
