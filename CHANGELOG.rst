Changelog
=========
1.5.1 (2021-04-15)
___________________
- Not crashing if the cache throws an exception

1.5.0 (2021-03-15)
___________________
- Refactored serialization out of the cache into a decorator
- Added a compression decorator
- Postgresql cache now supports binary values (for compression)

1.3.1 (2021-03-07)
___________________
- Bug fix in lazy connection pool creation

1.3.0 (2021-03-07)
___________________
- Added an option to create `psycopg2` connection pools lazily, to fix problems with forking (e.g. with uwsgi)

1.2.1 (2020-12-01)
___________________
- Fixed a bug in Postgresql cache

1.2.0 (2020-11-29)
___________________
- Can get a cached value from a cached method without calling it
- Fixed a bug in redis cache factory

1.1.1 (2020-11-26)
___________________
- Fixed a bug in Posgtresql cache

1.1.0 (2020-11-25)
___________________
- Added `cache_method`
- Passing the function and not the method to the cache creator

1.0.0 (2020-11-24)
-------------------
- First version
