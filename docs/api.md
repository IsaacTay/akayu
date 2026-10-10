# API Reference

::: akayu.Stream
options:
members:
\- __init__
\- map
\- filter
\- flatten
\- collect
\- flush
\- sink
\- accumulate
\- starmap
\- batch_map
\- prefetch
\- emit
\- emit_batch
\- compile
\- chunk_by
\- drain
\- to_list
\- to_set
\- par
\- parallel
\- seq
\- union
\- combine_latest
\- zip

<!-- ## Module Functions -->

<!---->

<!-- Top-level functions for combining streams. -->

<!---->

<!-- ::: akayu.union -->

<!---->

<!-- ::: akayu.combine_latest -->

<!---->

<!-- ::: akayu.zip -->

## MultiStream

::: akayu.MultiStream
options:
  members:
    - __init__
    - map
    - filter
    - flatten
    - collect
    - flush
    - sink
    - accumulate
    - starmap
    - batch_map
    - prefetch
    - compile
    - chunk_by
    - drain
    - to_list
    - to_set
    - to_dict
    - par
    - parallel
    - seq
    - union
    - combine_latest
    - zip
    - profile
    - keys
    - values
    - items

<!-- ## Module Functions -->

<!---->

<!-- Top-level functions for combining streams. -->

<!---->

<!-- ::: akayu.union -->

<!---->

<!-- ::: akayu.combine_latest -->

<!---->

<!-- ::: akayu.zip -->

## Utility Functions

Helper functions for stream processing and file I/O.

::: akayu.to_text_file
options:
heading_level: 3

::: akayu.from_text_file
options:
heading_level: 3

::: akayu.TextFileSource
options:
heading_level: 3
