# API Reference

Generated from the docstrings in the type stub (`python/akayu/__init__.pyi`).

::: akayu.Stream

::: akayu.MultiStream

::: akayu.union

::: akayu.zip

::: akayu.combine_latest

::: akayu.from_text_file

::: akayu.TextFileSource

::: akayu.to_text_file

::: akayu.ProfileReport

## Errors

Every error akayu raises about a graph's state is an `akayu.StreamError`, itself a
`RuntimeError`, so `except RuntimeError` still catches them. Invalid arguments raise `ValueError`
or `TypeError`. An exception from your own function arrives as a `CallbackError`, with the
original as its `__cause__`; `KeyboardInterrupt`, `SystemExit` and other exceptions that aren't
`Exception` subclasses pass through unwrapped.

::: akayu.StreamError

::: akayu.CallbackError

::: akayu.FrozenGraphError

::: akayu.NotASourceError

::: akayu.ReentrantError

::: akayu.BusyError

::: akayu.DeadlockError
