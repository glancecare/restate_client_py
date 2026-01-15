# Restate Client

A client library to access restate handlers, state, and workflows.

## Installation

```bash

pip install -e ./<PATH_TO_THIS_DIR>/restate_client

```

## Usage

```python

# Sync Client:
from restate_client import RestateClient
client = RestateClient(
    base_url="http://localhost:9070",
    debug=True,
)

result = client.gutenberg.handler_name(data)

# Async Client:
from restate_client import RestateAsyncClient
client = RestateAsyncClient(
    base_url="http://localhost:9070",
    debug=True,
)

result = await client.gutenberg.handler_name(data)
```
