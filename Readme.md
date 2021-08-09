[![codecov](https://codecov.io/gh/diegodesousas/go-rabbitmq/branch/main/graph/badge.svg?token=XC6DRT9X8J)](https://codecov.io/gh/diegodesousas/go-rabbitmq)
[![Development](https://github.com/diegodesousas/go-rabbitmq/actions/workflows/development.yml/badge.svg)](https://github.com/diegodesousas/go-rabbitmq/actions/workflows/development.yml)

# High level package for rabbitmq interaction. 

## Features

- Simple configuration bootstrap.
- Gracefully shutting down.
- Consume messages in parallel specifying a number of goroutines.
- Publish messages with confirmation by default.
- Run various consumers with same connection handler.