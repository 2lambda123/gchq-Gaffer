#!/usr/bin/env bash
curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d @../json-operation-examples/operation_getAllElements.json 'http://localhost:8080/rest/graph/operations/execute'
