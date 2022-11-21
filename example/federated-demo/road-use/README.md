Copyright 2022 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Federated Demo - Road Use
=========================

This folder contains additional examples of operations that can be run.

## JSON
The JSON files contain examples of a few of the key operations avaliable. These can be used by copying the JSON directly into Swagger or through curl commands. An example of doing this using curl can be found under ./scripts

## Scripts
The createGraphs script use curl and the json files to automatically generate 3 graphs and populate them with some data.

This script should be run from within this directory (road-use)

```bash
./scripts/createGraphs.sh
```
