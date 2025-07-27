# platforms-clients

the repo, contains all the clients for the different platforms and manages the download of social media metadata and
store it in a database.

# Installation

git clone https://github.com/ERC-BIG-5/platform-clients.git

1.

`uv sync`

2.

install optional dependencies based on the platform you want to use

`uv sync -extra <platform>`

for now there is `youtube`, `twitter` , `tiktok`

copy template.env and name the new file: .env

and comment in the lines for the credentials of the platforms you want to use.

3.

run `typer main.py run init`

This will create the main database located in `data/dbs/main.sqlite`

## Running commands

Running commands with Typer:

commands:

- `collect`: Collect with the current RUN_CONFIG
- `status`: Gives a status on the defined databases (in the RUN_CONFIG)
- `db-stats`: Get the stats post-count per month/day

### Collect

Command: `collect`

The collect command first checks the `data/task` folder for new task files, processes them and then continues collection activities.

#### Collection task general format

There are two principle collection task formats. The first is a simple collection-task, the second is a collection-task
group definition, which will be converted into a list of tasks by creating permutations of the search parameters it
defines.

Collection task files can come in 3 formats:

1. A simple collection-task (json-object)
2. A collection-task group definition (json-object)
3. A lists of collection-tasks or collection-task group definitions (the types can be mixed, it does not have to be of
   one type only).

```python
from big5_databases.databases.external import ClientTaskConfig
ClientTaskConfig.model_json_schema()
```