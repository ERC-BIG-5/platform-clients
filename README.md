# platforms-clients

the repo, contains all the clients for the different platforms and manages the download of social media metadata and
store it in a database.

# Installation

git clone <this_repo>

1. 
uv sync

2.
install optional dependencies based on the platform you want to use
uv sync -e <platform> e.g.
`uv sync -e youtube`
`uv sync -e twitter`

copy template.env and name the new file: .env

and comment in the lines for the credential of the platforms you want to use.

3.
run `typer main.py run init`

This will create the main databse located in `data/dbs/main.sqlite`

## Running commands

Running commands with Typer:

commands:

- collect: Collect with the current RUN_CONFIG
- status: Gives a status on the defined databases (in the RUN_CONFIG)
- db-stats: Get the stats post-count per month/day
