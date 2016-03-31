Illuminate is a small set of scripts that helps us gather and process user data.

## Requirements

- Node.js
- Postgres

## Gathering Data

It's probably easiest to create a couple scripts in your root to run the sync scripts.

**sync-local.sh**
```
export MC_AUTH_USER="MOBILE COMMONS USER"
export MC_AUTH_PASSWORD="MOBILE COMMONS PASSWORD"

node api-profile-sync.js --local
node api-messages-sync.js --local
```

**sync-aws.sh**
```
export MC_AUTH_USER="MOBILE COMMONS USER"
export MC_AUTH_PASSWORD="MOBILE COMMONS PASSWORD"

export SHINE_API_SYNC_USER="DB USER"
export SHINE_API_SYNC_PASSWORD="DB PASSWORD"
export SHINE_API_SYNC_HOST="DB HOST"
export SHINE_API_SYNC_PORT=DB_PORT_NUMBER     # Number. Defaults to 5432
export SHINE_API_SYNC_SSL=DB_SSL              # Boolean. Defaults to true

node api-profile-sync.js --aws
node api-messages-sync.js --aws
```

Then to run the scripts you can just do...
```
$ ./sync-local.sh
$ ./sync-aws.sh
```

## Processing Data

```
$ node run-tasks.js
```

Use the `--help` option to learn more.
