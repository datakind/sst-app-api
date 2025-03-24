# Overview of the REST API for SST

## See Swagger UI for self-documentation of all currently available API endpoints

Go to `<env>-sst.datakind.org/api/v1/docs`: e.g. https://dev-sst.datakind.org/api/v1/docs

Note that dev and staging links are all behind a GCP Identity-Aware Proxy.

## Authentication

Authentication of the API is primarily via JWTs in the Authorization header of the HTTP calls. These JWTs are short-term tokens that expire and are signed by the API. They are slightly more secure than using API keys directly to authenticate every call as API keys are long-term/non-expiring authentication tokens which are more powerful if stolen. So the mechanism used here is that API keys are exchanged for JWTs, which are then used to authenticate each call and can store additional information such as "enduser" identity.

Note that intentionally, there is no way to use the user table's email/password combination to authenticate to the API directly. An API key is required. This means that a "user" is almost only a frontend concept. However, we do have to retain access to the user table in the backend to look up access type/institution and such. See more in the databases section below.

There are also multiple types of API keys, and they can have the same access types as a user. Additionally, API keys can have the additional attribute of "allows_enduser". BE CAUTIOUS WHEN SETTING THIS TO TRUE when generating API keys. This means that this API key can impersonate any user. This API key should really only be used for the frontend, which needs to do enduser impersonation. Note that only DATAKINDER access types can allow endusers. 

Additionally, API keys, like users, can have an institution set. DATAKINDER type keys should not set an institution.

### Authenticate/Generating Tokens via the Swagger UI

NOTE: Treat keys as secrets. These will grant full access to this API.

0. Get a valid API key. In the LOCAL environment, you can use the `INITIAL_API_KEY` set in your `.env` file or `key_1`. In other environments, you can use `INITIAL_API_KEY` set in the `.env` file or any existing generated API keys.
1. Hit the authorize button on the top right, enter a valid API key in the `api-key` field (ignore the `api_key_scheme` which contains username/password -- this is to allow FastAPI to auto populate bearer tokens generated from the API keys; the actual username/password fields intentionally do not work).
2. Then generate a token using the `/token-from-api-key` POST method. This will be the only endpoint you can access with the API key directly.
3. Get the resulting token value which you can then use to CURL to any endpoint. For example, 

```
$ curl -X 'GET' \
  'http://127.0.0.1:8000/api/v1/non-inst-users' \
  -H 'Authorization: Bearer <paste_the_token_here>'
```

In the long-term, look into a way to have the API key --> token conversion be handled directly by FastAPI so that the Swagger UI can do the conversion directly and you won't have to curl with your token.

## Databases

All data is stored in MySQL databases for dev/staging/prod, these are databases in GCP's Cloud SQL. In the local environment, the database is sqlite. The main file you'll want to look at for database table definitions is [src/webapp/database.py](https://github.com/datakind/sst-app-api/blob/develop/src/webapp/database.py).

At time of writing, the databases the API cares about and tracks, are as follows:

* Institution Table ("inst"): the institutions, including info about them like PDP ID if applicable, creator/creation time, etc.
* API Key Table ("apikey"): the API keys including access type, valid status (you can disable a key), etc.
* Account Table ("users"): **THIS TABLE IS (the only table) SHARED WITH THE FRONTEND**. This contains enduser email/password, access types, inst if applicable etc. Because this table is shared with the frontend, any changes to the table definition should be reflected in both the ORM handling the table in the frontend _and_ the backend. Note that intentionally, there's no way to create new users from the backend. This is because the backend only uses API keys to authenticate and also lacks some reqiured fields such as team id generation that is required by Laravel to use the user table. The frontend can directly create users in the table which the backend will be able to read.
* Account History Table ("account_history"): audit trail of certain events undertaken by users. TODO: interactions with this table largely remain unimplemented.
* File Table ("file"): tracks files
* Batch Table ("batch"): tracks batches
* Model Table ("model"): tracks models
* Job Table ("job"): tracks Databricks jobs, storing the per-run unique job_run_id. Status of the job is also partially tracked here. Note that failed jobs are currently indistinguishable from incomplete jobs. 

NOTE: naming convention is to use a singular descriptor for the table name, however, the "users" table has to follow Laravel's table naming convention, which has the users table called "users".

### Tip

If for any reason you want to manipulate the local database directly during local development or testing, you can do that via the sqlite commandline functionality where you can issue direct sql queries to your local sqlite database https://sqlite.org/cli.html.

The local database is sqlite, the deployed databases in GCP are all mysql.

## Testing

Unit test files are named `<file_under_test>_test.py` to correspond with the files they are testing. Unit tests only test behavior introduced by logic written in those files and do not test any integration with other systems. To respect test isolation, we have the following levels of testing:

1. Unit tests where all other systems are mocked out (e.g. Databricks, GCP storage etc.)
2. Dev environment with fake data to test integration on real systems, as all integration points are connected to the real endpoints in Databricks, in GCP etc.
3. Staging environment with real data (potentially sampled if your datasets are large) to test real data flowing through the full end to end setup of a real system that mimics prod.
4. Prod environment with real data on real systems.


That means for functions that are mainly doing integration pieces, we do not have unit tests for them, as we assume external systems work and mocking and testing these integration points would be near-useless. These can be tested at level 2 in the dev environment, which is setup for just this purpose.

This also means, it's not recommended for the local environment to connect to the dev environment. The four environments, `local`, `dev`, `staging`, `prod`, should also be isolated from each other.

While working in the local environment, it's recommended you mock out/stub out the calls to external systems. If you don't want to do that feel free to look into official documentation on how to auth to GCS and to Databricks from your local environment.

### Comment on Deployment

* Dev environment: gets deployed upon any new commit to b/develop.
* Staging environment: requires manual Cloud Build Trigger Run initiated by a human to pick up the most recent changes from b/develop.
* Prod environment: requires manual Cloud Build Trigger Run initiated by a human to pick up the most recent changes from b/develop.

For more information on deployment, see the Terraform setup and the GCP setup in the GCP console.

## Package Management

Package management is done via [uv](https://docs.astral.sh/uv/). When adding a new package, add it according to the uv documents and keep the `uv.lock` and `pyproject.toml` files up to date.

## Local Environment Setup

Enter into the root directory of the repo.

1. Copy the `./src/webapp/.env.example` file to `./src/webapp/.env`
1. `python3 -m venv .venv`
1. `source .venv/bin/activate`
1. `pip install uv`
1. `uv sync --all-extras --dev`

You're now in your virtual env with all your dependencies added.

For all of the following, the steps above are pre-requisites and you should be in the root folder of `sst-app-api/`.

### Spin up the app locally:

1. `export ENV_FILE_PATH=<full_path_to_your_webapp_.env_file>` 
1. `fastapi dev src/webapp/main.py --port 8000`
1. Go to `http://127.0.0.1:8000/api/v1/docs`
1. Hit the `Authorize` button on the top right and enter `key_1` in the `api-key` field (scroll past/ignore `api_key_scheme` fields).
1. Generate a token using the `/token-from-api-key` POST method.
1. Use the token in to CURL to any endpoint. For example, 

```
$ curl -X 'GET' \
  'http://127.0.0.1:8000/api/v1/non-inst-users' \
  -H 'Authorization: Bearer <paste_the_token_here>'
```

### Before committing, run the formatter and run the unit tests

1. Formatter: `black src/webapp/.`
1. Unit tests: `coverage run -m pytest  -v -s ./src/webapp/`

#### Optionally run pylint

`uv run pylint './src/webapp/*' --errors-only` for only errors.

Non-error Pylint is very opinionated, and **SOMETIMES WRONG**. For example, there exist warnings to switch `== NONE` to `is None` for SQL query where clauses. THIS WILL CAUSE THE SQL QUERY TO NOT WORK -- (it appears to be due to how SqlAlchemy understands the clauses). So be careful when following the recommendations from pylint.

## Environment Variables

For the deployed instances (dev, staging, prod) database related environment variables are set in the [terraform templates](../../terraform/modules/service/main.tf#L39), 
but most are set per project in [Cloud Secret Manager](https://console.cloud.google.com/security/secret-manager/secret/dev-webapp-env-file/versions?project=dev-sst-02). To update the environment
variables in secret manager, create a new version from the UI, copying the previous values with
your changes.

## Usage Notes

Some general things that may be helpful to call out.

### Adding a Datakinder vs an institutional user

The flow to add a Datakinder user is different from adding a user to an institution:

* adding a user to an institution has to happen prior to that user creating an account (by allowlisting their email for a given institution)
* adding a Datakinder user has to happen after the Datakinder person has already created their account, then their account's access type is updated.

### Uploading files

The process to upload a file involves three API calls:
1. Get the GCS upload URL: `GET /institutions/{inst_id}/upload-url/{file_name}`
1. Post to the GCS upload URL: `POST <the_gcp_url_returned_from_step_1>`
1. Validate the file: `POST /institutions/{inst_id}/input/validate-upload/{file_name}` OR `POST /institutions/{inst_id}/input/validate-sftp/{file_name}` -- depending on what input mechanism your file used. This sets a field in the File database table which indicates the source of the file (`MANUAL_UPLOAD` etc.) which is helpful information for the frontend.

## Local VSCode Debugging

From the Run & Debug panel (⇧⌘D on 🍎) you can run the [debug launch config](../../.vscode/launch.json) for the webapp or worker modules. This will allow you to set breakpoints within the source code while the applications are running.
