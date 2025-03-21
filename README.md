# Overview

This repo contains:

* [src/webapp/](https://github.com/datakind/sst-app-api/tree/develop/src/webapp): The source code for the SST API (which is called by the SST frontend and by any direct API callers)
* [src/worker/](https://github.com/datakind/sst-app-api/tree/develop/src/worker): The source code for the SFTP Worker (which calls the SST API)
* [terraform/]
(https://github.com/datakind/sst-app-api/tree/develop/terraform): The Terraform configuration for the SST API/Frontend and other GCP resources including Cloud SQL setup, networking setup, secrets setup
* .devcontainer/ and .vscode/: which allow easy setup if you are using VSCode as your IDE.
* [devtools/](https://github.com/datakind/sst-app-api/tree/develop/devtools): is a place to put utility scripts
* .github/: contains mostly copied over files when this directory was forked from the student-success-tool repo, so likely much of it is outdated. The only Github action we've added is the [webapp-and-worker-precommit](https://github.com/datakind/sst-app-api/blob/develop/.github/workflows/webapp-and-worker-precommit.yml) which is run on every push to develop. This action contains a python linter (we use [black](https://black.readthedocs.io/en/stable/)), and automated runs of the unit tests in the src/webapp/ and src/worker/ directories.
* Additionally, [pyproject.toml](https://github.com/datakind/sst-app-api/blob/develop/pyproject.toml) and [uv.lock](https://github.com/datakind/sst-app-api/blob/develop/uv.lock) are important for dependency management. At time of writing, the worker is just skeleton code so there's no separate dependency management. In the long-term consider separating out the dependency management for the two programs. 


NOTE: this repo was forked from the https://github.com/datakind/student-success-tool repo, which means some of the static files (e.g. CONTRIBUTING.md) may be outdated or may include irrelevant information from that repo. Please update those as you see fit. For information about the specific items listed above, defer to the specific readmes in the relevant directory.
