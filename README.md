# Dataform to DBT

A tool to assist in migrating from [Dataform](https://dataform.co/) to [DBT](https://www.getdbt.com/).

# :warning: Disclaimer

This is very rough and ready, used to convert some private projects. It has not been well tested, it may eat your homework, kill your pets, etc. Review the created models etc carefully before using, build into test datasets, write assertions and verify the output, take all possible precautions before running in production.

# Requirements

Should work with Node 14.8+ (native modules, top level await), but has not been tested.

# Usage

The migration will create models, macros, etc in the target directory; the project can then be run and leftover Dataform config can be removed.

Review the DBT docs and create a template `dbt_project.yml` and a `profiles.yml`; `dataform-to-dbt` assumes that the production profile output will be called `prod` and will fail if it's not.

```sh
npx dataform-to-dbt \
  --directory /path/to/dataform/project \
  --template /path/to/template/dbt_project.yml \
  --profile /path/to/dbt/profiles.yml
```

# Not Implemented

Dataform stand alone tests are not yet migrated.
