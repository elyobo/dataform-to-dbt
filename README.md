# Dataform to DBT

A tool to assist in migrating from [Dataform](https://dataform.co/) to [DBT](https://www.getdbt.com/).

## What works

- Models are converted, with Dataform `${ref()}` calls replaced by appropriate DBT `{{ ref() }}` and `{{ source() }}` calls.
- DBT currently doesn't support namespacing, so model names must be unique; `dataform-to-dbt` supports renaming tables as part of the migration, correcting references along the way.
- Source and model definitions are output in schema specific model and source `.yml`.
- Model documentation is included in the model `.yml`.
- Dataform [auto-generated assertions](https://docs.dataform.co/guides/assertions#auto-generated-assertions) (`uniqueKey`, `nonNull`, and `rowConditions`) are migrated to appropriate tests in the model `.yml`, using built in tests where possible and [`dbt_utils`](https://github.com/dbt-labs/dbt-utils) tests where not.
- Dataform [manual assertions](https://docs.dataform.co/guides/assertions#manual-assertions) are migrated DBT [singular tests](https://docs.getdbt.com/docs/build/tests#singular-tests).

## What might work

- Dataform [includes](https://docs.dataform.co/guides/javascript/includes) are out of scope to ever work completely, as they can do most things javascript can and translating that to DBT's jinja based macros just isn't going to happen. That said, where the include is a simple string, a [DBT macro](https://docs.getdbt.com/docs/build/jinja-macros) will be produced and inserted in the right location so that it's still a single replacement (instead of inlining the replacement and duplicating it at the DBT version). Includes that call functions are _not_ supported, but a stub macro will be produced and inserted in the right place and a warning produced so that they can be manually adjusted (by amending the implementation to actually work, and by amending all the invocations to pass appropriate arguments).
- DBT doesn't support generating temporary tables in the model header as you can with dataform pre-operations (not with a `ref()`, anyway), so these are extracted to their own tables and the references updated.
- Table materialisations aren't set (except for extracted temp tables), views are - check the default materialisations set in the DBT config and adjust as necessary.

## What doesn't work

- View vs table materializations are not supported.
- No support for incremental datasets, these will require manual adjustments.
- No support for clustering, partitioning.
- Tags are not migrated.

# :warning: Disclaimer

This is very rough and ready, used to convert some private projects. It has not been well tested, it may eat your homework, kill your pets, etc. Review the created models etc carefully before using, build into test datasets, write assertions and verify the output, take all possible precautions before running in production.

# Requirements

Should work with Node 14.8+ (native modules, top level await), but has not been tested.

# Usage

The migration will create models, macros, etc in the target directory; the project can then be run and leftover Dataform config can be removed.

Review the DBT docs and create a template `dbt_project.yml` and a `profiles.yml`; `dataform-to-dbt` assumes that the production profile output will be called `prod` and will fail if it's not.

Some Dataform assertions convert to [`dbt_utils`](https://github.com/dbt-labs/dbt-utils) tests (row conditions and multi-column unique assertions), so [install `dbt_utils`](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) if that applies.

```sh
# Get help
npx dataform-to-dbt --help

# Run a migration
npx dataform-to-dbt \
  --directory /path/to/dataform/project \
  --template /path/to/template/dbt_project.yml \
  --profile /path/to/dbt/profiles.yml
```
