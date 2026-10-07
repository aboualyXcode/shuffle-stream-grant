# 4. Asset Bundles and CI/CD

Databricks Asset Bundles describe jobs, pipelines and other resources as code in a `databricks.yml`, so they can be reviewed, validated and deployed like any other software. The **Grant** lab's bundle challenges cover the parts that most often go wrong.

## The shape of a bundle

```yaml
bundle:
  name: orders                      # required

variables:
  catalog:
    description: Catalog to publish to

resources:
  pipelines:
    orders_pipeline:
      name: orders-pipeline
      catalog: ${var.catalog}
      schema: orders
      serverless: true
      libraries:
        - file:
            path: ./pipelines/orders.sql
  jobs:
    nightly:
      name: nightly-orders
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: UTC
      tasks:
        - task_key: refresh
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
        - task_key: report
          depends_on:
            - task_key: refresh
          notebook_task:
            notebook_path: ./notebooks/report.py

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: dev
  prod:
    mode: production
    variables:
      catalog: prod
    run_as:
      service_principal_name: etl-prod
    permissions:
      - level: CAN_MANAGE
        group_name: data-engineers
      - level: CAN_VIEW
        group_name: analysts
```

## Validate before you deploy

`databricks bundle validate -t <target>` catches problems before anything reaches a workspace:

- **Unknown fields**, usually typos (`serverles`). These are warnings, so treat warnings as errors in CI.
- **Variables without a value**: no default, no target override, no `--var` or `BUNDLE_VAR_<name>`.
- **References that do not resolve**, such as `${resources.pipelines.order_pipeline.id}` for a pipeline that does not exist.

The lab adds checks a deploy would also hit: duplicate task keys, `depends_on` naming a task that does not exist, cycles in the task graph, tasks with zero or two task types, and invalid permissions. Practise: `dab-fix`.

## Variables and substitutions

- `${var.name}` reads a variable. Values come from `--var`, `BUNDLE_VAR_name`, the target's `variables`, or the variable's `default`, in that order.
- `${bundle.target}`, `${bundle.name}` and `${workspace.current_user.userName}` are always available.
- `${resources.<type>.<key>.id}` references another resource in the bundle; the ID is resolved at deploy time.

## Targets and modes

A target is one deployment of the same definition. The two modes encode the usual difference between them:

| | `mode: development` | `mode: production` |
|---|---|---|
| Names | Prefixed `[dev <user>]`, so each engineer has their own copy | As written |
| Schedules and triggers | Paused | Active |
| Pipelines | Development mode | Production |
| Identity | The deploying user | Set `run_as` to a service principal |

Production deployments should come from CI, not laptops, and run as a **service principal** so they do not depend on, or act as, any person. Practise: `dab-targets`.

## Task graphs

Tasks run as soon as their dependencies succeed. Expressing the real graph (fan-out after ingest, fan-in before gold) shortens the critical path compared with a chain. Practise: `dab-dag`.

## Permissions

`permissions` at the top level or in a target are applied to every resource the bundle deploys. Levels for jobs are `CAN_VIEW`, `CAN_MANAGE_RUN` (start and cancel runs), `CAN_MANAGE` and `IS_OWNER`. Grant to groups, so people can join and leave without editing the bundle. Practise: `dab-permissions`.

## A CI/CD pipeline

```yaml
# .github/workflows/deploy.yml (sketch)
on:
  pull_request:          # validate and test every change
  push:
    branches: [main]     # deploy main to production
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t prod
      - run: databricks bundle deploy -t staging && databricks bundle run integration_tests -t staging
      - if: github.ref == 'refs/heads/main'
        run: databricks bundle deploy -t prod
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}          # OAuth for the deploying service principal
      DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}
```

- Validate on every pull request; deploy to a staging target and run tests; deploy production only from `main`.
- Authenticate CI as a service principal with OAuth, never a personal token.
- Unity Catalog grants for that service principal are part of the platform's code too (chapter 3).

## What the lab simplifies

The validator covers bundles, variables, workspace, targets and modes, `run_as`, permissions, jobs, pipelines and schemas, which is a subset of the real schema. It does not deploy anything, resolve `lookup` variables, build artifacts, or check every field of every resource type. See [chapter 5](05-about-the-simulators.md).
