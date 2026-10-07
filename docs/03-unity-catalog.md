# 3. Unity Catalog access control

Unity Catalog governs data with SQL grants on a hierarchy of objects. Most access problems are one of three things: a missing `USE` privilege, a grant on the wrong object, or a grant that is far too broad. The **Grant** lab's Unity Catalog challenges train all three.

## The hierarchy

```
metastore
└── catalog            prod
    └── schema         prod.gold
        ├── table      prod.gold.daily_revenue
        ├── view       prod.gold.acme_orders
        ├── volume     prod.bronze.landing
        └── function   prod.silver.region_filter
```

Tables are addressed with three-level names: `catalog.schema.table`.

## What it takes to read a table

All three, each held directly, through a group, or inherited:

1. `USE CATALOG` on the catalog;
2. `USE SCHEMA` on the schema;
3. `SELECT` on the table.

`USE CATALOG` and `USE SCHEMA` reveal nothing by themselves; they are "required but not sufficient", as the documentation puts it. Writing needs `MODIFY`; creating tables needs `CREATE TABLE` on the schema; reading files in a volume needs `READ VOLUME`.

When access is denied, the error names the first missing privilege:

```
[INSUFFICIENT_PERMISSIONS] Insufficient privileges: User alice@stagedoor.io does not have USE SCHEMA on Schema 'prod.gold'.
```

## Inheritance

Privileges are inherited **downward**. `SELECT` granted on a schema applies to every current and future table in it; on a catalog, to everything in every schema.

```sql
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `analysts`;
```

Schema-level grants are how access survives new tables. Catalog-level `SELECT` is usually too broad. Practise: `uc-first-grant`.

## Principals

- **Users**, **groups** (which can contain users and other groups) and **service principals** (identities for jobs and tools).
- `account users` is a built-in group containing everyone.
- **Grant to groups**, not people: membership changes without touching grants, and access reviews read well.
- **Production writes belong to service principals.** People can read what the pipeline produced; only the pipeline identity changes it.

Practise: `uc-medallion-roles`.

## Owners

Every object has an owner, who holds all privileges on that object and can grant them. Owning a table does not grant access to its parents: an owner still needs `USE CATALOG` and `USE SCHEMA` to reach it. Owners of a catalog or schema can manage privileges on the objects inside. Transfer ownership to a group or service principal (`ALTER TABLE … OWNER TO`) so objects do not depend on one person.

## Views

A reader needs `SELECT` on the view (plus the `USE` privileges on its parents), and **not** on the tables underneath. The view reads its base tables with its **owner's** privileges. If the owner loses access, the view fails for everyone. Practise: `uc-partner-view`.

## Row filters and column masks

Row filters and column masks are SQL functions attached to a table. They apply to every query, from every tool, and they run as the **querying user**, so `is_account_group_member()` and `current_user()` see who is asking.

```sql
CREATE OR REPLACE FUNCTION prod.silver.region_filter(region STRING)
RETURN is_account_group_member('finance')
    OR (is_account_group_member('regional-eu') AND region = 'EU');
ALTER TABLE prod.silver.orders SET ROW FILTER prod.silver.region_filter ON (region);

CREATE OR REPLACE FUNCTION prod.silver.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('support') THEN email
            ELSE '***@' || split_part(email, '@', 2) END;
ALTER TABLE prod.silver.customers ALTER COLUMN email SET MASK prod.silver.mask_email;
```

Use them instead of a view per audience. Practise: `uc-row-filter`, `uc-column-mask`.

## Least privilege, as rules you can check

The lab encodes common policies as automated checks, the same way a platform team would in CI or an audit query:

- grants go to groups or service principals, never individual users;
- no `ALL PRIVILEGES`;
- no `SELECT` or `MODIFY` at catalog level;
- nothing beyond `USE CATALOG` for `account users`.

## Debugging access

1. Read the error: it names the missing privilege and the object.
2. `SHOW GRANTS ON CATALOG prod`, `SHOW GRANTS ON SCHEMA prod.gold`, `SHOW GRANTS ON TABLE …`: check each level of the path.
3. Check group membership: is the user in the group that holds the grant?
4. For a view, check its owner's access to the base tables.
5. Fix with a group grant at the right level, and revoke any broad "temporary" workaround.

Practise: `uc-access-incident`.

## What the lab simplifies

The lab models privilege model 1.0's inheritance, the `USE` requirements, ownership, nested groups, views and row filters and masks. It does not model metastore admins, `BROWSE`, tags and ABAC policies, external locations and credentials, Delta Sharing, or workspace bindings. See [chapter 5](05-about-the-simulators.md).
