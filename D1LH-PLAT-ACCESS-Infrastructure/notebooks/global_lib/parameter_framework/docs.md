# Parameter Framework – Backend Usage Guide (Databricks Edition)

> **Audience:** Databricks data‑engineers / analysts who need to *read* or *update* application parameters from notebooks, jobs or Workflows.
> **Scope:** How to work with the **Python helpers** in `utils.py`, the underlying Delta tables and the ACL/UDF machinery.

---

## 1. Why you need this

* Configuration and business switches live in **one consistent store** instead of scattered constants.
* Jobs can **self‑document** their changes – every update is historised with a timestamp, Databricks user and job/notebook name.
* Uniform **access‑control**: the same roles you use in notebooks are enforced for parameter writes.

---

## 2. The data model at a glance *(all tables live in `generaldiscovery_globalconfig`)*

| Table                    | Purpose                                                      | Key columns                                                                    |
| ------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| **`parameters`**         | Latest value of every parameter                              | `context, content, application, name, value, type, last_update`                |
| **`parameters_history`** | Full history of changes (12 months rolling, plus last entry) | `old_value, new_value, user, execution_reason, update_time`                    |
| **`parameters_access`**  | ACL matrix (role ‑> parameter scope)                         | `entity` (Databricks group), optional wildcards `*` / `NULL` in each dimension |

### Helper UDFs

| UDF                                                     | Return    | What for                                                                 |
| ------------------------------------------------------- | --------- | ------------------------------------------------------------------------ |
| **`pf_has_access(ctx, cont, app, nm)`**                 | `BOOLEAN` | Checks if *current* user may **write** the parameter (wildcards honored) |
| **`V_READPARAM_V1(cont, app, nm, outFormat, context)`** | `STRING`  | SQL‑side reader (no ACL) used mainly by BI tools                         |

---

## 3. `utils.py` – public API

| Function                    | Action                                       | ACL?                          | Raises                          |
| --------------------------- | -------------------------------------------- | ----------------------------- | ------------------------------- |
| **`read_parameter(...)`**   | Fetch one parameter & deserialize value.     | *No* (open read)              | `None` if not found             |
| **`update_parameter(...)`** | Overwrite existing parameter, write history. | **Yes** (via `pf_has_access`) | `PermissionError`, `ValueError` |

### Supported `param_type` values

`STRING` | `INTEGER` | `DOUBLE` | `BOOLEAN` | `JSON`  → JSON is kept in Delta as a compact string and parsed to `dict`/`list` on read.

---

## 4. How‑to:

### 4.1 Read a parameter inside a notebook

```python
from utils import read_parameter

snap = read_parameter(
    content="SC&L",
    application="Demand Planning",
    name="SNAP_DP",
)["value"]          # → e.g. "06.2025"

print(f"Using snapshot {snap}")
```

* No roles required.
* Value is already typed (string/int/float/bool/dict).

---

## 4.2 Update a parameter from a scheduled Workflow

```python
from utils import update_parameter

update_parameter(
    content="SC&L",
    application="Demand Planning",
    name="SNAP_DP",
    context="Variable",          # default, can be omitted
    new_value="07.2025",
    param_type="STRING",
    description="Turn of the month roll-over"
)
```

### What happens under the hood

1. **ACL check via UDF**
   A Spark SQL call to

   ```sql
   SELECT generaldiscovery_globalconfig.pf_has_access(
     '<context>', '<content>', '<application>', '<name>'
   )
   ```

   is executed as part of `has_write_access()`.

   * **In production/QA/dev**, all jobs run under the **service principal**
     `ad0ba414-8c82-43eb-97a0-25551931be2f`.
   * That SP is a member of the Databricks group
     `ELSA-DLDB-{env}-AR-DEVELOPER`, which is granted write rights in
     `generaldiscovery_globalconfig.parameters_access`.
   * **Result:** Once your job is deployed, the SP’s group membership satisfies the ACL, so updates succeed automatically.
   * **Testing or in adhoc notebooks**, the check uses your personal Databricks roles.

2. **History recording**

   * The old and new values (plus `user` and `execution_reason`) are inserted into
     `parameters_history`.
   * Automatically cleans out entries older than 12 months, while retaining the most recent entry for each parameter.

3. **Live table update**

   * The `parameters` table is updated in place, setting `value`, `type`, `description` and `last_update = current_timestamp()`.

4. **Return fresh state**

   * After write, the helper re-reads the parameter and returns a Python dict with the parsed value and metadata.

---


### 4.3 Validate write permissions *without* updating

```python
from utils import has_write_access  # re‑exported

if not has_write_access("Variable", "SC&L", "Demand Planning", "SNAP_DP"):
    raise RuntimeError("Current user cannot update SNAP_DP")
```

---

## 5. Best practices & pitfalls

| 👍 Do                                                                                                                                       | ⚠️ Don’t                                                                              |
| ------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| **Always call parameters by keyword**, e.g. `update_parameter(content=…, application=…)`. Reduces ordering mistakes in long argument lists. | Change the `type` of an existing parameter – raise a new one if the semantics differ. |
| **Batch logic**: read once at the beginning of a job and cache; parameters are small.                                                       | Query `read_parameter` thousands of times inside a tight loop.                        |
| Use `JSON` for structured lists/objects – you get a dict/list back automatically.                                                           | Store comma‑separated strings and parse them manually.                                |
| Let the **business owner**’s Databricks group be the ACL entity – no e‑mail based ACLs.                                                     | Grant `*/*/*/*` to everyone; keep scopes tight.                                       |

---

## 6. Troubleshooting checklist

| Symptom                                                                           | Likely cause / fix                                                                                                                |
| --------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `PermissionError: No write access…`                                               | Your user is not in a Databricks group that matches `parameters_access` wildcard rules. Ask the parameter owner to add the group. |
| `ValueError: Parameter does not exist`                                            | Use the front‑end or REST API to **create** the parameter first; backend utils only update existing ones.                         |
| JSON decode error on read                                                         | The value in Delta is not valid JSON (was edited manually). Correct it via UI or SQL.                                             |

---


### 📌 Quick reference snippet

```python
from utils import read_parameter, update_parameter

# Read once
cfg = read_parameter(content="SC&L", application="Inventory",
                     name="MVMT_TYP_SLOW")
slow_mvmts = cfg["value"]        # JSON list → ['201', '251', …]

# … processing …

# Conditional update
update_parameter(content="SC&L", application="Inventory",
                 name="MVMT_TYP_SLOW",
                 new_value=slow_mvmts + ["299"],
                 param_type="JSON",
                 description="Added new slow movement type 299")
```

Copy‑paste the block above into any Databricks notebook or task – everything else (ACL, history, execution\_reason) is handled automatically.