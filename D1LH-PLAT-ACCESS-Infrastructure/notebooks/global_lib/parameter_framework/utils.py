"""
Module: utils
Author: Mykola Nepyivoda
Owner: Data Platform Team
Email: mykola.nepyivoda.ext@bayer.com

utils.py – Databricks‑native helper utilities for Parameter Framework
=======================================================================
This module contains helper functions that run *inside a Databricks cluster* to
read and update the Parameter Framework tables directly (no REST calls).
The API mirrors the behaviour of the FastAPI service so that notebooks and
workflows can interact with parameters locally while still preserving the same
history semantics while delegating **write‑time access control** to the shared
SQL UDF `generaldiscovery_globalconfig.pf_has_access`.

Key points
----------
* **Writes** are permitted only when the caller belongs to an ACL entity that
  the UDF resolves to *true* for the given parameter tuple `(context, content,
  application, name)`.
* **Reads are always allowed** – there is **no ACL check** when fetching a
  parameter.
* **History** is written to `parameters_history` and automatically cleaned so
  that only the last 12 months are retained (while always keeping at least the
  newest entry for every parameter).
* **Execution context** (Databricks user + job/notebook name) is captured and
  persisted in the history table, giving the same audit trail you get through
  the REST API.
* Helper functions are synchronous and return native Python objects, raising
  Python exceptions when something goes wrong.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
import json

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _now_utc_str() -> str:
    """
    Function: _now_utc_str
    Description: Returns the current UTC timestamp as a string in the format 'YYYY-MM-DD HH:MI:SS', suitable for inline SQL operations.
    Parameters: None
    Returns: str - UTC timestamp in string format.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _sanitize(value: str | None) -> str:
    """
    Function: _sanitize
    Description: Escapes single quotes in string literals to prevent SQL injection or syntax errors.
    Parameters:
        value (str | None): The input string to sanitize.
    Returns: str - The sanitized string with single quotes escaped.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """
    if value is None:
        return ""
    return value.replace("'", "''")


# ---------------------------------------------------------------------------
# Databricks context helpers (user + execution reason)
# ---------------------------------------------------------------------------


def get_current_user_email() -> str:
    """
    Function: get_current_user_email
    Description: Retrieves the email address of the currently logged-in Databricks user.
    Parameters: None
    Returns: str - The email address of the current user, or an empty string if unavailable.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """
    try:
        return spark.sql("SELECT current_user() AS user").first().user
    except Exception:
        return ""


def get_execution_reason() -> str:
    """
    Function: get_execution_reason
    Description: Returns a string identifying the Databricks job and notebook in the format 'jobName/notebookName'.
                 Attempts several methods to extract this information for compatibility with all Databricks runtimes
                 (including shared and standard clusters). Falls back to 'Unknown/Unknown' if not available.
    Parameters: None
    Returns: str - Concatenated job and notebook names, or 'Unknown/Unknown' if they cannot be determined.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """

    # Helper
    def _clean(s: str | None, default: str) -> str:
        if not s or not s.strip():
            return default
        return s.strip().split("/")[-1]

    # 1. Modern repl context (DBR 14+)
    try:
        from dbruntime.databricks_repl_context import get_context  # type: ignore

        repl = get_context()
        if repl is not None:
            d = getattr(repl, "__dict__", {})
            job_name = _clean(
                d.get("jobName") or d.get("runName") or d.get("job_name") or d.get("run_name"),
                "NoJob",
            )
            nb_name = _clean(
                d.get("notebookPath")
                or d.get("notebook_path")
                or d.get("notebookName")
                or d.get("notebook_name"),
                "NoNotebook",
            )
            if job_name != "NoJob" or nb_name != "NoNotebook":
                return f"{job_name}/{nb_name}"
    except Exception:
        pass  # fall‑through

    # 2. safeToJson (allow‑listed on shared‑mode)
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        try:
            import json  # local import to avoid cost if branch above fails

            data = json.loads(ctx.safeToJson())  # type: ignore[attr-defined]
            job_name = _clean(
                data.get("jobName") or data.get("runName"),
                "NoJob",
            )
            nb_name = _clean(
                data.get("notebookPath") or data.get("notebook_path"),
                "NoNotebook",
            )
            if job_name != "NoJob" or nb_name != "NoNotebook":
                return f"{job_name}/{nb_name}"
        except Exception:
            pass

        # 3. tags()
        tags = ctx.tags()
        for j_key in ["jobName", "runName", "job_name", "run_name"]:
            opt = tags.get(j_key)
            if opt.isDefined():
                job_name = _clean(opt.get(), "NoJob")
                break
        else:
            job_name = "NoJob"

        for n_key in [
            "notebookPath",
            "notebook_path",
            "notebookName",
            "notebook_name",
        ]:
            opt = tags.get(n_key)
            if opt.isDefined():
                nb_name = _clean(opt.get(), "NoNotebook")
                break
        else:
            nb_name = "NoNotebook"

        if job_name != "NoJob" or nb_name != "NoNotebook":
            return f"{job_name}/{nb_name}"

        # 4. Fallback scala methods
        try:
            nb_name = _clean(ctx.notebookPath().get(), "NoNotebook")  # type: ignore[attr-defined]
        except Exception:
            nb_name = "NoNotebook"
        return f"NoJob/{nb_name}"

    except Exception as e:
        print(f"Unable to derive execution_reason: {e}")
        return "Unknown/Unknown"


# ---------------------------------------------------------------------------
# ACL helper – uses the shared SQL UDF (write only)
# ---------------------------------------------------------------------------


def has_write_access(
    context: str,
    content: str,
    application: str,
    name: str,
) -> bool:
    """
    Function: has_write_access
    Description: Checks if the current Databricks user (or the service principal) has write access to a parameter.
                 Delegates access control logic to the SQL UDF 'pf_has_access', which evaluates wildcards and role membership.
    Parameters:
        context (str): The context value (e.g., 'Variable').
        content (str): The content area the parameter belongs to.
        application (str): The application the parameter belongs to.
        name (str): The name of the parameter.
    Returns: bool - True if the user has write access; otherwise, False.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """

    res = spark.sql(
        f"""
        SELECT generaldiscovery_globalconfig.pf_has_access(
                 '{_sanitize(context)}',
                 '{_sanitize(content)}',
                 '{_sanitize(application)}',
                 '{_sanitize(name)}') AS allowed
        """
    ).first()

    # In case the UDF is missing (e.g. sandbox), treat as no access
    return bool(res and res.allowed)


# ---------------------------------------------------------------------------
# Type (de)serialization (includes JSON support)
# ---------------------------------------------------------------------------


def serialize_value(value: Any, ptype: str) -> str:
    """
    Function: serialize_value
    Description: Serializes a Python value to a string representation according to the specified parameter type.
                 Supports types: STRING, INTEGER, DOUBLE, BOOLEAN, JSON.
    Parameters:
        value (Any): The value to serialize.
        ptype (str): The parameter type ('STRING', 'INTEGER', 'DOUBLE', 'BOOLEAN', 'JSON').
    Returns: str - The serialized value as a string.
    Raises: ValueError if the value cannot be serialized to the specified type.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """
    t = ptype.lower()
    if t == "string":
        return str(value)
    if t == "integer":
        return str(int(value))
    if t == "double":
        return str(float(value))
    if t == "boolean":
        lowered = str(value).strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return "true"
        if lowered in {"false", "0", "no", "off"}:
            return "false"
        raise ValueError(f"Unsupported boolean literal: {value}")
    if t == "json":
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(",", ":"))
        try:
            json.loads(value)
            return value  # already a valid JSON string
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e}")
    raise ValueError(f"Unsupported value '{value}' for type '{ptype}'.")


def deserialize_value(raw: str, ptype: str) -> Any:
    """
    Function: deserialize_value
    Description: Deserializes a raw string value from the parameters table into its corresponding Python type.
                 Supports types: STRING, INTEGER, DOUBLE, BOOLEAN, JSON.
    Parameters:
        raw (str): The raw string value from the database.
        ptype (str): The parameter type ('STRING', 'INTEGER', 'DOUBLE', 'BOOLEAN', 'JSON').
    Returns: Any - The deserialized Python object.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """
    t = ptype.lower()
    if t == "string":
        return raw
    if t == "integer":
        return int(raw)
    if t == "double":
        return float(raw)
    if t == "boolean":
        return raw.lower() in {"true", "1", "yes", "on"}
    if t == "json":
        return json.loads(raw)
    return raw


# ---------------------------------------------------------------------------
# Public functions – read & update
# ---------------------------------------------------------------------------


def read_parameter(
    *,
    content: str,
    application: str,
    name: str,
    context: str = "Variable",
    parameters_table: str = "generaldiscovery_globalconfig.parameters",
) -> Optional[Dict[str, Any]]:
    """
    Function: read_parameter
    Description: Reads and returns a parameter from the Delta table, with value automatically deserialized to a native Python type.
                 No ACL checks are performed; reading is always allowed.
    Parameters:
        content (str): The content area the parameter belongs to.
        application (str): The application the parameter belongs to.
        name (str): The name of the parameter.
        context (str, optional): The context value (default 'Variable').
        parameters_table (str, optional): The table to query (default 'generaldiscovery_globalconfig.parameters').
    Returns: dict | None - Dictionary with parameter fields and deserialized value, or None if not found.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """

    row = spark.sql(
        f"""
        SELECT context, content, application, name, type, value, description
          FROM {parameters_table}
         WHERE context     = '{_sanitize(context)}'
           AND content     = '{_sanitize(content)}'
           AND application = '{_sanitize(application)}'
           AND name        = '{_sanitize(name)}'
         LIMIT 1
        """
    ).collect()

    if not row:
        return None

    r = row[0]
    return {
        "context": r.context,
        "content": r.content,
        "application": r.application,
        "name": r.name,
        "type": r.type,
        "value": deserialize_value(r.value, r.type),
        "description": r.description,
    }


def update_parameter(
    *,
    content: str,
    application: str,
    name: str,
    new_value: Any,
    param_type: str,
    description: str = "",
    context: str = "Variable",
    parameters_table: str = "generaldiscovery_globalconfig.parameters",
    history_table: str = "generaldiscovery_globalconfig.parameters_history",
) -> Dict[str, Any]:
    """
    Function: update_parameter
    Description: Updates the value and metadata of an existing parameter in the Delta table, with full change history recorded.
                 Write access is enforced using the 'pf_has_access' SQL UDF. Also prunes parameter history older than 12 months,
                 except for the latest entry per parameter.
    Parameters:
        content (str): The content area the parameter belongs to.
        application (str): The application the parameter belongs to.
        name (str): The name of the parameter.
        new_value (Any): The new value to assign, must be compatible with param_type.
        param_type (str): The type of the parameter ('STRING', 'INTEGER', 'DOUBLE', 'BOOLEAN', 'JSON').
        description (str, optional): Description of the parameter (default empty).
        context (str, optional): The context value (default 'Variable').
        parameters_table (str, optional): The main parameters table name.
        history_table (str, optional): The parameter history table name.
    Returns: dict - Dictionary with the updated parameter state (deserialized).
    Raises: PermissionError if the user lacks write access. ValueError if the parameter does not exist or type mismatches.
    Author: Mykola Nepyivoda
    Date: 2025-05-20
    """

    if not has_write_access(context, content, application, name):
        raise PermissionError(f"No write access to {content}/{application}/{name}.")

    stored_value = serialize_value(new_value, param_type)

    # Ensure parameter exists & get current state
    current = spark.sql(
        f"""
        SELECT value, type
          FROM {parameters_table}
         WHERE context     = '{_sanitize(context)}'
           AND content     = '{_sanitize(content)}'
           AND application = '{_sanitize(application)}'
           AND name        = '{_sanitize(name)}'
         LIMIT 1
        """
    ).collect()
    if not current:
        raise ValueError("Parameter does not exist – use create workflow instead.")

    old_value, old_type = current[0].value, current[0].type
    if old_type != param_type:
        raise ValueError(f"Type mismatch: existing '{old_type}' vs new '{param_type}'.")

    ts = _now_utc_str()
    user_email = _sanitize(get_current_user_email())
    exec_reason = _sanitize(get_execution_reason())

    # Insert history row
    spark.sql(
        f"""
        INSERT INTO {history_table}
        (context, content, application, name, old_value, new_value, type, user, update_time, execution_reason)
        VALUES ('{_sanitize(context)}',
                '{_sanitize(content)}',
                '{_sanitize(application)}',
                '{_sanitize(name)}',
                '{_sanitize(old_value)}',
                '{_sanitize(stored_value)}',
                '{_sanitize(param_type)}',
                '{user_email}',
                '{ts}',
                '{exec_reason}')
        """
    )

    # Prune history
    spark.sql(
        f"""
        DELETE FROM {history_table} h
              WHERE h.update_time < date_sub(current_timestamp(), 365)
                AND h.update_time < (
                    SELECT max(h2.update_time)
                      FROM {history_table} h2
                     WHERE h2.context = h.context
                       AND h2.content = h.content
                       AND h2.application = h.application
                       AND h2.name = h.name)
        """
    )

    # Update live parameter
    spark.sql(
        f"""
        UPDATE {parameters_table}
           SET value       = '{_sanitize(stored_value)}',
               type        = '{_sanitize(param_type)}',
               description = '{_sanitize(description)}',
               last_update = current_timestamp()
         WHERE context     = '{_sanitize(context)}'
           AND content     = '{_sanitize(content)}'
           AND application = '{_sanitize(application)}'
           AND name        = '{_sanitize(name)}'
        """
    )

    # Return fresh state
    param = read_parameter(
        content=content,
        application=application,
        name=name,
        context=context,
        parameters_table=parameters_table,
    )
    assert param is not None  # can't be missing
    return param