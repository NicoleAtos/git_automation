"""
Module: sql_utils
Author: Mykola Nepyivoda
Owner: Data Platform Team
Email: mykola.nepyivoda.ext@bayer.com
"""

CREATE OR REPLACE FUNCTION generaldiscovery_globalconfig_r.V_READPARAM_V1(
    cont      STRING,
    app       STRING,
    pname     STRING,
    outFormat STRING DEFAULT 'STRING',  -- 'INT', 'BOOLEAN', 'STRING', 'DOUBLE'
    context STRING DEFAULT 'Variable'
)
RETURNS STRING
COMMENT 'A Scalar Function for reading parameters from generaldiscovery_globalconfig.parameters table. Supports STRING, INT, BOOLEAN, DOUBLE types as output.'
RETURN
(
  WITH single_param AS (
    SELECT
      p.value AS raw_value
    FROM generaldiscovery_globalconfig_r.parameters_view p
    WHERE p.content     = cont
      AND p.application = app
      AND p.name        = pname
      AND p.context     = context
    LIMIT 1
  )
  SELECT 
    CASE
      WHEN outFormat = 'INT'
        THEN CAST(TRY_CAST(raw_value AS INT) AS STRING)
      WHEN outFormat = 'BOOLEAN'
        THEN CAST(TRY_CAST(raw_value AS BOOLEAN) AS STRING)
      WHEN outFormat = 'DOUBLE'
        THEN CAST(TRY_CAST(raw_value AS DOUBLE) AS STRING)
      ELSE
        raw_value
    END 
  FROM single_param
);


-- This function checks if a user has access to a specific parameter based on the provided context, content, application, and name.
-- It returns TRUE if there is a matching entry in the parameters_access table where the user is a member of the specified entity.
-- The function allows for wildcard ('*') and NULL values in the context, content, application, and name fields to provide flexible access control.

CREATE OR REPLACE FUNCTION generaldiscovery_globalconfig.pf_has_access(
        ctx STRING, cont STRING, app STRING, nm STRING)
RETURNS BOOLEAN
COMMENT 'Checks if a user has access to a specific parameter based on context, content, application, and name'
RETURN EXISTS (
  SELECT 1
    FROM generaldiscovery_globalconfig.parameters_access a
   WHERE (a.context     = ctx  OR a.context     = '*' OR a.context     IS NULL)
     AND (a.content     = cont OR a.content     = '*' OR a.content     IS NULL)
     AND (a.application = app  OR a.application = '*' OR a.application IS NULL)
     AND (a.name        = nm   OR a.name        = '*' OR a.name        IS NULL)
     AND is_member(a.entity));