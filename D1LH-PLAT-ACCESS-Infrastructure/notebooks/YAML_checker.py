# Databricks notebook source
from typing import Union, List, Optional, Literal, Set
from pydantic import BaseModel, Field, validator
from pathlib import Path
from datetime import datetime

import yaml

# COMMAND ----------

ALLOWED_ENVIRONMENTS = ["dev", "qas", "prd"]
ALLOWED_SYSTEMS = ["databricks"]
ALLOWED_PRIVILEGES = ["SELECT", "INSERT", "DELETE", "UPDATE"]
ALLOWED_OBJECT_TYPES = ["VIEW", "TABLE", "DATABASE", "SCHEMA"]
ALLOWED_PRINCIPAL_TYPES = ["user", "group", "service_principal"]

# COMMAND ----------

def validate_allowed_values(field_name: str, allowed_values: Set[str]):
    def validator_func(cls, v):
        values = [v] if isinstance(v, str) else v
        if not all(item in allowed_values for item in values):
            raise ValueError(
                f"{field_name} contains invalid values: {values}. Allowed: {allowed_values}"
            )
        return v
    return validator_func


class HeaderModel(BaseModel):
    created_at: datetime
    created_by: str
    system: Union[str, List[str]]
    environment: Union[str, List[str]]

    _validate_environment = validator("environment", allow_reuse=True)(validate_allowed_values("environment", ALLOWED_ENVIRONMENTS))
    _validate_system = validator("system", allow_reuse=True)(validate_allowed_values("system", ALLOWED_SYSTEMS))

class ObjectModel(BaseModel):
    type: Literal[*ALLOWED_OBJECT_TYPES]
    name: Union[str, List[str]]

class PrincipalModel(BaseModel):
    type: Literal[*ALLOWED_PRINCIPAL_TYPES]
    name: Union[str, List[str]]

class FullDocumentModel(BaseModel):
    id: int
    object: ObjectModel
    principal: PrincipalModel
    privilege: str
    expiry: Optional[datetime] = None

    @validator("privilege")
    def validate_privilege(cls, v):
        tokens = v.strip().upper().split()
        if tokens[0] == "REVOKE":
            if len(tokens) == 1:
                return v  # revoke-all
            elif len(tokens) == 2 and tokens[1] in ALLOWED_PRIVILEGES:
                return v  # revoke single privilege
            else:
                raise ValueError(f"Invalid privilege format: {v}")
        elif tokens[0] in ALLOWED_PRIVILEGES:
            return v  # normal grant
        else:
            raise ValueError(f"Invalid privilege format: {v}")

def validate_yaml_file(file_path: Path):
    with open(file_path) as f:
        docs = list(yaml.safe_load_all(f))

    for i, doc in enumerate(docs):
            if i == 0:
                validated = HeaderModel(**doc)
                print(f"✅ Header document validated successfully.")
            else:
                validated = FullDocumentModel(**doc)
                print(f"✅ Entry [{validated.id}] validated successfully.")

# COMMAND ----------

import sys

files_to_check = sys.argv[1:]

for path in files_to_check:
    print(f"Validating {path}")
    validate_yaml_file(path)
