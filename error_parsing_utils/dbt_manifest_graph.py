from typing import Dict, List, Optional
from enum import Enum
from pydantic import BaseModel, validator


# data struct for dbt_resource_type field
class DbtResourceType(str, Enum):
    model = "model"
    analysis = "analysis"
    test = "test"
    operation = "operation"
    seed = "seed"
    source = "source"
    snapshot = "snapshot"


# data struct for dbt_materialization_type field
class DbtMaterializationType(str, Enum):
    table = "table"
    view = "view"
    incremental = "incremental"
    ephemeral = "ephemeral"
    seed = "seed"
    snapshot = "snapshot"
    test = "test"


# create base class for node dependencies
class NodeDeps(BaseModel):
    nodes: List[str]


# create base class for node config
class NodeConfig(BaseModel):
    materialized: Optional[DbtMaterializationType]


# create base class for Node model
class Node(BaseModel):
    unique_id: str
    path: str
    resource_type: DbtResourceType
    description: str
    depends_on: Optional[NodeDeps]
    config: NodeConfig


# create base class for Manifest, along with validator decoration
class Manifest(BaseModel):
    nodes: Dict["str", Node]
    sources: Dict["str", Node]

    @validator("nodes", "sources")
    def filter(cls, val):
        return {
            k: v
            for k, v in val.items()
            if v.resource_type.value in ("test", "snapshot", "model")
        }
