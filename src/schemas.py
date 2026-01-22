from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class Node(BaseModel):
    id: int = Field(..., description="Unique identifier for the node")
    node_type: str = Field(..., description="Type of the node (e.g., officer, address)")
    display_name: Optional[str] = Field(None, description="Display name of the node")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Additional properties of the node")

class Edge(BaseModel):
    id: int = Field(..., description="Unique identifier for the edge")
    type: str = Field(..., description="Type of the relationship")
    source: int = Field(..., description="ID of the source node")
    target: int = Field(..., description="ID of the target node")

class NeighborsResponse(BaseModel):
    nodes: List[Node] = Field(..., description="List of neighbor nodes")
    edges: List[Edge] = Field(..., description="List of edges connecting the requested node and neighbors")

class NodeResponse(BaseModel):
    count: int = Field(..., description="Number of nodes found (0 or 1)")
    data: Optional[Node] = Field(None, description="Node data if found")

class NeighborsCountDetails(BaseModel):
    details: Dict[str, int] = Field(..., description="Breakdown of neighbor counts by node type")

class NeighborsCountResponse(BaseModel):
    count: int = Field(..., description="Total count of neighbors")
    details: Dict[str, int] = Field(..., description="Breakdown of neighbor counts by node type")

class ColumnInfo(BaseModel):
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column data type")
    nullable: bool = Field(..., description="Whether the column can be NULL")

class SchemaResponse(BaseModel):
    nodes: List[ColumnInfo] = Field(..., description="Schema definition for nodes table")
    edges: List[ColumnInfo] = Field(..., description="Schema definition for edges table")

class SearchResponse(BaseModel):
    count: int = Field(..., description="Number of results found")
    results: List[Dict[str, Any]] = Field(..., description="List of search results (nodes or edges)")


