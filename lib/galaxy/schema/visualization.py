from datetime import datetime
from typing import (
    List,
    Optional,
)

from pydantic import (
    Extra,
    Field,
)
from typing_extensions import (
    Literal,
)

from galaxy.schema.fields import (
    DecodedDatabaseIdField,
)
from galaxy.schema.schema import (
    CreateTimeField,
    TagCollection,
    UpdateTimeField,
    Model,
)


VisualizationSortByEnum = Literal["update_time", "title", "username"]


class VisualizationIndexQueryPayload(Model):
    deleted: bool = False
    show_published: Optional[bool] = None
    show_shared: Optional[bool] = None
    user_id: Optional[DecodedDatabaseIdField] = None
    sort_by: VisualizationSortByEnum = Field("update_time", title="Sort By", description="Sort pages by this attribute.")
    sort_desc: Optional[bool] = Field(default=True, title="Sort descending", description="Sort in descending order.")
    search: Optional[str] = Field(default=None, title="Filter text", description="Freetext to search.")
    limit: Optional[int] = Field(default=100, lt=1000, title="Limit", description="Maximum number of pages to return.")
    offset: Optional[int] = Field(default=0, title="Offset", description="Number of pages to skip.")


class VisualizationSummary(Model):
    id: DecodedDatabaseIdField = Field(
        ...,  # Required
        title="ID",
        description="Encoded ID of the Visualization.",
    )
    title: str = Field(
        title="Title",
        description="The name of the visualization.",
    )
    type: str = Field(
        ...,  # Required
        title="Type",
        description="The type of the visualization.",
    )
    dbkey: str = Field(
        default=None,
        title="DbKey",
        description="The database key of the visualization.",
    )


class VisualizationDetails(VisualizationSummary):
    create_time: Optional[datetime] = CreateTimeField
    update_time: Optional[datetime] = UpdateTimeField
    tags: Optional[TagCollection] = Field(
        ...,  # Required
        title="Tags",
        description="A list of tags to add to this item.",
    )

    class Config:
        extra = Extra.allow  # Allow any other extra fields


class VisualizationDetailsList(Model):
    __root__: List[VisualizationDetails] = Field(
        default=[],
        title="List with detailed information of Visualizations.",
    )
