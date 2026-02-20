import enum
import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class AgencyTypeEnum(str, enum.Enum):
    municipal_pd = "municipal_pd"
    sheriff = "sheriff"
    district_attorney = "district_attorney"
    campus = "campus"
    transit = "transit"
    state = "state"
    federal_task_force = "federal_task_force"
    coroner = "coroner"
    other = "other"


class FeedTypeEnum(str, enum.Enum):
    press_releases = "press_releases"
    daily_activity_log = "daily_activity_log"
    arrest_log = "arrest_log"
    incident_reports = "incident_reports"
    transparency_portal = "transparency_portal"
    crimemapping_embed = "crimemapping_embed"
    community_alerts = "community_alerts"
    rss_feed = "rss_feed"
    open_data_api = "open_data_api"
    pdf_library = "pdf_library"


# Feed schemas


class AgencyFeedResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    feed_id: uuid.UUID
    agency_id: str
    feed_type: FeedTypeEnum
    url: str
    format: str | None
    is_active: bool
    last_scraped: datetime | None
    last_successful: datetime | None
    notes: str | None


class FeedCreate(BaseModel):
    feed_type: FeedTypeEnum
    url: str
    format: str | None = None
    is_active: bool = True
    notes: str | None = None


class FeedUpdate(BaseModel):
    feed_type: FeedTypeEnum | None = None
    url: str | None = None
    format: str | None = None
    is_active: bool | None = None
    notes: str | None = None


# Agency schemas


class AgencyListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    agency_id: str
    canonical_name: str
    aliases: list[str] | None
    county: str | None
    region: str | None
    agency_type: AgencyTypeEnum
    homepage_url: str | None
    platform_type: str | None
    parser_id: str | None
    scrape_frequency: str
    has_activity_data: bool | None
    last_verified: datetime | None
    foia_contact: str | None
    notes: str | None
    created_at: datetime
    updated_at: datetime | None
    feed_count: int


class AgencyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    agency_id: str
    canonical_name: str
    aliases: list[str] | None
    county: str | None
    region: str | None
    agency_type: AgencyTypeEnum
    homepage_url: str | None
    platform_type: str | None
    parser_id: str | None
    scrape_frequency: str
    has_activity_data: bool | None
    last_verified: datetime | None
    foia_contact: str | None
    notes: str | None
    created_at: datetime
    updated_at: datetime | None
    feeds: list[AgencyFeedResponse]


class AgencyListResponse(BaseModel):
    items: list[AgencyListItem]
    total: int
    limit: int
    offset: int


class AgencyCreate(BaseModel):
    agency_id: str
    canonical_name: str
    aliases: list[str] | None = None
    county: str | None = None
    region: str | None = None
    agency_type: AgencyTypeEnum = AgencyTypeEnum.other
    homepage_url: str | None = None
    platform_type: str | None = None
    parser_id: str | None = None
    scrape_frequency: str = "daily"
    has_activity_data: bool | None = None
    foia_contact: str | None = None
    notes: str | None = None


class AgencyUpdate(BaseModel):
    canonical_name: str | None = None
    aliases: list[str] | None = None
    county: str | None = None
    region: str | None = None
    agency_type: AgencyTypeEnum | None = None
    homepage_url: str | None = None
    platform_type: str | None = None
    parser_id: str | None = None
    scrape_frequency: str | None = None
    has_activity_data: bool | None = None
    foia_contact: str | None = None
    notes: str | None = None


# Stats schema


class RegistryStatsResponse(BaseModel):
    total_agencies: int
    by_type: dict[str, int]
    by_platform: dict[str, int]
    by_region: dict[str, int]
    agencies_with_feeds: int
    total_feeds: int
    verified_agencies: int
