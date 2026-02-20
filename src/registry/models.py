import enum
import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, String, Text, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class AgencyType(enum.Enum):
    municipal_pd = "municipal_pd"
    sheriff = "sheriff"
    district_attorney = "district_attorney"
    campus = "campus"
    transit = "transit"
    state = "state"
    federal_task_force = "federal_task_force"
    coroner = "coroner"
    other = "other"


class FeedType(enum.Enum):
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


class Agency(Base):
    __tablename__ = "agencies"

    agency_id: Mapped[str] = mapped_column(String, primary_key=True)
    canonical_name: Mapped[str] = mapped_column(String, nullable=False)
    aliases: Mapped[list[str] | None] = mapped_column(
        ARRAY(String), default=list
    )
    county: Mapped[str | None] = mapped_column(String)
    region: Mapped[str | None] = mapped_column(String)
    agency_type: Mapped[AgencyType] = mapped_column(
        default=AgencyType.other
    )
    homepage_url: Mapped[str | None] = mapped_column(String)
    platform_type: Mapped[str | None] = mapped_column(String)
    parser_id: Mapped[str | None] = mapped_column(String)
    scrape_frequency: Mapped[str] = mapped_column(String, default="daily")
    has_activity_data: Mapped[bool | None] = mapped_column()
    last_verified: Mapped[datetime | None] = mapped_column()
    foia_contact: Mapped[str | None] = mapped_column(String)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime | None] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )

    feeds: Mapped[list["AgencyFeed"]] = relationship(
        back_populates="agency", cascade="all, delete-orphan"
    )


class AgencyFeed(Base):
    __tablename__ = "agency_feeds"

    feed_id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    agency_id: Mapped[str] = mapped_column(
        ForeignKey("agencies.agency_id"), nullable=False
    )
    feed_type: Mapped[FeedType] = mapped_column(nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    format: Mapped[str | None] = mapped_column("format", String)
    is_active: Mapped[bool] = mapped_column(default=True)
    last_scraped: Mapped[datetime | None] = mapped_column()
    last_successful: Mapped[datetime | None] = mapped_column()
    notes: Mapped[str | None] = mapped_column(Text)

    agency: Mapped["Agency"] = relationship(back_populates="feeds")
