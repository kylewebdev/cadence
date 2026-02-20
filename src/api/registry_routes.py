from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import distinct, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.api.deps import get_db
from src.api.schemas import (
    AgencyCreate,
    AgencyFeedResponse,
    AgencyListItem,
    AgencyListResponse,
    AgencyResponse,
    AgencyTypeEnum,
    AgencyUpdate,
    FeedCreate,
    FeedUpdate,
    RegistryStatsResponse,
)
from src.registry.models import Agency, AgencyFeed

router = APIRouter(prefix="/api")

DB = Annotated[AsyncSession, Depends(get_db)]


@router.get("/agencies", response_model=AgencyListResponse)
async def list_agencies(
    db: DB,
    limit: int = 50,
    offset: int = 0,
    county: str | None = None,
    region: str | None = None,
    agency_type: AgencyTypeEnum | None = None,
    platform_type: str | None = None,
    has_activity_data: bool | None = None,
):
    filters = []
    if county is not None:
        filters.append(Agency.county == county)
    if region is not None:
        filters.append(Agency.region == region)
    if agency_type is not None:
        filters.append(Agency.agency_type == agency_type.value)
    if platform_type is not None:
        filters.append(Agency.platform_type == platform_type)
    if has_activity_data is not None:
        filters.append(Agency.has_activity_data == has_activity_data)

    total_result = await db.execute(
        select(func.count()).select_from(Agency).where(*filters)
    )
    total = total_result.scalar_one()

    feed_count_subq = (
        select(func.count(AgencyFeed.feed_id))
        .where(AgencyFeed.agency_id == Agency.agency_id)
        .correlate(Agency)
        .scalar_subquery()
    )

    rows = await db.execute(
        select(Agency, feed_count_subq.label("feed_count"))
        .where(*filters)
        .order_by(Agency.canonical_name)
        .limit(limit)
        .offset(offset)
    )

    items = []
    for agency, feed_count in rows:
        data = {
            **{c.key: getattr(agency, c.key) for c in Agency.__table__.columns},
            "agency_type": agency.agency_type.value if agency.agency_type else None,
            "feed_count": feed_count,
        }
        items.append(AgencyListItem.model_validate(data))

    return AgencyListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/agencies/{agency_id}", response_model=AgencyResponse)
async def get_agency(agency_id: str, db: DB):
    result = await db.execute(
        select(Agency)
        .where(Agency.agency_id == agency_id)
        .options(selectinload(Agency.feeds))
    )
    agency = result.scalar_one_or_none()
    if agency is None:
        raise HTTPException(status_code=404, detail="Agency not found")
    return agency


@router.post("/agencies", response_model=AgencyResponse, status_code=201)
async def create_agency(body: AgencyCreate, db: DB):
    agency = Agency(**body.model_dump(exclude_unset=True))
    db.add(agency)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Agency ID already exists")
    await db.refresh(agency)
    result = await db.execute(
        select(Agency)
        .where(Agency.agency_id == agency.agency_id)
        .options(selectinload(Agency.feeds))
    )
    return result.scalar_one()


@router.patch("/agencies/{agency_id}", response_model=AgencyResponse)
async def update_agency(agency_id: str, body: AgencyUpdate, db: DB):
    result = await db.execute(
        select(Agency)
        .where(Agency.agency_id == agency_id)
        .options(selectinload(Agency.feeds))
    )
    agency = result.scalar_one_or_none()
    if agency is None:
        raise HTTPException(status_code=404, detail="Agency not found")
    for k, v in body.model_dump(exclude_unset=True).items():
        setattr(agency, k, v)
    await db.commit()
    await db.refresh(agency)
    result = await db.execute(
        select(Agency)
        .where(Agency.agency_id == agency_id)
        .options(selectinload(Agency.feeds))
    )
    return result.scalar_one()


@router.get("/agencies/{agency_id}/feeds", response_model=list[AgencyFeedResponse])
async def list_feeds(agency_id: str, db: DB):
    agency_result = await db.execute(
        select(Agency).where(Agency.agency_id == agency_id)
    )
    if agency_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Agency not found")

    result = await db.execute(
        select(AgencyFeed).where(AgencyFeed.agency_id == agency_id)
    )
    return result.scalars().all()


@router.post(
    "/agencies/{agency_id}/feeds", response_model=AgencyFeedResponse, status_code=201
)
async def create_feed(agency_id: str, body: FeedCreate, db: DB):
    agency_result = await db.execute(
        select(Agency).where(Agency.agency_id == agency_id)
    )
    if agency_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Agency not found")

    feed = AgencyFeed(agency_id=agency_id, **body.model_dump())
    db.add(feed)
    await db.commit()
    await db.refresh(feed)
    return feed


@router.patch(
    "/agencies/{agency_id}/feeds/{feed_id}", response_model=AgencyFeedResponse
)
async def update_feed(agency_id: str, feed_id: str, body: FeedUpdate, db: DB):
    result = await db.execute(
        select(AgencyFeed).where(
            AgencyFeed.feed_id == feed_id,
            AgencyFeed.agency_id == agency_id,
        )
    )
    feed = result.scalar_one_or_none()
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    for k, v in body.model_dump(exclude_unset=True).items():
        setattr(feed, k, v)
    await db.commit()
    await db.refresh(feed)
    return feed


@router.delete("/agencies/{agency_id}/feeds/{feed_id}", status_code=204)
async def delete_feed(agency_id: str, feed_id: str, db: DB):
    result = await db.execute(
        select(AgencyFeed).where(
            AgencyFeed.feed_id == feed_id,
            AgencyFeed.agency_id == agency_id,
        )
    )
    feed = result.scalar_one_or_none()
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    await db.delete(feed)
    await db.commit()
    return Response(status_code=204)


@router.get("/registry/stats", response_model=RegistryStatsResponse)
async def registry_stats(db: DB):
    total = (
        await db.execute(select(func.count()).select_from(Agency))
    ).scalar_one()

    by_type_rows = (
        await db.execute(
            select(Agency.agency_type, func.count()).group_by(Agency.agency_type)
        )
    ).all()
    by_type = {(r[0].value if r[0] else "unknown"): r[1] for r in by_type_rows}

    by_platform_rows = (
        await db.execute(
            select(Agency.platform_type, func.count()).group_by(Agency.platform_type)
        )
    ).all()
    by_platform = {(r[0] or "unknown"): r[1] for r in by_platform_rows}

    by_region_rows = (
        await db.execute(
            select(Agency.region, func.count()).group_by(Agency.region)
        )
    ).all()
    by_region = {(r[0] or "unknown"): r[1] for r in by_region_rows}

    agencies_with_feeds = (
        await db.execute(
            select(func.count(distinct(AgencyFeed.agency_id)))
        )
    ).scalar_one()

    total_feeds = (
        await db.execute(select(func.count()).select_from(AgencyFeed))
    ).scalar_one()

    verified_agencies = (
        await db.execute(
            select(func.count()).select_from(Agency).where(Agency.last_verified.isnot(None))
        )
    ).scalar_one()

    return RegistryStatsResponse(
        total_agencies=total,
        by_type=by_type,
        by_platform=by_platform,
        by_region=by_region,
        agencies_with_feeds=agencies_with_feeds,
        total_feeds=total_feeds,
        verified_agencies=verified_agencies,
    )
