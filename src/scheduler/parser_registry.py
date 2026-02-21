from collections.abc import Callable

from src.parsers.base import BaseParser
from src.parsers.platform_arcgis import ArcGISParser
from src.parsers.platform_civicplus import CivicPlusParser
from src.parsers.platform_crimemapping import CrimeMappingParser
from src.parsers.platform_nixle import NixleParser
from src.parsers.platform_pdf import PDFParser
from src.parsers.platform_rss import RSSParser
from src.parsers.platform_socrata import SocrataParser
from src.registry.models import Agency

PLATFORM_PARSERS: dict[str, Callable[["Agency"], BaseParser]] = {
    "rss": lambda a: RSSParser(a.agency_id),
    "crimemapping": lambda a: CrimeMappingParser(
        a.agency_id, a.crimemapping_agency_id or 0
    ),
    "civicplus": lambda a: CivicPlusParser(a.agency_id),
    "nixle": lambda a: NixleParser(a.agency_id),
    "socrata": lambda a: SocrataParser(a.agency_id),
    "arcgis": lambda a: ArcGISParser(a.agency_id),
    "pdf": lambda a: PDFParser(a.agency_id),
}


def get_parser(agency: Agency) -> BaseParser | None:
    factory = PLATFORM_PARSERS.get(agency.platform_type or "")
    return factory(agency) if factory else None
