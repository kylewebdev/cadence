import logging

from src.parsers.base import BaseParser
from src.parsers.platform_arcgis import ArcGISParser
from src.parsers.platform_citizenrims import CitizenRimsParser
from src.parsers.platform_civicplus import CivicPlusParser
from src.parsers.platform_crimemapping import CrimeMappingParser
from src.parsers.platform_nixle import NixleParser
from src.parsers.platform_pdf import PDFParser
from src.parsers.platform_rss import RSSParser
from src.parsers.platform_socrata import SocrataParser

logger = logging.getLogger(__name__)


class ParserNotImplementedError(Exception):
    """Raised when get_parser() is called with an unknown parser_id."""


PARSER_REGISTRY: dict[str, type[BaseParser]] = {
    "rss":          RSSParser,
    "civicplus":    CivicPlusParser,
    "citizenrims":  CitizenRimsParser,
    "crimemapping": CrimeMappingParser,
    "nixle":        NixleParser,
    "rave":         NixleParser,   # Rave Mobile Safety — same parser, handles redirect
    "socrata":      SocrataParser,
    "arcgis":       ArcGISParser,
    "pdf":          PDFParser,
}


def get_parser(parser_id: str, agency_id: str, **kwargs) -> BaseParser:
    """
    Instantiate a parser by registry key.

    Args:
        parser_id: PARSER_REGISTRY key (typically agency.parser_id or agency.platform_type).
        agency_id: Always passed as first constructor arg.
        **kwargs:  Extra constructor args (e.g. crimemapping_id=42 for CrimeMappingParser).

    Raises:
        ParserNotImplementedError: logs and raises if parser_id not in registry.
    """
    cls = PARSER_REGISTRY.get(parser_id)
    if cls is None:
        logger.error("No parser for parser_id=%r (agency=%s)", parser_id, agency_id)
        raise ParserNotImplementedError(f"No parser registered for {parser_id!r}")
    return cls(agency_id, **kwargs)


__all__ = [
    "ArcGISParser",
    "CitizenRimsParser",
    "CivicPlusParser",
    "CrimeMappingParser",
    "NixleParser",
    "PDFParser",
    "RSSParser",
    "SocrataParser",
    "ParserNotImplementedError",
    "PARSER_REGISTRY",
    "get_parser",
]
