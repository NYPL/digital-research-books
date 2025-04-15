import re
from logging import Logger
from typing import List, Set, Tuple, Optional, Any
from sqlalchemy.exc import DataError
from managers import DBManager


from model import Record
from logger import create_log

logger = create_log(__name__)


class RecordBlocker:
    """Performs blocking on Records to find a candidate pool of potentially
    related records.
    """
    
    # Maximum number of "hops" to follow when matching records through identifiers
    MAX_MATCH_DISTANCE = 4
    # Maximum number of records that can be in a block
    BLOCK_SIZE_LIMIT = 10000
    # Regular expression to identify identifiers that should be matched
    IDENTIFIERS_TO_MATCH = r"\|(?:isbn|issn|oclc|lccn|owi)$"
    
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
    
    def find_candidate_records(self, record: Record) -> List[Record]:
        """Find records that might be related to the input record.
        
        Raises:
            Exception: If the number of related records exceeds BLOCK_SIZE_LIMIT
        """
        matched_record_ids = self._find_all_matching_record_ids(record)
        
        # Include the original record's ID if it exists in the database
        if record.id:
            matched_record_ids.append(record.id)
        
        # Query the actual Record objects
        return self._get_records_by_ids(matched_record_ids)
    
    def _find_all_matching_record_ids(self, record: Record) -> List[str]:
        """Finds all record IDs that might be related to the input record.

        Uses an iterative process to find potential matches:
        1. Start with record's identifiers
        2. Find records matching one of the identifiers 
        3. For each match, check title similarity
        4. If similar, add their identifiers to check
        5. Repeat up to MAX_MATCH_DISTANCE times
        
        Raises:
            Exception: If the number of matched records exceeds BLOCK_SIZE_LIMIT
        """
        tokenized_record_title = self._tokenize_title(record.title)
        ids_to_check = {
            id for id in record.identifiers if re.search(self.IDENTIFIERS_TO_MATCH, id)
        }

        matched_record_ids = set()
        checked_ids = set()

        for match_distance in range(0, self.MAX_MATCH_DISTANCE):
            matched_records = self._get_matched_records(
                list(ids_to_check), matched_record_ids.copy()
            )

            if not matched_records:
                break

            checked_ids.update(ids_to_check)
            ids_to_check.clear()

            for (
                matched_record_title,
                matched_record_id,
                matched_record_identifiers,
                *_,
            ) in matched_records:
                if not matched_record_title:
                    logger.warning("Invalid title found in matched records")
                    continue

                tokenized_matched_record_title = self._tokenize_title(
                    matched_record_title
                )

                if match_distance > 0 and not self._titles_overlap(
                    tokenized_record_title, tokenized_matched_record_title
                ):
                    continue

                ids_to_check.update(
                    {
                        id
                        for id in matched_record_identifiers
                        if re.search(self.IDENTIFIERS_TO_MATCH, id)
                        and id not in checked_ids
                    }
                )
                matched_record_ids.add(matched_record_id)

        if len(matched_record_ids) > self.BLOCK_SIZE_LIMIT:
            raise Exception(
                f"Records matched is greater than {self.BLOCK_SIZE_LIMIT}"
            )

        return list(matched_record_ids)
    
    def _get_matched_records(
        self, identifiers: List[str], already_matched_record_ids: List[str]
    ) -> List[Tuple]:
        """Queries database for records matching the given identifiers.
        
        Processes identifiers in batches to avoid database query size limits.
        
        Returns:
            List of (title, id, identifiers, has_part) tuples for matching records
        """
        batch_size = 100
        matched_records = []

        for i in range(0, len(identifiers), batch_size):
            id_batch = self._format_identifiers(identifiers[i : i + batch_size])

            try:
                records = (
                    self.db_manager.session.query(
                        Record.title, Record.id, Record.identifiers, Record.has_part
                    )
                    .filter(~Record.id.in_(already_matched_record_ids))
                    .filter(Record.identifiers.overlap(id_batch))
                    .filter(Record.title.isnot(None))
                    .all()
                )

                matched_records.extend(records)
            except DataError:
                logger.exception("Unable to get matching records")

        return matched_records
    
    def _get_records_by_ids(self, record_ids: List[str]) -> List[Record]:
        return (
            self.db_manager.session.query(Record)
            .filter(Record.id.in_(record_ids))
            .all()
        )
    
    @staticmethod
    def _titles_overlap(
        tokenized_record_title: Set[str], tokenized_matched_record_title: Set[str]
    ) -> bool:
        """Determines if two titles are similar enough to be considered matching.
        
        Rules:
        1. Single word titles must be subset/superset
        2. Multi-word titles must share at least 2 words
        """
        # Single-word title handling
        if (
            len(tokenized_record_title) == 1
            and not tokenized_record_title <= tokenized_matched_record_title
        ):
            return False
        elif (
            len(tokenized_matched_record_title) == 1
            and not tokenized_record_title >= tokenized_matched_record_title
        ):
            return False
        # Multi-word title handling
        elif (
            len(tokenized_record_title) > 1 and len(tokenized_matched_record_title) > 1
        ) and len(tokenized_record_title & tokenized_matched_record_title) < 2:
            return False

        return True
    
    @staticmethod
    def _tokenize_title(title: str) -> Set[str]:
        """Converts a title string into a set of normalized "tokens" (words).
        
        1. Extracts words using regex
        2. Converts to lowercase
        3. Removes common stop words
        """
        title_tokens = re.findall(r"(\w+)", title.lower())

        return set(title_tokens) - set(["a", "an", "the", "of"])
    
    @staticmethod
    def _format_identifiers(identifiers: List[str]) -> str:
        """Formats identifiers for PostgreSQL array overlap query.
        
        Handles escaping of special characters and creates proper array syntax.
        
        Args:
            identifiers: List of identifiers to format
            
        Returns:
            Formatted string for Postgres array overlap query
        """
        formatted_ids = []

        for id in identifiers:
            formatted_id = f'"{id}"' if re.search(r"[{},]{1}", id) else id
            formatted_ids.append(formatted_id)

        return "{{{}}}".format(",".join(formatted_ids)) 