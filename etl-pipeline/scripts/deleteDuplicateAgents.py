import os
import re
import logging

from model import Work, Edition
from managers import DBManager, SFRRecordManager
from Levenshtein import jaro_winkler
from logger import create_log
from sqlalchemy import or_, func


def main(dryRun=False):
    logger = create_log(__name__)

    logging.basicConfig(
        filename="duplicateAgents.log", encoding="utf-8", level=logging.INFO
    )

    """Deleting current duplicate authors/contributors with improved Levenshtein implementation"""

    dbManager = DBManager(
        user=os.environ.get("POSTGRES_USER", None),
        pswd=os.environ.get("POSTGRES_PSWD", None),
        host=os.environ.get("POSTGRES_HOST", None),
        port=os.environ.get("POSTGRES_PORT", None),
        db=os.environ.get("POSTGRES_NAME", None),
    )

    dbManager.create_session()

    countAuth = 0
    countWorkContrib = 0
    countEditContrib = 0
    batchSize = 1000

    for work in (
        dbManager.session.query(Work)
        .filter(
            or_(
                func.jsonb_array_length(Work.authors) > 1,
                func.jsonb_array_length(Work.contributors) > 1,
            )
        )
        .yield_per(batchSize)
    ):
        logging.info("_________Work Duplicate Authors/Contributors Deletion_________")

        if work.authors and len(work.authors) > 1:
            work.authors = deleteDuplicateAgents(
                work.id, work.authors, countAuth, "author"
            )
        if work.contributors and len(work.contributors) > 1:
            work.contributors = deleteDuplicateAgents(
                work.id, work.contributors, countWorkContrib, "work contributor"
            )
        dbManager.session.add(work)

    for edition in (
        dbManager.session.query(Edition)
        .filter(func.jsonb_array_length(Edition.contributors) > 1)
        .yield_per(batchSize)
    ):
        logging.info("_________Edition Duplicate Contributors Deletion_________")

        edition.contributors = deleteDuplicateAgents(
            edition.id, edition.contributors, countEditContrib, "edition contributor"
        )
        dbManager.session.add(edition)

    if dryRun == True:
        logging.info("________DRY RUN COMPLETED________")

        dbManager.rollback_changes()
        dbManager.session.close()
        dbManager.engine.dispose()
    else:
        logging.info("________DELETION COMPLETED________")
        dbManager.commit_changes()
        dbManager.close_connection()


def deleteDuplicateAgents(agentID, agent, count, type):
    """Deleting duplicate agents in works and editions"""

    originalAgent = agent.copy()

    for currIndex, currAgent in enumerate(agent):
        if currIndex == len(agent) - 1:
            break

        cleanCurrAgent = re.sub(r"[.,:\(\)\-0-9]+", "", currAgent["name"].lower())

        for nextAgent in agent[currIndex + 1 :]:
            cleanNextAgent = re.sub(r"[.,:\(\)\-0-9]+", "", nextAgent["name"].lower())

            if jaro_winkler(cleanCurrAgent, cleanNextAgent) > 0.74:
                logging.info(cleanCurrAgent)
                logging.info(cleanNextAgent)
                logging.info(
                    f"Match Score: {jaro_winkler(cleanCurrAgent, cleanNextAgent)}"
                )

                SFRRecordManager.mergeAgents(currAgent, nextAgent)
                agent.remove(nextAgent)

                count += 1

    if count >= 1:
        logging.info(f"ID: {agentID}")
        logging.info("___OLD AGENT___")
        logging.info(originalAgent)
        logging.info("___NEW AGENT___")
        logging.info(agent)

        if type == "author":
            logging.info(f"Deleted {count} duplicate work author(s)")

        elif type == "work contributor":
            logging.info(f"Deleted {count} duplicate work contributor(s)")

        elif type == "edition contributor":
            logging.info(f"Deleted {count} duplicate edition contributor(s)")

        else:
            logging.info(agent)
            raise Exception

    return agent
