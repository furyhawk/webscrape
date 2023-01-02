from typing import Any, Literal
import logging
import pandas as pd

import requests
from requests import Response
from bs4 import BeautifulSoup

logger: logging.Logger = logging.getLogger(__name__)


def get_url_page(url_link, user_agent, parser) -> BeautifulSoup | Literal[""]:
    """This accepts an URL as a parameter,
    accesses and loads the webpage into a variable
    retuns a document of the type BeautifulSoup"""

    # uses requests function to access and load the web page
    stock_page_response: Response = requests.get(
        url_link, headers={"user-agent": user_agent}
    )

    if not stock_page_response.ok:
        logger.info(f"Status code for {url_link}: {stock_page_response.status_code}")
        # raise Exception('Failed to fetch web page ' + url_link)
        return ""

    # If the status code is success , the page is sent through html parser and builds a parsed document.
    stock_page_doc: BeautifulSoup = BeautifulSoup(stock_page_response.text, parser)

    # Returns a beautifulSoup document.
    return stock_page_doc


def write_csv(dict_items, file_name: str) -> None:
    """
    Accepts list of python dictionary with stock details and write it to a csv file
    logger.infos success message upon completing the writing to the file
    """

    # open the file for writing
    with open(file_name, "w") as f:

        # Get headers(keys) of the first dictionary from the list. Convert to a list, join each element of the list
        # with ',' to form a string and write to the file.
        headers: list = list(dict_items[0].keys())
        f.write(",".join(headers) + "\n")

        # For each Dictionary item, create a list with values and write it to the file
        for dict_item in dict_items:
            values: list = []
            for header in headers:
                try:
                    values.append(str(dict_item.get(header, "")))
                except:
                    pass
            f.write(",".join(values) + "\n")

    logger.info(f"Writing to file '{file_name}' completed")


def verify_results(file_name: str) -> None:
    """
    This Function verifies the File Output.
    Accepts file name as the parameter and displays sample output and row count.
    """

    # Create the dataFrame with the csv file
    stocks_df: pd.DataFrame = pd.read_csv(file_name)

    # logger.info a record count of a single column
    logger.info("")
    logger.info("Checking Output written to the file")
    logger.info("---------------------------------------")
    logger.info(f"Number of records written to the file : {stocks_df.count()[1]}")
    logger.info("")
    # logger.info a sample output of first 4 rows in the file alson with its headers
    logger.info("Sample Output : ")
    logger.info(stocks_df.head(5))
