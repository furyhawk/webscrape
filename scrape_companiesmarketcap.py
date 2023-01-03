from typing import Any, Literal
import logging
from math import ceil
import datetime

from dataclasses import dataclass

import requests
from requests import Response
from bs4 import BeautifulSoup
import pandas as pd

from tqdm import tqdm

import webscraping_lib

from omegaconf import MISSING, OmegaConf, DictConfig
import hydra

from hydra.core.config_store import ConfigStore


logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class Config:
    web: webscraping_lib.CompaniesMarketCapConfig = MISSING
    debug: bool = False


cs: ConfigStore = ConfigStore.instance()
cs.store(name="base_config", node=Config)

# webscraping_lib registers its configs
# in webscraping_lib/web
webscraping_lib.register_configs()


def scrape_companiesmarketcap(
    num_stocks: int, start_num: int, companies_by, cfg: DictConfig
) -> None:
    # Initialise an empty DataFrame
    columns: list[str] = [
        "company",
        "ticker",
        f"{companies_by.by}",
        "price",
        "daily change",
        "country",
    ]
    df: pd.DataFrame = pd.DataFrame(columns=columns)
    # Get the number of pages to access based on the number of stocks that need to be processed. each page has 100 stocks
    start_page: int = int((lambda x: 1 if x < 1 else ceil(x / 100))(start_num))
    end_page: int = int(
        (lambda x: 1 if x < 1 else ceil(x / 100))(start_num + num_stocks - 1)
    )
    pbar: tqdm[int] = tqdm(range(start_page, end_page + 1))
    for page_number in pbar:
        # 01. Define the URL of the website
        URL: str = (
            str(cfg.web.companies_url)
            + str(companies_by.category)
            + str(cfg.web.page_param)
            + str(page_number)
            + "/"
        )

        # 02. Make a get request and print a message about whether it was successful or not
        while True:
            response: Response = requests.get(
                URL, headers={"user-agent": cfg.web.user_agent}
            )

            if response.ok:
                break

            logger.info(
                f"  Page {page_number:02d} - {'!The request was not accepted!'}"
            )

        message = (
            "The request was successfully."
            if response.ok
            else "The request was not successful."
        )
        pbar.set_description(f"Page {page_number:02d} - {message}")

        # 03. Extract the raw HTML and create a Beatiful Soup object
        html: bytes = response.content
        soup: BeautifulSoup = BeautifulSoup(html, cfg.web.parser)

        # 04. Retrieve data for all companies
        table = soup.find("tbody").find_all("tr")

        # 05. Retrieve data for each feauture individually
        companies, tickers, by_value, prices, changes, countries = (
            [],
            [],
            [],
            [],
            [],
            [],
        )

        for i in range(len(table)):
            companies.append(
                table[i].find("div", {"class": "company-name"}).text.strip()
            )
            tickers.append(table[i].find("div", {"class": "company-code"}).text)
            by_value.append(
                table[i]
                .find_all("td", {"class": "td-right"})[1]
                .text.replace(",", "")
                .strip()
            )
            prices.append(table[i].find_all("td", {"class": "td-right"})[2].text)
            changes.append(table[i].find_all("span")[1].text)
            countries.append(
                table[i].find_all("span", {"class": "responsive-hidden"})[0].text
            )

        # 06. Append to the existing DataFrame
        dfCurrent: pd.DataFrame = pd.DataFrame(
            {
                "company": companies,
                "ticker": tickers,
                f"{companies_by.by}": by_value,
                "price": prices,
                "daily change": changes,
                "country": countries,
            }
        )
        dfCurrent["company"] = dfCurrent["company"].str.strip("\r\n")
        df = pd.concat([df, dfCurrent])
        today: datetime = datetime.datetime.now()
        df.to_csv(
            f"{str(start_num + num_stocks - 1)}"
            f"_{companies_by.by}"
            f"{cfg.web.output_filename}"
            f"{today.strftime('%Y-%m-%d')}.csv",
            index=False,
        )


@hydra.main(
    version_base=None,
    config_path="conf",
    config_name="config",
)
def main(cfg: DictConfig) -> None:
    logger.info(OmegaConf.to_yaml(cfg))
    for category in cfg.web.companies_by:
        scrape_companiesmarketcap(
            num_stocks=cfg.web.max_companies,
            start_num=cfg.web.start_from,
            companies_by=category,
            cfg=cfg,
        )


if __name__ == "__main__":
    main()
