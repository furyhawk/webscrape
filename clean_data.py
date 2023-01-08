import logging
import glob
from dataclasses import dataclass
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
    data: webscraping_lib.DataConfig = MISSING
    debug: bool = False
    project_name: str = ""
    outdir: str = ""


cs: ConfigStore = ConfigStore.instance()
cs.store(name="base_config", node=Config)

# webscraping_lib registers its configs
# in webscraping_lib/web webscraping_lib/data
webscraping_lib.register_configs()


def read_csv(filename: str) -> pd.DataFrame:
    """"""
    csv_files: list[str] = glob.glob(filename)
    logger.info(csv_files)
    df_list: list[pd.DataFrame] = [pd.read_csv(csv_file) for csv_file in csv_files]

    return pd.concat(df_list, axis=0, ignore_index=True)


@hydra.main(
    version_base=None,
    config_path="conf",
    config_name="config",
)
def main(cfg: DictConfig) -> None:
    logger.info(OmegaConf.to_yaml(cfg))
    df_dict = {}
    df = None
    for category in cfg.web.companies_by:
        df_dict[category.by]: pd.DataFrame = read_csv(
            f"*_{category.by}{cfg.web.output_filename}*.csv"
        )
    for category in df_dict:
        logger.info(df_dict[category])
        if df is not None:
            df = pd.merge(
                df,
                df_dict[category],
                on=["ticker", "company", "country", "price", "daily change"],
            )
        else:
            df = df_dict[category]
    logger.info(df)
    df.to_csv("clean.csv")


if __name__ == "__main__":
    main()
