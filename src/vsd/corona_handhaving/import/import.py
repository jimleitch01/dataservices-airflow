import argparse
import os
import pandas as pd
from sqlalchemy import Integer

from common.db import get_engine
import logging

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", type=str, help="CSV file to process")
    args = parser.parse_args()
    df = pd.read_csv(args.input_csv, sep=";")
    df.index.name = "id"
    engine = get_engine()
    df.to_sql(
        "corona_handhaving_new",
        engine,
        dtype={"id": Integer(), "aantal": Integer(), "week_nummer": Integer()},
    )


if __name__ == "__main__":
    main()