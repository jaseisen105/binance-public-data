#!/usr/bin/env python

import os
import gzip
import shutil
import sys

import boto3 as boto3
import logging
import pandas as pd
from logging.handlers import TimedRotatingFileHandler

from enums import *
from utility import download_file, get_all_symbols, convert_to_date_object, get_path

log = logging.getLogger(__name__)
os.chdir("/home/ubuntu/binance-public-data/python")


def download_monthly_trades(trading_type, symbols, num_symbols, years, months, start_date, end_date, folder, checksum):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    log.info("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        log.info("[{}/{}] - start download monthly {} trades ".format(current + 1, num_symbols, symbol))
        for year in years:
            for month in months:
                current_date = convert_to_date_object('{}-{}-01'.format(year, month))
                if current_date >= start_date and current_date <= end_date:
                    path = get_path(trading_type, "trades", "monthly", symbol)
                    file_name = "{}-trades-{}-{}.zip".format(symbol.upper(), year, '{:02d}'.format(month))
                    download_file(path, file_name, date_range, folder)

                    if checksum == 1:
                        checksum_path = get_path(trading_type, "trades", "monthly", symbol)
                        checksum_file_name = "{}-trades-{}-{}.zip.CHECKSUM".format(symbol.upper(), year, '{:02d}'.format(month))
                        download_file(checksum_path, checksum_file_name, date_range, folder)

        current += 1


def download_daily_trades(trading_type, symbols, num_symbols, dates, folder):
    current = 0
    date_range = None

    log.info("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        if "USDT" not in symbol:
            continue
        log.info("[{}/{}] - start download daily {} trades ".format(current + 1, num_symbols, symbol))
        for current_date in dates:
            try:
                path = get_path(trading_type, "trades", "daily", symbol)
                file_name = "{}-trades-{}.zip".format(symbol.upper(), current_date.date())
                download_file(path, file_name, date_range, folder)

                # shutil.unpack_archive(f"{path}{file_name}", path)
                # unzipped_file = file_name.replace(".zip", ".csv")
                # gzip_file = "binance-" + file_name.replace(".zip", ".gz")
                # with open(f"{path}{unzipped_file}", "rb") as f, gzip.open(f"{path}{gzip_file}", "wb") as out:
                #     out.writelines(f)

                s3 = boto3.client('s3')
                s3.upload_file(f"{path}{file_name}", "exchange-daily-trades", f"binance-{file_name}")
                log.info(f"uploaded {file_name} to daily trades bucket")

                # os.remove(f"{path}{gzip_file}")
                # os.remove(f"{path}{unzipped_file}")
                os.remove(f"{path}{file_name}")
            except:
                log.exception(f"failed to download {current} {symbol}")

        current += 1


def init_log(name, log_level=logging.INFO, use_file=True):
    try:
        os.makedirs('logs', exist_ok=True)
    except TypeError:
        pass

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(console_handler)

    if use_file:
        time_rotating_handler = TimedRotatingFileHandler(f"logs/{name}.log", when='midnight', backupCount=10)
        time_rotating_handler.suffix = "%Y-%m-%d"
        time_rotating_handler.setFormatter(formatter)
        logger.addHandler(time_rotating_handler)


def upload_binance_trade_files(days):
    symbols = get_all_symbols("t")
    base = datetime.today().date()
    date_list = [base - timedelta(days=x) for x in range(days)]
    download_daily_trades("spot", symbols, len(symbols), date_list, None)


if __name__ == "__main__":
    init_log("binance-trade-files")
    days = 1
    if len(sys.argv) > 1:
        days = int(sys.argv[1])
    upload_binance_trade_files(days)