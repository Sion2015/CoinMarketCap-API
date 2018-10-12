import requests
import pandas as pd
import time
import datetime
import dateutil.parser


pd.set_option('display.max_columns', None)


class CoinMarketCap:

    PRODUCTION_BASE_URL = 'https://pro-api.coinmarketcap.com'
    SANDBOX_BASE_URL = 'https://sandbox-api.coinmarketcap.com'
    API_TYPE_MAP = {
        "standard": 30,
        "professional": 365
    }
    default_historical_data_length = 29

    def __init__(self, api_key, is_sandbox=True, api_type="standard", default_timeout=10):
        self.api_key = api_key
        self.version = "v1"
        self.default_timeout = default_timeout

        self.url = "{}/{}".format(self.SANDBOX_BASE_URL if is_sandbox else self.PRODUCTION_BASE_URL,
                                  self.version)
        self._session = None

        if api_type in self.API_TYPE_MAP.keys():
            # minus 1 make sure no error occurs
            self.default_historical_data_length = self.API_TYPE_MAP[api_type] - 1

        today_begin = self.convert_to_day_begin(datetime.date.today())
        start_date = self.convert_to_day_begin(
            datetime.date.today() - datetime.timedelta(days=self.default_historical_data_length))

        self.default_start_date = start_date
        self.default_end_date = today_begin

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.headers.update({
                "Content-Type": "application/json",
                "X-CMC_PRO_API_KEY": self.api_key
            })

        return self._session

    def __get_response(self, endpoint, retry=1, params=None):
        attempt = 0
        response = self.session.get(
            "{}{}".format(self.url, endpoint),
            params=params,
            timeout=self.default_timeout
        )

        while response.status_code is not 200:
            # todo: same content code block
            time.sleep(5)
            response = self.session.get(
                "{}{}".format(self.url, endpoint),
                params=params,
                timeout=self.default_timeout
            )

            attempt += 1
            if attempt > retry:
                break

        return self.__get_response_data(response)

    @staticmethod
    def __get_response_data(response):
        response_dict = response.json()
        if response.status_code == 200:
            return response_dict["data"]
        else:
            print(response_dict)

    def get_ticker_list(self):
        # todo: inactive coin
        data = self.__get_response("/cryptocurrency/map")
        return data

    def get_meta_data_from_id(self, ticker_id):
        data = self.__get_response("/cryptocurrency/info", params={"id": ticker_id})
        return data

    def get_market_quotes_from_id(self, ticker_id, time_start=None, time_end=None, convert_currency="USD"):
        """
        :param ticker_id: A string of number which represent the ticker
        :param time_start: int timestamp
        :param time_end: int timestamp
        :param convert_currency: Default: USD
        :return: data dict {"symbol", symbol string
        """

        _time_start = self.default_start_date if time_start is None else time_start
        _time_end = self.default_end_date if time_end is None else time_end

        if _time_start > _time_end:
            print("Time period out of range, check time input and default time point")

        data = self.__get_response("/cryptocurrency/quotes/historical",
                                   params={"id": ticker_id,
                                           "time_start": _time_start,
                                           "time_end": _time_end,
                                           # "time_end": 1534377600,
                                           "interval": "6h",
                                           "convert": convert_currency})
        return self.parse_quote_data(data)

    @staticmethod
    def parse_quote_data(data_dict):
        result = []
        symbol = data_dict["symbol"]
        quotes_list = data_dict["quotes"]
        for quotes in quotes_list:
            result_dict = dict()
            result_dict["timestamp"] = quotes["timestamp"]
            for key, value in quotes["quote"]["USD"].items():
                result_dict[key] = value
            result.append(result_dict)

        result_df = pd.DataFrame.from_dict(result)
        result_df["timestamp"] = result_df["timestamp"].apply(CoinMarketCap.convert_datetime_precision,
                                                              time_interval="h")
        return {"symbol": symbol,
                "result_df": result_df}

    def get_ohlcv_from_id(self, ticker_id, convert_currency="USD"):
        data = self.__get_response("/cryptocurrency/ohlcv/historical",
                                   params={"id": ticker_id,
                                           "time_start": self.default_start_date,
                                           "time_end": self.default_end_date,
                                           # "time_end": 1534377600,
                                           "interval": "1d",
                                           "convert": convert_currency})
        return self.parse_ohlcv_data(data)

    @staticmethod
    def parse_ohlcv_data(data_dict):
        result = []
        quotes_list = data_dict["quotes"]

        for quotes in quotes_list:
            result_dict = dict()
            result_dict["time_close"] = quotes["time_close"]
            for key, value in quotes["quote"]["USD"].items():
                result_dict[key] = value
            result.append(result_dict)

        result_df = pd.DataFrame.from_dict(result)
        return result_df

    @staticmethod
    def parse_list_to_string(ori_list):
        return ",".join(map(str, ori_list))

    @staticmethod
    def split_list(ori_list, length):
        return tuple(ori_list[i: i+length] for i in range(0, len(ori_list), length))

    @staticmethod
    def convert_datetime_precision(timestamp, time_interval):
        if isinstance(timestamp, str):
            timestamp = dateutil.parser.parse(timestamp)

        params = {"d": timestamp.strftime("%Y-%m-%d"),
                  "h": timestamp.strftime("%Y-%m-%d %H"),
                  "M": timestamp.strftime("%Y-%m-%d %H:%M"),
                  "s": timestamp.strftime("%Y-%m-%d %H:%M:%S")}
        try:
            return params[time_interval]
        except:
            raise ValueError('time_interval should be "d, h, M, s"')

    @staticmethod
    def convert_to_day_begin(date_time):
        return int(time.mktime(time.strptime(str(date_time), '%Y-%m-%d')))


def main():
    pass


if __name__ == "__main__":
    main()
