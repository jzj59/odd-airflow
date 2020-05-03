import requests
import os
import time
from datetime import datetime
from calendar import timegm
import pandas


class TDAClient:

    def __init__(self, client_id):
        # need to initialize with client_id found in developer account settings
        self.client_id = client_id
        self.access_token = None
        self.url = "https://api.tdameritrade.com/v1/"

    def get_access_token(self):
        # implement refresh token method for getting access token
        # will need to implement method for refreshing refresh token (90 day expiration)
        # reliant on a tokeninfo.txt containing the refresh token to exist in on-da-dip/tdaclient within the server

        cwd = os.getcwd()
        dir = os.path.dirname(cwd)
        refresh_token_file = open(dir + "/creds/tokeninfo.txt", "r")
        refresh_token = refresh_token_file.readline()
        refresh_token_file.close()

        endpoint = self.url + "oauth2/token"
        grant_type = "refresh_token"
        access_type = "offline"

        data = {
            "grant_type": grant_type,
            "access_type": access_type,
            "refresh_token": refresh_token,
            "client_id": self.client_id
        }

        result = requests.post(url=endpoint, data=data)

        if result.status_code == 200:
            result_body = result.json()
            self.access_token = result_body["access_token"]

            cwd = os.getcwd()
            dir = os.path.dirname(cwd)
            refresh_token_file = open(dir + "/creds/tokeninfo.txt", "wt")
            # need to update token file with latest refresh token
            refresh_token_file.write(result_body["refresh_token"])
            refresh_token_file.close()

        elif result.status_code == 401:
            print("Invalid credentials.")
        elif result.status_code == 403:
            print("User doesn't have access to this account and/or permissions.")
        elif result.status_code == 400:
            print("Validation unsuccessful.  Check that client id and refresh tokens are correct.")
        elif result.status_code == 500:
            print("Server error, try again later.")
        else:
            print("Unknown error.")

    def _request(self, url, authorized=True, method="GET", **kwargs):
        endpoint = self.url + "marketdata/" + url

        params = {
            "apikey": self.client_id
        }

        if kwargs:
            for key in kwargs:
                params[key] = kwargs[key]

        if authorized:
            self.get_access_token()
            headers = {
                "Authorization": "Bearer " + self.access_token
            }
        else:
            headers = None

        if method == "GET":
            result = requests.get(url=endpoint, headers=headers, params=params)

        return result

    def get_quote(self, symbol):
        endpoint = symbol + "/quotes"

        result = self._request(url=endpoint)
        return result

    def get_quote_history(self, symbol, startdate=None, enddate=None):
        # default is YTD
        if startdate is None:
            current_year = datetime.today().year
            startdate = str(current_year) + "-01-01"
        if enddate is None:
            enddate = str(datetime.today().strftime("%Y-%m-%d"))

        url = symbol + "/pricehistory"

        start_converted = timegm(time.strptime(startdate + "T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"))*1000
        end_converted = timegm(time.strptime(enddate + "T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"))*1000
        period_type = "year"
        frequency_type = "daily"
        frequency = 1

        result = self._request(
            url=url,
            method="GET",
            start_date=start_converted,
            end_date=end_converted,
            period_type=period_type, frequency_type=frequency_type, frequency=frequency
        )
        return result

    def get_option_chain(self, symbol, contract_type="ALL", include_quotes=False, strike_range="ALL", strategy="SINGLE", option_type="ALL", exp_month="ALL", strike_count=-1):
        url = "chains"

        result = self._request(
            url=url,
            method="GET",
            authorized=True,
            symbol=symbol,
            contractType=contract_type,
            includeQuotes=include_quotes,
            range=strike_range,
            strategy=strategy,
            optionType=option_type,
            expMonth=exp_month,
            strikeCount=strike_count
        )

        return result


class OptionChain:

    def __init__(self, symbol, strategy, contract_type):
        self.symbol = symbol
        self.strategy = strategy
        self.contract_type = contract_type
        self.chain = None

    def unpack(self):
        if self.chain:
            chain_data = self.chain
            first_value = chain_data[list(chain_data.keys())[0]]
            column_names = list(first_value[list(first_value.keys())[0]][0].keys())
            column_names.extend(['strike_price', 'expiration_date'])
            chain_df = pandas.DataFrame(columns=column_names)

            for exp in chain_data:
                exp_date = exp.split(":")[0]
                subchain = OptionSubChain(exp_date, chain_data[exp])
                subchain_df = subchain.unpack()
                subchain_df['expiration_date'] = exp_date
                chain_df = chain_df.append(subchain_df)

            return chain_df


class OptionSubChain:

    def __init__(self, expiration_date, expiration_dict):
        self.expiration_dict = expiration_dict
        self.expiration_date = expiration_date

    def unpack(self):
        exp_dict = self.expiration_dict
        column_names = list(exp_dict[list(exp_dict.keys())[0]][0].keys())
        column_names.append('strike_price')
        subchain_df = pandas.DataFrame(columns=column_names)

        for strike in exp_dict:
            contract = OptionContract(strike, exp_dict[strike])
            contract_df = contract.unpack()
            contract_df['strike_price'] = float(strike)
            subchain_df = subchain_df.append(contract_df)

        return subchain_df


class OptionContract:

    def __init__(self, strike_price, data):
        self.strike_price = strike_price
        self.data = data

    def unpack(self):
        column_names = list(self.data[0].keys())
        contract_df = pandas.DataFrame(columns=column_names)
        stage_df = pandas.DataFrame(self.data)
        contract_df = contract_df.append(stage_df)

        return contract_df




