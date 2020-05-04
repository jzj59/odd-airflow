import requests
import os
import time
from datetime import datetime
from calendar import timegm
import pandas
import boto3
import io
from utilities.exceptions import ApiError
from airflow.models import Variable


class TDAClient:
    """
    Class for accessing TDA API.

    ...

    Attributes
    ----------
    client_id : str
        client ID associated with your TDA developer app
    access_token: str
        temporary access token that's generated in order to send authenticated requests to TDA
    url : str
        url prefix for API endpoints and API version

    Methods
    -------
    get_access_token():
        Refresh access token. (Best way to use this is to catch an expired access token exception from their API and programatically refresh.)
    _request(url, authorized=True, method="GET", **kwargs):
        Internal method for making requests to API.  Handles creating headers, parsing additional arguments, making request, and handling error codes.
    get_quote(symbol):
        Fetch current ticker price for a ticker symbol.
    get_quote_history(symbol, ...):
        Get entire history for a ticker.  Can pass in date ranges.
    get_option_chain(symbol, ...)
        Get current option chain for a ticker.  See method documentation for additional parameters.
    """
    def __init__(self, client_id):
        # need to initialize with client_id found in developer account settings
        self.client_id = client_id
        self.access_token = None
        self.url = "https://api.tdameritrade.com/v1/"

    def get_access_token(self):
        """implement refresh token method for getting access token, reliant on an existing access token stored in a /creds/tokeninfo.txt file in the working directory (make sure this gets copied to prod)"""
        # will need to implement method for refreshing refresh token (90 day expiration)

        aws_access_key = Variable.get("aws_access_key_id")
        aws_secret_key = Variable.get("aws_secret_access_key")
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        bytes_buffer = io.BytesIO()
        s3_client.download_fileobj(Bucket="on-da-dip", Key="tokeninfo.txt", Fileobj=bytes_buffer)
        byte_value = bytes_buffer.getvalue()
        refresh_token = byte_value.decode()

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

            s3_client.upload_file(Filename=dir + "/creds/tokeninfo.txt", Bucket="on-da-dip", Key="tokeninfo.txt")

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
        """
        Internal generic method for handling requests to TDA endpoints.  This should never need to be called directly by an end user.
        Helps manage headers for authenticated requests, url construction, parameter construction, and also error handling.

        :param url: TDA endpoint to be concatenated with https://api.tdameritrade.com/v1/marketdata/
        :param authorized: Make an authenticated vs. unauthenticated request.  Setting this to False allows you to bypass access token step, but will typically return stale data (1 day old).
        :param method: Possible values are GET and POST right now. Default is GET.
        :param kwargs: can extend this with additional arguments for the respective endpoints.
        :return: Response from TDA.
        """
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

            if result.status_code == 200:
                return result
            else:
                raise ApiError(result.status_code)
        else:
            raise TypeError("Invalid method.  Please pass either GET or POST.")

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
        """
        Method for fetching option chain from TDA.  Returns their option chain object, which the end user will have to traverse to extract key datapoints.

        :param symbol: Ticke symbol.
        :param contract_type: Supports ALL, PUT, or CALL.  Default is ALL.  This param is case sensitive.
        :param include_quotes: Whether or not to include underlying quote data. Default is false.
        :param strike_range: Which strike prices to return. Supports ITM (In-the-money), NTM (Near-the-money), OTM (Out-of-the-money), SAK (Strikes Above Market), SBK (Strikes Below Market), SNK (Strikes Near Market), ALL (All Strikes). Default is ALL.
        :param strategy: Options strategy (i.e. SINGLE, ANALYTICAL, SPREAD, etc.).  Right now only supports SINGLE.
        :param option_type: Option type i.e. ALL, S (standard), NS (nonstandard).  Right now only supports ALL.
        :param exp_month: Expiration month to return, given in all caps and first three letters, i.e. JUN. Also supports ALL. Default is ALL.
        :param strike_count:  Number of strikes to return above and below at-the-money price.  Default is ignore parameter.
        :return: JSON blob representing option chain information
        """
        url = "chains"

        if strike_count == -1:
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
                expMonth=exp_month
            )
        else:
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
    """
    Class for representing an option chain object from TDA.  Consists of some high level attributes, in combination with a dictionary where each key is an expiration date representing an OptionSubChain class.

    Attributes
    ----------
    symbol: ticker symbol
    strategy: only support single
    contract_type: either PUT or CALL
    chain: an ExpDateMap dictionary from TDA. Every key is an expiration date where the values are another set of dictionaries. The second level dictionary has keys representing strike prices where the values are information about the specific contract.
    """
    def __init__(self, symbol, strategy, contract_type):
        self.symbol = symbol
        self.strategy = strategy
        self.contract_type = contract_type
        self.chain = None

    def unpack(self):
        """
        This is the primary function users will interact with.  If an ExpDateMap has been attached to the chain attribute, unpack() will traverse the nested dictionaries and convert to a denormalized dataframe.
        :return: Dataframe where data has been denormalized to represent all data at every expiration and strike combination.
        """
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
    """
    Internal class representing a series of contracts at multiple strikes prices, for a given expiration date.  A contract at a specific strike price can be represented by an OptionContract class.
    The primary purpose of this class is to make unpacking the overall option chain object a little easier for the OptionChain class.  There's almost no reason to every directly access this class.
    """

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
    """
    A set of data representing a contract at a given strike and expiration.  Example fields include price, bids, volatility, etc.
    The primary purpose of this class is to make unpacking the overall option chain object a little easier for the OptionChain class.  There's almost no reason to every directly access this class.
    """

    def __init__(self, strike_price, data):
        self.strike_price = strike_price
        self.data = data

    def unpack(self):
        column_names = list(self.data[0].keys())
        contract_df = pandas.DataFrame(columns=column_names)
        stage_df = pandas.DataFrame(self.data)
        contract_df = contract_df.append(stage_df)

        return contract_df




