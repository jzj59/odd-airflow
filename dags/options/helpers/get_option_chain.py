import utilities.tda as tda
import boto3
from datetime import datetime
from airflow.models import Variable


def fetch_and_land_option_data(symbol, client_id):
    """
    Initialize a TDAClient class using JJ's TDA developer account.
    -> fetch option chain object from TDA for a given symbol.
    -> traverse {}ExpDateMap keys in TDA, i.e. callExpDateMap (contains a hierarchy of all calls at a given expiration and strike price, in that order)
    -> turn lowest level dictionary in a ExpDateMap (a contract for a given symbol, expiration, and strike) into a dataframe
    -> bind all the data together
    -> write data to s3
    :param symbol:
    :param client_id:
    :return:
    """
    client = tda.TDAClient(client_id)
    client.get_access_token()

    chain = client.get_option_chain(symbol=symbol)

    if chain.status_code == 200:
        chain_results = chain.json()
        uso_df_final = None

        for contract_type in ['CALL', 'PUT']:
            uso_chain = tda.OptionChain(symbol, strategy="SINGLE", contract_type=contract_type)
            uso_chain.chain = chain_results[contract_type.lower() + "ExpDateMap"]
            uso_df = uso_chain.unpack()

            if uso_df_final is not None:
                uso_df_final = uso_df_final.append(uso_df)
            else:
                uso_df_final = uso_df
            print(contract_type + " chain built.")

        print(uso_df_final.head())

        filename = "results.csv"
        uso_df_final.to_csv(filename)

        aws_access_key = Variable.get("aws_access_key_id")
        aws_secret_key = Variable.get("aws_secret_access_key")
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        response = s3_client.upload_file(Filename=filename, Bucket="on-da-dip", Key="option_chains/" + symbol + "/" + datetime.date(datetime.now()).strftime("%Y-%m-%d") + "_" + filename)
    else:
        response = chain.status_code + " error"

    return response
