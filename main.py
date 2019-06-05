import logging

# external modules
import click
import pandas as pd
from google.cloud import bigquery

# local modules
from train.train import getModel
from predict.predict import getPrediction

# configuration file
from config import AIRLINES

@click.command()
@click.argument('bq_dataset')
@click.argument('table_name')
@click.argument('country')
@click.option('-l', '--debug', default='INFO', type=click.Choice(['CRITICAL','ERROR','WARNING','INFO','DEBUG']), help='Sets the logging level.')
def main(bq_dataset, table_name, country, debug):
    # logging setup

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.getLevelName(debug), datefmt='%Y-%m-%d %H:%M:%S')

    logging.info('Program initiated')

    # training

    xgbmodel, xtrain_columns = getModel(country)
    df = pd.DataFrame()

    # inference

    for competitor in AIRLINES[country]:

        logging.info("Predicting load factors for {}".format(competitor))


        try:
            result = getPrediction(xgbmodel, xtrain_columns, competitor, country)
            df     = pd.concat([df, result])
        except Exception as err:
            logging.warning(err)

    if not df.empty:

        client      = bigquery.Client()
        dataset_ref = client.dataset(bq_dataset)
        table_ref   = dataset_ref.table(table_name)

        df.reset_index(drop=True, inplace=True)

        logging.info("Uploading predictions to BigQuery")

        client.load_table_from_dataframe(df, table_ref).result()

    else:

        logging.warning("Prediction dataframe is empty")

if __name__ == '__main__':
    main()
