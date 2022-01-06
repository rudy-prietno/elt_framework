import asyncio
from preparation import operation_etl_datamodel
from helpers.queries import query_pararelize_services
from access import credential
import time
import sys


start_time = time.time()  # to check time execute


if __name__ == '__main__':

    try:
        # running asycn prosess
        loop = asyncio.get_event_loop()
        S3bucket_folder = 'S3://path obeject'./{years}/{month}/{datetime_}/{cluster}/'.format(
                                                                    years ='2021',
                                                                    month ='12', 
                                                                    datetime_ ='bulk-2021-12-28-until-2021-12-28',
                                                                    cluster = '0'
                                                                )
        ops_etl_data_sds_monitoring = asyncio.ensure_future(
            operation_etl_datamodel(
                key_access_='aws key access',
                secret_access_='aws secret key',
                hostname='host database',
                dbname='database name',
                username='users',
                password='your password',
                S3bucket_folder=S3bucket_folder,
                SQL=query_pararelize_services.select_service_data_mart.format(date_start='date', date_end='date', cluster_mod= '6', cluster='0'),
                table_name='put here you table name',
                schema='put here you schema',
                primary_key='put here your primary key',
                mode_insert='put here your method inser upsert, append or overwrite',
                varchar_lengths= {
                  put here your custom varchar lengths
                },
                sortkey=[
                  put here your sortkey
                ]
            )
        )

        all_groups = asyncio.gather(ops_etl_data)
        results = loop.run_until_complete(all_groups)
        loop.close()

    except:
        print('Error ingestion in : \n', sys.exc_info())
    else:
        print('Success ingestion data')

    print("--- %s seconds ---" % (time.time() - start_time))
