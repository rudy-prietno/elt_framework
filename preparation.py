from awswrangler.redshift import unload_to_files
from helpers.spartanETL import SpartanBase, SpartanTransaction
from typing import Optional, Dict, List
import asyncio
import sys


async def etl_datamodel(
    key_access_:str,
    secret_access_:str,
    hostname:str,
    dbname:str,
    username:str,
    password:str,
    S3bucket_folder:str,
    SQL:str,
    table_name:str,
    schema:str,
    primary_key: Optional[str] = None,
    mode_insert: Optional[str] = None,
    varchar_lengths:Optional[Dict[str, int]] = None,
    sortkey: Optional[List[str]] = None
    ):
    try:

        s3_exec = SpartanBase (
                key_access=key_access_,
                secret_access=secret_access_
            )

        redshift_exec = SpartanTransaction(
            db_host=hostname,
            db_dbname=dbname,
            db_user=username,
            db_pass=password
        )

        connections = redshift_exec.connection_redshift_()
        unloader = redshift_exec.selectUnload_redshift_(
            connection_reshift= connections,
            connection_s3= s3_exec.call_sessionS3(),
            sql_statement= SQL,
            path_s3= S3bucket_folder
        )
        print('Success unload data to path folder S3 :\n',S3bucket_folder)

        copier = redshift_exec.selectCopy_redshift_(
            connection_reshift= connections,
            connection_s3= s3_exec.call_sessionS3(),
            path_s3=S3bucket_folder,
            table_=table_name,
            schema_=schema,
            primary_keys_=[
                primary_key
            ],
            mode_=mode_insert,
            varchar_lengths_= varchar_lengths,
            sortkey_=sortkey
        )
        print('Success copy data to path folder S3 :\n',table_name, schema)

    except:
        print('Error ingestion in : \n', sys.exc_info())
    else:
        return print('Success doing ETL')


async def operation_etl_datamodel(
    key_access_:str,
    secret_access_:str,
    hostname:str,
    dbname:str,
    username:str,
    password:str,
    S3bucket_folder:str,
    SQL:str,
    table_name:str,
    schema:str,
    primary_key: Optional[str] = None,
    mode_insert: Optional[str] = None,
    varchar_lengths:Optional[Dict[str, int]] = None,
    sortkey: Optional[List[str]] = None
):

    try:
        tasks = []
        tasks.append(
            asyncio.ensure_future(
                etl_datamodel(
                    key_access_,
                    secret_access_,
                    hostname,
                    dbname,
                    username,
                    password,
                    S3bucket_folder,
                    SQL,
                    table_name,
                    schema,
                    primary_key,
                    mode_insert,
                    varchar_lengths,
                    sortkey
                )
            )
        )
        original_result_etl = await asyncio.gather(*tasks)

    except:
        print('operations etl exeception in :\n', sys.exc_info())
    else:
        return original_result_etl
