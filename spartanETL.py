from typing import Optional, Dict, Union, List
import redshift_connector
import awswrangler as wr
import boto3
import sys


class SpartanBase:

    def __init__(
        self,
        key_access: str,
        secret_access: str,
        region: str = 'ap-southeast-1',
        params: Optional[Dict[str, Union[str, int]]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if params is None:
            params = {}
        self.key_access = key_access
        self.secret_access = secret_access
        self.region = region
        self.params = params


    # generated session access to aws console
    def call_sessionS3 (
        self
    ):
        session_s3 = boto3.Session (
	                            aws_access_key_id= self.key_access,
	                            aws_secret_access_key=self.secret_access,
	                            region_name=self.region
	                        )

        return session_s3

    
    # get list path folder/object on s3 bucket
    def S3_bucketObject_list(
                            self,
                            s3_path: str,
                            s3_session
                        ):
        try:
            path_list = wr.s3.list_directories(
                                           path=s3_path,
                                           boto3_session = s3_session
                                        )

        except wr.exceptions as exepts:
            print(exepts)

        else:
            return path_list


    def S3_Object_list(
                            self,
                            s3_path: str,
                            s3_session
                        ):
        try:
            objetc_list = wr.s3.list_objects(
                                           path=s3_path,
                                           boto3_session = s3_session
                                        )

        except wr.exceptions as exepts:
            print(exepts)

        else:
            return objetc_list


    def S3_remove_object(
                            self,
                            s3_path: str,
                            s3_session
                        ):
        try:
            objetc_list = wr.s3.delete_objects(
                                           path=s3_path,
                                           boto3_session = s3_session
                                        )

        except wr.exceptions as exepts:
            print(exepts)

        else:
            return objetc_list


class SpartanTransaction:

    def __init__(
        self,
        db_host: str,
        db_dbname: str,
        db_user: str,
        db_pass : str,
        region: Optional[str] = None
    ):
        self.db_host = db_host
        self.db_dbname = db_dbname
        self.db_user = db_user
        self.db_pass = db_pass
        self.region = region


    # define connection redshift
    def connection_redshift_(
        self 
    ):
        try:
            connection_data = redshift_connector.connect(
                host=self.db_host,
                database=self.db_dbname,
                user=self.db_user,
                password=self.db_pass
            )

        except redshift_connector.Error as er:
            print("OOps: Something Else:", er)

        else:
            return connection_data


    def read_data_redshift_(
            self,
            dbCon,
            sql_statement
    ): #read data from sources
        try:

            connection_= dbCon
            datareader = wr.redshift.read_sql_query(
                sql=sql_statement,
                con=connection_
            )

            connection_.close()

        except:
            print('Opps: Check your statemen query:', sys.exc_info())

        else:
            return datareader


    def unloader_reshift(
        self,
        sql_,
        path_s3,
        connection_reshift,
        iamrole,
        region: str = 'ap-southeast-1'
    ):
        try:
            unloader = wr.redshift.unload(
                sql=sql_,
                path=path_s3,
                con=connection_reshift,
                iam_role=iamrole,
                region=region,
                max_file_size= 3000
            )

        except:
            print('Opps: Check your statemen unload:', sys.exc_info())

        else:
            return print('success unload data to \n', str(path_s3))


    def selectUnload_redshift_(
        self,
        connection_reshift,
        connection_s3,
        sql_statement,
        path_s3,
        unload_to_files_:str = 'PARQUET',
        region_:str = 'ap-southeast-1',
        field_partition_:Optional[Dict[str, Union[str, int]]] = None,
        iam_role:str =None
    ):
        try:
            connection_= connection_reshift,
            loader_= wr.redshift.unload_to_files(
                sql=sql_statement,
                path=path_s3,
                con=connection_reshift,
                boto3_session=connection_s3,
                unload_format=unload_to_files_,
                max_file_size= 3000,
                partition_cols=field_partition_,
                region=region_,
                iam_role=iam_role
            )


        except:
            print('Opps: Check your statemen unload:', sys.exc_info())

        else:
            return print('success unload data to \n', str(path_s3))



    def selectCopy_redshift_(
        self,
        connection_reshift,
        connection_s3,
        path_s3,
        table_:str,
        schema_:str,
        mode_:str="upsert",
        overwrite_method_: str = "cascade",
        diststyle_: str = "ALL",
        distkey_: Optional[str] = None,
        sortstyle_: str = "COMPOUND",
        sortkey_: Optional[List[str]] = None,
        primary_keys_: Optional[List[str]] = None,
        varchar_lengths_default_: int = 356,
        varchar_lengths_: Optional[Dict[str, int]] = None,
        iam_role:str =None
    ):
        try:
            connection_= connection_reshift,
            copy_= wr.redshift.copy_from_files(
                path=path_s3,
                con=connection_reshift,
                boto3_session=connection_s3,
                table=table_,
                schema=schema_,
                mode=mode_,
                overwrite_method=overwrite_method_,
                diststyle=diststyle_,
                distkey=distkey_,
                sortstyle=sortstyle_,
                sortkey=sortkey_,
                primary_keys=primary_keys_,
                varchar_lengths_default=varchar_lengths_default_,
                varchar_lengths=varchar_lengths_,
                use_threads=True,
                iam_role=iam_role
            )


        except:
            print('Opps: Check your statemen copy : \n', sys.exc_info())

        else:
            return print('success copy data to \n', str(schema_), str(table_))
