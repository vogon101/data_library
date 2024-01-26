import pandas as pd
import geopandas as gpd
import os
import requests
import logging

logger = logging.getLogger(__name__)

class DataLibrary:

    def __init__(self, folder, server, project_key, sftp_client=None, sftp_root=None):
        self.folder = folder
        self.server = server
        self.project_key = project_key
        self.sftp_client = sftp_client
        self.sftp_root = sftp_root

    def read_csv(self, filename, key=None, reader_args={}):
        return self.read(pd.read_csv, filename, key, reader_args)
    
    def read_geo(self, filename, key=None, reader_args={}):
        return self.read(gpd.read_file, filename, key, reader_args)


    def read(self, reader, filename, key=None, reader_args={}):
        if key is None:
            key = filename
        if not os.path.exists(f"{self.folder}/{filename}"):
            logger.info(f"Downloading {filename}")
            self.server_fetch(filename, key)
        else:
            if not self.server_exists(key):
                logger.info(f"Uploading {filename}")
                self.server_upload(filename, key)
            else:
                logger.info(f"Using cached {filename}")
                # if not self.server.hash(key) == self.hash(filename):
                #     raise Exception(f"Hash mismatch for {filename}.csv")

        return reader(f"{self.folder}/{filename}", **reader_args)
    
    def server_exists(self, key):
        url = f"{self.server}/{self.project_key}/{self.folder}/{key}"
        logger.debug(f"Checking if {url} exists on server")

        r = requests.head(url)
        return r.status_code == 200
    
    ## TODO: Implement this
    # def server_hash(self, key):
    #     r = requests.head(f"{self.server}/{key}")
    #     return r.headers["ETag"]

    def server_fetch(self, filename, key):
        url = f"{self.server}/{self.project_key}/{self.folder}/{key}"
        r = requests.get(url)
        
        if r.status_code != 200:
            raise Exception(f"Error fetching {url}")
        
        with open(f"{self.folder}/{filename}", "wb") as f:
            f.write(r.content)
    
    def server_upload(self, filename, key):
        if not self.sftp_client:
            logger.warning("No sftp client provided")
            return
        
        file_handle = f"{self.project_key}/{self.folder}/{filename}"
        logger.info(f"Uploading {filename} to {file_handle}")

        # Make sure the directory exists
        dirname = os.path.dirname(file_handle)
        
        logger.info(f"Creating directory {dirname}")
        sftp_make_dirs(self.sftp_client, dirname)
        
        self.sftp_client.chdir(self.sftp_root)
        self.sftp_client.put(f"{self.folder}/{filename}", file_handle)

def sftp_make_dirs(sftp_client, path):
    path_list = path.split('/')
    for s in path_list:
        if s == '':
            continue
        try:
            sftp_client.chdir(s)
        except IOError:
            logger.debug(f"Creating directory {s}")
            sftp_client.mkdir(s)
            sftp_client.chdir(s)
        
def create_data_library(folder, server, project_key=None, sftp_host=None, sftp_username=None, sftp_password=None, sftp_root=None):
    if sftp_host:
        import paramiko
        transport = paramiko.Transport((sftp_host, 22))
        transport.connect(username=sftp_username, password=sftp_password)
        sftp_client = paramiko.SFTPClient.from_transport(transport)
        sftp_client.chdir(sftp_root)
    else:
        sftp_client = None
    return DataLibrary(folder, server, project_key, sftp_client, sftp_root)

def get_data_library(envpath=None):
    import os
    from dotenv import load_dotenv
    if envpath:
        load_dotenv(dotenv_path=envpath)
    else:
        load_dotenv()
    return create_data_library(
        os.getenv("DATA_FOLDER"),
        os.getenv("DATA_SERVER"),
        os.getenv("DATA_PROJECT_KEY"),
        os.getenv("DATA_SFTP_HOST"),
        os.getenv("DATA_SFTP_USERNAME"),
        os.getenv("DATA_SFTP_PASSWORD"),
        os.getenv("DATA_SFTP_ROOT")
    )