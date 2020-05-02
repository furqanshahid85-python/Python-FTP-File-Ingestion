"""
This module provides the functionality of uploading files to s3 from a FTP server. An SFTP connection is
created with the FTP server and all the files present in the specified directory are uploaded to the specified
s3 bucket.
Following are the key features of this module:
* Creates a secure ssh connection with FTP server.
* Handles multipart upload to s3 automatically, if file size is greater than 100MB (can be configured).
* Automatically handles retires in case of failed uploads during multipart upload.
* Partitions the data in s3 based on current year,month,day,hour.
* Ensures which file has been processed or needs to be processed.
*
*
*
*
"""

import os
import boto3
import config as cfg
from boto3.s3.transfer import TransferConfig
from datetime import datetime

# uncomment following lines if running as an AWS Glue Job.

# install_path = os.environ['GLUE_INSTALLATION']
# easy_install.main( ["--install-dir", install_path, "https://files.pythonhosted.org/packages/ac/15/4351003352e11300b9f44a13576bff52dcdc6e4a911129c07447bda0a358/paramiko-2.7.1.tar.gz"] )
# reload(site)

import paramiko

congfig_file = "config.json"


class FTPIngestion:

    def __init__(self):
        self.ssh_client = cfg.SSH_CLIENT
        self.sftp_client = cfg.SFTP_CLIENT
        self.ssh_ok = cfg.SSH_OK
        self.sftp_ok = cfg.SFTP_OK
        self.ftp_host = cfg.FTP_HOST
        self.ftp_port = cfg.FTP_PORT
        self.ftp_username = cfg.FTP_USERNAME
        self.ftp_password = cfg.FTP_PASSWORD
        self.s3 = boto3.client('s3')
        self.s3_bucket_name = cfg.S3_BUCKET
        self.ftp_directory_path = cfg.PARENT_DIR_PATH
        self.ftp_processed_path = cfg.PROCESSED_DIR_PATH

    def create_ssh_connection(self):
        """
        Creates SSH connection with FTP server. The authentication credentials are provided in the config file.
        Can also use AWS SSM instead of Config file.
        :return : Bool; True if connection successfull, False if not
        """

        print('Establishing connection with host...')
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())

            # in production, use load_system_host_keys
            # ssh_client.load_system_host_keys()

            self.ssh_client.connect(hostname=self.ftp_host, username=self.ftp_username,
                                    password=self.ftp_password, port=self.ftp_port)
            print('connected')
            self.ssh_ok = True

        except paramiko.AuthenticationException as AuthFailException:
            self.ssh_ok = False
            print('Authentication Failed, error: ', AuthFailException)
        except paramiko.SSHException as sshException:
            self.ssh_ok = False
            print('Could not establish ssh connection, error: ', sshException)
        except Exception as error:
            self.ssh_ok = False
            print('Error establishing connection, error: ', error)

        return self.ssh_ok

    def create_sftp_connection(self):
        """
        Creates an SFTP connection from the ssh connection created wiht the FTP server.
        :return : Bool; True if connection successful, False if not.
        """
        try:
            if self.create_ssh_connection():
                print('Establishing SFTP connection...')
                self.sftp_client = self.ssh_client.open_sftp()
                print('SFTP connection successfull')
                self.sftp_ok = True
            else:
                print('Could not establish ssh connection')

        except paramiko.SFTPError as sftpError:
            self.sftp_ok = False
            print('could not establish sftp connection, error: ', sftpError)
        except Exception as error:
            self.sftp_ok = False
            print('could not establish sftp connection, error: ', error)
        return self.sftp_ok

    def move_files_to_processed(self, source_file_name):
        """
        This method moves the original files from parent directory to processed directory on the FTP server
        after the files have been successfully uploaded on s3.
        :param source_file_path: directory path in which file resides
        :param source_file_name: name of the file
        """

        print('moving '+source_file_name+' to processed directory.')
        processed_directory = self.ftp_processed_path
        src = self.ftp_directory_path+'/' + source_file_name
        dest = processed_directory + source_file_name

        try:
            _, _, _ = self.ssh_client.exec_command(
                "mv " + src+" " + dest)
        except Exception as error:
            print("error moving files to processed directory, error: ", error)

    def create_s3_partition(self):
        """
        This method develops the partition string that will be used when uploading to the s3. The files are
        uploaded to the specified partition.
        The partition structure is as follows:
        /<directory_name>/year = <year>/ month = <month>/ day = <day>/ hour = <hour>/file

        """
        parent_dir = self.ftp_directory_path.split("/")[-1]
        current_date = datetime.now()
        current_year = str(current_date.year)
        current_month = str(current_date.month)
        current_day = str(current_date.day)
        current_hour = str(current_date.hour)

        s3_partition = parent_dir + '/year = ' + current_year + '/month = ' + \
            current_month + '/day = ' + current_day + '/hour = ' + current_hour + '/'
        return s3_partition

    def s3_upload_file_multipart(self, source_file, s3_target_file):
        """
        This method uploads file to s3. If file size is greater than 100MB then file gets uploaded via
        multipart. The retires are handed automatically in case of failure. If file is less than 100MB in size
        then it gets uploaded in regular manner. Each part of the file during multipart upload would be 50MB in
        size.
        Following parameters are configurable in Config File based on user needs:

        multipart_threshold : 100MB (default)
        multipart_chunksize : 20 MB (default)
        max_concurrency : 10 (default)
        user_threads : True (default)

        :param source_file: the file object that is to be uploaded.
        :param s3_target_file: the object in s3 bucket.

        """
        try:
            config = TransferConfig(
                multipart_threshold=cfg.MULTIPART_THRESHOLD, multipart_chunksize=cfg.MULTIPART_CHUNKSIZE,
                max_concurrency=cfg.MAX_CUNCURRENCY, use_threads=cfg.USER_THREADS)

            self.s3.upload_fileobj(source_file, self.s3_bucket_name,
                                   s3_target_file, Config=config)
            return True
        except Exception as error:
            print('could not upload file using multipart upload, error: ', error)
            return False

    def initiate_ingestion(self):
        """
        This method initiates the calls to establish ssh and sftp connections. Changes FTP directory path to
        specified path. Gets list of all the files in the FTP specified path and starts upload to s3. Once all
        files are uploaded closes all the connections. Ensures which file has been processed or needs to be
        processed.
        """
        try:
            if self.create_sftp_connection():
                self.sftp_client.chdir(self.ftp_directory_path)
                files_to_upload = self.sftp_client.listdir()
                s3_partition = self.create_s3_partition()
                files_to_move = []

                for ftp_file in files_to_upload:
                    sftp_file_obj = self.sftp_client.file(ftp_file, mode='r')
                    if self.s3_upload_file_multipart(
                            sftp_file_obj, s3_partition+ftp_file):
                        print('file uploaded to s3')
                        files_to_move.append(ftp_file)

                # move files from parent dir to processed dir
                if files_to_move:
                    for uploaded_file in files_to_move:
                        self.move_files_to_processed(uploaded_file)
                else:
                    print("nothing to upload")
            else:
                print('Could not establish SFTP connection')
        except Exception as error:
            print("file ingestion failed, error: ", error)

        self.close_connections()

    def close_connections(self):
        """
        This mehtod is used to close the ssh and sftp connections.
        """
        print('closing connections')
        self.sftp_client.close()
        self.ssh_client.close()


if __name__ == "__main__":
    ftp_obj = FTPIngestion()
    ftp_obj.initiate_ingestion()
