from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from zipfile import ZipFile, ZIP_DEFLATED
from datetime import datetime
from tools import timer, human_size
import os
import sys


class Crypter:
    """
    The Crypter class provides functionalities for encrypting, decrypting,
    zipping, and unzipping files. It supports operations in a staged manner
    across different directory paths for raw, staging, and clean data.

    Attributes:
        RAW_PATH (str): Directory path for raw files.
        STAGING_PATH (str): Directory path for staging files.
        CLEAN_PATH (str): Directory path for clean files.
        QUERIES_PATH (str): Directory path for SQL query files.
        AES_KEY (bytes): The AES key used for encryption and decryption. Needs to be length=16
    """
    RAW_PATH = 'raw'
    STAGING_PATH = 'staging'
    CLEAN_PATH = 'clean'
    QUERIES_PATH = 'queries'
    # aes_key = get_random_bytes(16)
    AES_KEY = b"0361231230000000"

    def __init__(self, file_name: str = 'test') -> None:
        """
        Initializes the Crypter instance with a base file name and ensures
        necessary directories exist.

        Args:
            file_name (str): The base name of the file to be processed.
                             Defaults to 'test'.
        """
        if '.' in file_name:
            file_name = file_name.split('.')[0]
        self.file_name = file_name
        self.unique_name = self.create_unique_name()
        # Setting variables this class will use
        self.crypt_file_name = file_name + '.crypt'
        self.sql_file_name = file_name + '.sql'
        self.zip_file_name= self.unique_name + '.zip'
        
        self.crypt_file_size = ''
        self.sql_file_size = ''
        self.zip_file_size = ''
         
        # Ensure folders are in
        self.ensure_paths()
    
    def ensure_paths(self):
        """
        Ensures that the required directories (raw, staging, clean, and queries)
        exist, creating them if necessary.
        """
        for _path_ in (self.RAW_PATH, self. STAGING_PATH, self.CLEAN_PATH, self.QUERIES_PATH):
            if not os.path.exists(_path_):
                os.makedirs(_path_)
    
    def create_unique_name(self):
        """
        Generates a unique name for a file based on the current date and time.

        Returns:
            str: A unique file name.
        """
        return f"{self.file_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            
    def zip_file(self, input_file_name: str=None):
        """
        Zips the specified SQL file and stores it in the raw directory. If no
        file name is provided, uses the instance's SQL file name.

        Args:
            input_file_name (str, optional): The name of the file to zip.
                                             Defaults to None.
        """
        sql_file_name = self.sql_file_name if input_file_name is None else f"{input_file_name}.sql"
        zip_file_name = self.unique_name + '.zip' if input_file_name is None else f"{input_file_name}.zip"

        sql_file_path = os.path.join(self.QUERIES_PATH, sql_file_name)
        zip_file_path = os.path.join(self.RAW_PATH, zip_file_name)

        # Create a zip file and add the .sql file
        with ZipFile(zip_file_path, 'w', ZIP_DEFLATED) as zipf:
            # arcname is the name inside the zip
            zipf.write(sql_file_path, os.path.basename(sql_file_path))


    def extract_file_in_zip(self, file_name: str=None):
        """
        Extracts the first file inside the specified zip file into the clean
        directory. If no file name is provided, uses the instance's zip file name.

        Args:
            file_name (str, optional): The name of the zip file to extract.
                                       Defaults to None.
        """
        zip_file_path = os.path.join('staging', file_name) if file_name is not None else os.path.join('staging', self.zip_file_name)
        with ZipFile(zip_file_path, 'r') as myzip:
            inside_file = myzip.namelist()[0]
            myzip.extract(inside_file, 'clean')
        file_unique_zip_path_raw =  os.path.join('raw', self.unique_name+'.zip')
        # zip_file_path_staging =  os.path.join('raw', self.unique_name+'.zip')
        
        self.original_file_name = str(inside_file)
        self.sql_file_size = human_size(os.path.getsize(os.path.join('clean',self.original_file_name)))

        
        files_paths_to_remove = [zip_file_path,file_unique_zip_path_raw, ]

        self.remove_files(files_paths_to_remove)
        

    def crypt_data(self, data, file_name: str = None):
        """
        AES encryption of the data with with the name of the variable file_name, 
        otherwise uses the file_name instance.

        Args:
            data: Data that want to be encryipted as file´.
            file_name (str, optional): The name of the crypt file name.
                                       Defaults to None.
        """
        file_to_crypt = os.path.join('raw',self.crypt_file_name) if file_name is None else os.path.join('raw',f'{file_name}.crypt')

        cipher = AES.new(self.AES_KEY, AES.MODE_OCB)
        cipher_text, tag = cipher.encrypt_and_digest(data)

        with open(file_to_crypt, 'wb') as f:
            f.write(tag)
            f.write(cipher.nonce)
            f.write(cipher_text)
        
        self.crypt_file_size = human_size(os.path.getsize(file_to_crypt))

    def encrypt_zip_file(self, zip_file_name: str=None):
        """
        Encrypts a zip file specified by its name and stores the encrypted data
        in a '.crypt' file within the 'raw' directory. If no file name is provided,
        the instance's default zip file name is used.

        Args:
            zip_file_name (str, optional): The name of the zip file to encrypt.
                                        Defaults to None, which uses the instance's zip file name.
        """
        zip_file_path = os.path.join('raw', zip_file_name) if zip_file_name is not None else os.path.join('raw', self.zip_file_name)
        # Read the zip file as bytes
        with open(zip_file_path, 'rb') as f:
            zip_bytes = f.read()
        # Encrypt the zip bytes
        self.crypt_data(zip_bytes)



    def decrypt_data(self, file_name: str = None):
        """
        Decrypts a '.crypt' file specified by its name and returns the decrypted data.
        If the decryption fails due to data modification, the process is terminated.

        Args:
            file_name (str, optional): The name of the '.crypt' file to decrypt.
                                    Defaults to None, which uses the instance's crypt file name.

        Returns:
            bytes: The decrypted data from the '.crypt' file.

        Raises:
            SystemExit: If the decrypted data fails verification, indicating modification.
        """
        file_to_decrypt = os.path.join('raw', file_name+'.crypt') if file_name is not None else os.path.join('raw', self.crypt_file_name)
        with open(file_to_decrypt, "rb") as f:
            tag = f.read(16)
            nonce = f.read(15)
            ciphertext = f.read()

        cipher = AES.new(self.AES_KEY, AES.MODE_OCB, nonce=nonce)
        try:
            message = cipher.decrypt_and_verify(ciphertext, tag)
            return message
        except ValueError:
            print("The message was modified!")
            sys.exit(1)
    
    def decrypt_to_staging(self, file_name: str=None):
        """
        Decrypts a '.crypt' file and stores the decrypted data as a zip file in the
        'staging' directory. The method returns the name of the decrypted zip file.

        Args:
            file_name (str, optional): The name of the '.crypt' file to decrypt.
                                    Defaults to None, which uses the instance's crypt file name.

        Returns:
            str: The name of the decrypted zip file stored in the 'staging' directory.
        """
        crypt_file_name = file_name+'.crypt' if file_name is not None else self.crypt_file_name
        
        # Decrypt the .crypt file
        decrypted_zip_bytes = self.decrypt_data()

        # Define the decrypted zip file path in the staging directory
        decrypted_zip_file_name = self.unique_name+'.zip'
        decrypted_zip_path = os.path.join('staging', decrypted_zip_file_name)

        # Write the decrypted zip bytes to the zip file in the staging directory
        with open(decrypted_zip_path, 'wb') as zip_file:
            zip_file.write(decrypted_zip_bytes)
        self.zip_file_size = human_size(os.path.getsize(decrypted_zip_path))
        return decrypted_zip_file_name  # Return the name of the decrypted zip file


    def decrypt_to_clean(self, file_name:str=None):
        """
        Extracts the '.sql' file from a decrypted zip file and stores it in the 'clean' directory.
        The method also cleans up by removing relevant files in the 'raw' and 'staging' directories.

        Args:
            file_name (str, optional): The name of the zip file without the '.zip' extension.
                                    Defaults to None, which uses the instance's unique name.
        """
        zip_file_name = file_name+'.zip' if file_name is not None else self.unique_name+'.zip'
        zip_file_path = os.path.join('raw', zip_file_name)
        # Extract the .sql file from the zip to the clean directory
        with ZipFile(zip_file_path, 'r') as zipf:
            zipf.extractall('clean')
        # Remove crypt and staging
        # file_to_decrypt = os.path.join('raw', file_name+'.crypt') if file_name is not None else os.path.join('raw', self.crypt_file_name)
        file_unique_zip_path_raw =  os.path.join('raw', self.unique_name+'.zip')
        zip_file_path_staging =  os.path.join('staging', self.file_name+'.zip')
        files_paths_to_remove = [zip_file_path,file_unique_zip_path_raw,zip_file_path_staging]
        self.remove_files(files_paths_to_remove)
        
            
    def extract_crypt_to_clean(self, file_name:str):
        """
        Extracts the '.sql' file from a '.crypt' file directly into the 'clean' directory
        after decryption. This method assumes the '.crypt' file contains a zip file with the '.sql'.

        Args:
            file_name (str): The name of the '.crypt' file to be decrypted and extracted.
        """
        zip_file_path = os.path.join('raw', file_name)

        # Extract the .sql file from the zip to the clean directory
        with ZipFile(zip_file_path, 'r') as zipf:
            zipf.extractall('clean')

    def remove_files(self, list_paths:list):
        """
        Removes the files specified in the list of paths.

        Args:
            list_paths (list): A list of file paths to be removed.
        """
        for file in list_paths:
            if os.path.exists(file):
                os.remove(file)

@timer
def real_scenario(file_name: str = 'bookstore_example2.sql'):
    """
    Simulates a real-world scenario of encrypting and decrypting a SQL file.

    This function performs the following steps:
    1. Zips the specified SQL file.
    2. Encrypts the zip file and stores the encrypted data in a '.crypt' file.
    3. Decrypts the '.crypt' file and moves the decrypted zip file to the staging directory.
    4. Extracts the SQL file from the decrypted zip file into the clean directory.

    Args:
        file_name (str): The name of the SQL file to process. Defaults to 'bookstore_example2.sql'.
    """
    cls = Crypter(file_name=file_name)
    cls.zip_file()
    cls.encrypt_zip_file()

    decrypted_zip_file_name = cls.decrypt_to_staging()
    cls.extract_file_in_zip()
    end_result_path = os.path.join('clean', file_name)
    with open(end_result_path, 'r') as f:
        data = f.read()
    print(data[0:10])

@timer
def real_scenario_memory(file_name: str = 'bookstore_example2.sql'):
    """
    Simulates a real-world scenario of encrypting and decrypting a SQL file,
    focusing on the decryption and extraction process into the clean directory.

    This function performs the following steps:
    1. Zips the specified SQL file.
    2. Encrypts the zip file and stores the encrypted data in a '.crypt' file.
    3. Decrypts the '.crypt' file and extracts the SQL file directly into the clean directory.

    Args:
        file_name (str): The name of the SQL file to process. Defaults to 'bookstore_example2.sql'.
    """
    cls = Crypter(file_name=file_name)
    cls.zip_file()
    cls.encrypt_zip_file()
    cls.decrypt_to_clean()
    
    end_result_path = os.path.join('clean', file_name)
    with open(end_result_path, 'r') as f:
        data = f.read()
    print(data[0:10])
    
if __name__ == '__main__':
    real_scenario()
    real_scenario_memory()
