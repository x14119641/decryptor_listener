from Database import Database
from Crypter import Crypter
from random import randint



def insert_item(file_name='example.sql'):
    db = Database()
    db.initialize_metadata_table()
    item = Crypter(file_name=file_name)
    
    # Encrypt the file
    item.zip_file()
    item.encrypt_zip_file()
    
    # Prepare insert
    #uploader_id, unique_filename, crypt_filename, zip_filename, original_file_size
    initial_insert_params = {
        'uploader_id': randint(1000000, 9999999),
        'unique_filename': item.unique_name, 
        'crypt_filename': item.crypt_file_name, 
        'zip_filename': item.zip_file_name, 
        'crypt_file_size': item.crypt_file_size
    }
    db.insert_item_metadata_table(params=initial_insert_params)
    
if __name__ == '__main__':
    insert_item()
    