from Database import Database
from Crypter import Crypter
from time import sleep
from datetime import datetime
import os

def main():
    while True:
        db = Database()
        item = db.check_if_new_item()
        if item:
            # Starts what needs to do
            print(dict(item))
            crypter = Crypter(file_name=item['crypt_filename'])
            decrypted_zip_file_name = crypter.decrypt_to_staging()
            update_params = {
                'zip_file_size': crypter.zip_file_size,
                'decrypted_at': datetime.now(),
                'id': item['id']
            }
            db.update_item_decrypted(params=update_params)
            crypter.extract_file_in_zip()
            update_params = {
                'original_file_size': crypter.sql_file_size,
                'final_filename': crypter.original_file_name,
                'extracted_at': datetime.now(),
                'id': item['id']
            }
            db.update_item_finished(params=update_params)
            end_result_path = os.path.join('clean', crypter.sql_file_name)
            with open(end_result_path, 'r') as f:
                data = f.read()
            print(data[0:10])
            # crypter.remove_files()
            break
        sleep(20)
        

if __name__=='__main__':
    main()