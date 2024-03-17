
# Decryptor/Listener

This project aims to simulate a listener that is checking if a new record is created in the table **file_metadata**.
A record in the table file_metadata is a file that has been inserted in the directory **/raw** that is **crypted**.

When a new item is deposited in the folder a "trigger" will update the table and the listener will start the process to decrypt that file while updating the item record with some metadata and once is finished sets the status as decrypted. 

The app sleeps for some seconds if there is no more new items in the directory.




## Installation

Create virtual machine and install requirements

```bash
  pipenv shell
  pip install -r requirements.txt
```
To initialitze the db
```bash
  python initialitze_db.py
```
## Usage/Examples
I have two main "runner" functions.
The 1st one initializes db if the db does not exists, encrypts a file in the /raw directory and creates an item in the file_metadata table.

```bash
python insert_item_runner.py
```

The second one is a while loop that checks if a new item has been inserted in the table file_metadata, that means a new file is in the /raw directory that must be decrypted. The end file is a .sql file. In the actual example we only do one item an exit the while loop.
```bash
python runner.py
```
**runner.py**
```python
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
```