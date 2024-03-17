import sqlite3
from contextlib import contextmanager
from tools import get_logger

class Database:
    def __init__(self, db_name: str = 'db.db') -> None:
        self.db_name = db_name
        self.logger = get_logger()  
        self._conn = None

    def _get_connection(self):
        """Get an existing database connection or create a new one."""
        if self._conn is None or self.db_name != ':memory:':
            self._conn = sqlite3.connect(self.db_name)
            self._conn.row_factory = sqlite3.Row
        return self._conn
    
    @contextmanager
    def connect(self):
        """Context manager for connections to db.
        
        This method established connection to a SQLite database, sets the 
        row factory as a dict and yields the cursor object for executing operations.
        If an error occur, it rolls back  any changes in the database and re-raises the exception.
        Finally, it commits transacion and closses the connection.

        Yields:
            SQLite3.Cursor: A cursor to interact with the db.
            
        Raises:
            Exceptions: Propagates any exception during the database operations, after rolling back the transacion.
        
        Usage example:
            with Database('my_database.db').connect() as cursor:
                cursor.execute('SELECT * FROM my_table')
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            yield cur
        except Exception as e:
            if conn:
                conn.rollback() # Roll back any changes if an exception occurs
            self.logger.error(f"<Database> Error occured: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.commit()
                conn.close()

    def initialize_metadata_table(self):
        """Creates the file_metadata table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS file_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            another_relational_id INTEGER NULL,
            uploader_id INTEGER NULL,
            unique_filename TEXT NOT NULL,
            crypt_filename TEXT,
            zip_filename TEXT,
            final_filename TEXT,
            original_file_size TEXT,
            zip_file_size TEXT,
            crypt_file_size TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            zipped_at DATETIME,
            encrypted_at DATETIME,
            decrypted_at DATETIME,
            extracted_at DATETIME,
            status TEXT CHECK(status IN ('created', 'zipped', 'encrypted', 'decrypted', 'extracted', 'error')) NOT NULL DEFAULT 'created',
            error_message TEXT
        );
        """
        with self.connect() as cursor:
            cursor.execute(create_table_sql)
            self.logger.info(f"<Database> Table file_metadata created.")
    
    def insert_item_metadata_table(self, params):
        """
        Inserts new item in table metadata.
        """
        insert_item_sql = """
        INSERT INTO file_metadata(uploader_id, unique_filename, crypt_filename, zip_filename, crypt_file_size)
        VALUES (?, ?, ?, ?, ?)
        """
        with self.connect() as cursor:
            cursor.execute(insert_item_sql, tuple(params.values()))
        self.logger.info(f"<Database> New item in file_metadata with unique_filename: {params['unique_filename']}")
        
    def check_if_new_item(self):
        """
        Gets last item created in file_metadata
        """
        insert_item_sql = """
        SELECT * FROM file_metadata WHERE status='created'
        """
        with self.connect() as cursor:
            result = cursor.execute(insert_item_sql).fetchone()
            if result:
                self.logger.info(f"<Database> New item in file_metadata with unique_filename: {result['unique_filename']}")
                return result
            
            
    def update_item_decrypted(self, params):
        """
        Update item in table metadata as decrypted.
        """
        update_item_sql = """
        UPDATE file_metadata
        SET zip_file_size = ?,
            decrypted_at = ?,
            status = 'decrypted'
        WHERE id = ?
        """
        with self.connect() as cursor:
            cursor.execute(update_item_sql, tuple(params.values()))
            self.logger.info(f"<Database> Updated item with id: {params['id']} as decrypted")
    
    def update_item_finished(self, params):
        """
        Update item in table metadata as finished.
        """
        update_item_sql = """
        UPDATE file_metadata
        SET original_file_size = ?,
            final_filename = ?,
            extracted_at = ?,
            status = 'extracted'
        WHERE id = ?
        """
        with self.connect() as cursor:
            cursor.execute(update_item_sql, tuple(params.values()))
            self.logger.info(f"<Database> Updated item with id: {params['id']} as extracted.")