from sqlite3 import OperationalError, Cursor
from Database import Database
import os
import pytest

def test_connect_succes():
    """
    Test the connect method for successful connection and cursor creation.
    """
    db = Database(':memory:')  # Use an in-memory database for testing
    with db.connect() as cursor:
        assert isinstance(cursor, Cursor)
     
def test_connect_exception():
    """
    Test the connect method to ensure it raises an exception for a failed connection.
    """
    db = Database('/non/existent/path.db')  # Intentionally incorrect path
    with pytest.raises(OperationalError):
        with db.connect() as cursor:
            pass

def test_transaction_rollback():
    """
    Test that transactions are rolled back on error.
    """
    db = Database(':memory:')
    with pytest.raises(OperationalError):
        with db.connect() as cursor:
            cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)')
            # Introduce an error by trying to create the same table again
            cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)')
    
    # Ensure the table was not created
    with db.connect() as cursor:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
        assert cursor.fetchone() is None

def test_transaction_commit():
    """
    Test that transactions are comitted if no errors occur.
    """
    db = Database('test.db')
    with db.connect() as cursor:
        cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)')
        cursor.execute('INSERT INTO test (id) VALUES (1)')
    
    # After the block is finished, the context manager should commit the transaction.
    # Now we check that the data was indeed committed.
    with db.connect() as cursor:
        cursor.execute('SELECT * FROM test WHERE id = 1')
        row = cursor.fetchone()
        assert row is not None
        assert row['id'] == 1  # Make sure the data matches what was inserted
    os.remove('test.db')