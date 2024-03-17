from Database import Database

def initialize_db():
    db = Database()
    db.initialize_metadata_table()


if __name__ == '__main__':
    initialize_db()