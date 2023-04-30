"""Objects used by account.py, scanner.py and progress.py to keep their data."""

class AbstractKV:
    """Abstract class for storing key-value pairs."""
    def __init__(self):
        self.cache = {}

    def __getitem__(self, key):
        return self.cache[key]

    def __setitem__(self, key, value):
        self.cache[key] = value

    def load(self):
        """Loads data from a file."""
        raise NotImplementedError

    def save(self):
        """Saves data to a file."""
        raise NotImplementedError


class SupabaseKV(AbstractKV):
    """Class for storing key-value pairs in a Supabase SQL table."""

    def __init__(self, supabase_client, table_name):
        super().__init__()
        self.supabase_client = supabase_client
        self.table_name = table_name

    def load(self):
        """Loads data from a the SQL table."""
        self.cache = self.supabase_client.execute(f"SELECT * FROM {self.table_name}").to_dict()
