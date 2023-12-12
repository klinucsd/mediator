import hashlib
from urllib.parse import urlparse

from decouple import config
from pglast.ast import RangeVar, ColumnRef, String, SelectStmt, RawStmt
from pglast.visitors import Visitor

# Get the secret key from the configuration
secret_key = config('secret_key').encode()


def to_table_name(input_string):
    """
    Convert an input string to a table name using MD5 hash.

    Args:
        input_string (str): The input string to be hashed.

    Returns:
        str: The resulting table name.
    """

    # Generate the MD5 hash of the input string
    hashcode = hashlib.md5(input_string.encode() + secret_key).hexdigest()

    # Ensure that the hash starts with a letter
    if not hashcode[0].isalpha():
        # If the first character is not a letter, find the first letter in the hash
        for char in hashcode:
            if char.isalpha():
                # Swap the first non-letter character with the found letter
                hashcode = char + hashcode[1:]  # Replace the first character
                break

    return hashcode


def is_valid_url(url):
    """
    Check if a URL is valid.

    Args:
        url (str): The URL to be validated.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


class URLReplacementVisitor(Visitor):
    """
    Visitor class for replacing URLs with hashed table names in AST.

    Attributes:
        url_to_table_mapping (dict): A mapping of original URLs to hashed table names.
    """

    def __init__(self):
        """
        Initializes a URLReplacementVisitor instance.
        """
        super().__init__()
        self.url_to_table_mapping = {}

    def visit(self, ancestors, node):
        """
        Visit method to replace URLs with hashed table names in AST.

        Args:
            ancestors (list): List of ancestor nodes.
            node: The current node being visited.
        """
        if isinstance(node, RangeVar):
            # Replace URL with hashed table name if it's a valid URL
            if is_valid_url(node.relname):
                new_relname = to_table_name(node.relname)
                self.url_to_table_mapping[node.relname] = new_relname
                node.relname = new_relname
        elif isinstance(node, ColumnRef):
            # Replace URL with hashed table name in ColumnRef if present in mapping
            for field in node.fields:
                if isinstance(field, String) and field.sval in self.url_to_table_mapping.keys():
                    field.sval = self.url_to_table_mapping[field.sval]

