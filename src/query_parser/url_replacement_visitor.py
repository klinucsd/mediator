import hashlib
from urllib.parse import urlparse

from decouple import config
from pglast.ast import RangeVar, ColumnRef, String, SelectStmt, RawStmt
from pglast.visitors import Visitor

secret_key = config('secret_key').encode()


def to_table_name(input_string):
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
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


class URLReplacementVisitor(Visitor):
    def __init__(self):
        super().__init__()
        self.url_to_table_mapping = {}

    def visit(self, ancestors, node):
        if isinstance(node, RangeVar):
            if is_valid_url(node.relname):
                new_relname = to_table_name(node.relname)
                self.url_to_table_mapping[node.relname] = new_relname
                node.relname = new_relname
        elif isinstance(node, ColumnRef):
            for field in node.fields:
                if isinstance(field, String) and field.sval in self.url_to_table_mapping.keys():
                    field.sval = self.url_to_table_mapping[field.sval]

