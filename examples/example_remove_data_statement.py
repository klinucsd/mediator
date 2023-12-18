from pglast import prettify

from src.query_parser.mediator_query import MediatorQuery
from src.query_rewriter.rewrite_query import rewrite_query

query = "SELECT md_remove_data('http://www.sdsc.edu/ArcGIS/FeatureServer/test/4')"
md_query = MediatorQuery(query)

print('=' * 70)
print(prettify(md_query.sql))

print('=' * 70)
print(prettify(rewrite_query('user', query, False)))





