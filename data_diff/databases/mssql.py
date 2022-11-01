# class MsSQL(ThreadedDatabase):
#     "AKA sql-server"

#     def __init__(self, host, port, user, password, *, database, thread_count, **kw):
#         args = dict(server=host, port=port, database=database, user=user, password=password, **kw)
#         self._args = {k: v for k, v in args.items() if v is not None}

#         super().__init__(thread_count=thread_count)

#     def create_connection(self):
#         mssql = import_mssql()
#         try:
#             return mssql.connect(**self._args)
#         except mssql.Error as e:
#             raise ConnectError(*e.args) from e

#     def quote(self, s: str):
#         return f"[{s}]"

#     def md5_as_int(self, s: str) -> str:
#         return f"CONVERT(decimal(38,0), CONVERT(bigint, HashBytes('MD5', {s}), 2))"
#         # return f"CONVERT(bigint, (CHECKSUM({s})))"

#     def to_string(self, s: str):
#         return f"CONVERT(varchar, {s})"
