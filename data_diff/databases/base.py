from data_diff.sqeleton.databases.base import BaseDialect, AbstractMixin_MD5, AbstractMixin_NormalizeValue


class BaseDialect(BaseDialect, AbstractMixin_MD5, AbstractMixin_NormalizeValue):
    pass
