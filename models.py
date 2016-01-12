import peewee

database_proxy = peewee.Proxy()


class BaseModel(peewee.Model):
    class Meta:
        database = database_proxy


class Page(BaseModel):
    project = peewee.CharField()
    wiki_id = peewee.IntegerField()
    title = peewee.CharField(null=True)

    class Meta:
        indexes = (
            # create a unique on from/to/date
            (('project', 'wiki_id'), True),
        )


class Identifier(BaseModel):
    type = peewee.CharField()
    name = peewee.CharField()

    class Meta:
        indexes = (
            (('type', 'name'), True),
        )


class IdentifiersHistory(BaseModel):
    identifier = peewee.ForeignKeyField(Identifier)
    page = peewee.ForeignKeyField(Page)
    start_date = peewee.DateTimeField(null=True)
    end_date = peewee.DateTimeField(null=True)

    class Meta:
        indexes = (
            (('identifier', 'page', 'start_date', 'end_date'), True),
        )


class IdentifiersHistoryCounts(BaseModel):
    history_entity = peewee.ForeignKeyField(IdentifiersHistory)
    count = peewee.IntegerField()


def create_tables(safe=True):
    # database_proxy.connect()
    database_proxy.create_tables([Page, Identifier, IdentifiersHistory, IdentifiersHistoryCounts], safe)
