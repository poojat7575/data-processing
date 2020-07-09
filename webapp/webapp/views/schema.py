import colander
from datetime import datetime, date


@colander.deferred
def validate_end_date(node, kw):
    start_date = kw["start_date"]
    try:
        min_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        raise colander.Invalid(node, msg="start_date invalid")
    return colander.Range(min=min_date,
            max=date.today(),
            min_err="end date must fall after start date",
            max_err="end date can't be from future")


class DateRangeSchema(colander.MappingSchema):
    start_date = colander.SchemaNode(colander.Date(),
                                     validator=colander.Range(
                                         max=date.today(),
                                         max_err="start date can't be from future"))
    end_date = colander.SchemaNode(colander.Date(),
                                   validator=validate_end_date)


def schema_validator(payload):
    """
    validates the payload against DateRangeSchema schema
    :param payload: json request body
    :return: deserialized payload
    :raise: colander.Invalid in case of unexpected payload
    """
    schema = DateRangeSchema()
    if payload.get("start_date"):
        schema = schema.bind(start_date = payload.get("start_date"))
    deserialized_payload = schema.deserialize(payload)
    return deserialized_payload


