from copy import deepcopy
from pyspark.sql.types import StructField, StructType, ArrayType
from belly import read_schema

def replace_with_type(schema, target, NewType):
    if hasattr(schema, 'fields'):
        new_struct = StructType()
        for field in schema:
            if field.name == target:
                try:
                    dt = NewType()
                except TypeError:
                    dt = deepcopy(NewType)
            else:
                dt = replace_with_type(field.dataType, target, NewType)

            new_struct.add(field.name, dt)
        return new_struct
    elif hasattr(schema, 'elementType'):
        dt = replace_with_type(schema.elementType, target, NewType)
        new_array = ArrayType(dt)
        return new_array
    else:
        return schema

def get_field(struct, name):
    return [f.dataType for f in struct.fields if f.name == name][0]

def set_field(struct, name, value):
    new_struct = StructType()
    for f in struct:
        if f.name != name:
            new_struct.add(f)

    struct.add(name, data_type = deepcopy(value))
    return struct


def filter_fields(struct, fields):
    new_struct = StructType()
    for f in struct:
        if f.name not in fields:
            new_struct.add(f)
    return new_struct

def create_schema():
    # random original schema inferred from a bunch of tweets
    schema = read_schema('gs://spain-tweets/schemas/tweet-3.pickle')
    user = get_field(schema, 'user')

    s = deepcopy(schema)
    s = replace_with_type(s, 'source_user', user)
    s = replace_with_type(s, 'created_at', TimestampType)

    s = filter_fields(s, ['th_original', 'th_rehydrated'])
    s = filter_fields(s, ['retweeted_status', 'quoted_status'])

    s = set_field(s, 'quoted_status', s)
    s = set_field(s, 'retweeted_status', s)

    # remove if we no longer want UB original formats
    s = set_field(s, 'th_original', get_field(schema, 'th_original'))
    s = set_field(s, 'th_rehydrated', get_field(schema, 'th_rehydrated'))

    return s
