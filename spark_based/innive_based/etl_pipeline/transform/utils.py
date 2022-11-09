




def remove_empty_elements(d):
    """recursively remove empty lists, empty dicts, or None elements from a dictionary
    ref: https://gist.github.com/nlohmann/c899442d8126917946580e7f84bf7ee7
    """

    def empty(x):
        return x is None or x == {} or x == []

    if not isinstance(d, (dict, list)):
        return d
    elif isinstance(d, list):
        return [v for v in (remove_empty_elements(v) for v in d) if not empty(v)]
    else:
        return {k: v for k, v in ((k, remove_empty_elements(v)) for k, v in d.items()) if not empty(v)}

def drop_null_columns(df, min_none=0):
    """
    This function drops all columns which contain null values.
    :param df: A PySpark DataFrame
    """
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[
        0].asDict()
    to_drop = [k for k, v in null_counts.items() if v > min_none]
    df = df.drop(*to_drop)
    return df.columns





 # without_null_columns = [when(col(c).isNotNull(), col(c)).otherwise(F.lit("")).alias(c) for c in df_otherNames.columns if c not in {'studentUniqueId','Operation'}]