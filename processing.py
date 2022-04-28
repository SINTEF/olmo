'''
Basic functions to run data processing before ingesting into influx.
'''


def constant_val_filter(df, col, lower=None, upper=None):

    assert ((lower is not None) or (upper is not None)), "You must filter either upper or lower bounds"
    # Assume that all data points will be approved or not (not none)
    df.loc[df['tag_approved'] == 'none', 'tag_approved'] = 'yes'
    if upper is not None:
        df.loc[df[col] > upper, 'tag_approved'] = 'no'
    if lower is not None:
        df.loc[df[col] < lower, 'tag_approved'] = 'no'

    return df
