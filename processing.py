'''
Basic functions to run data processing before ingesting into influx.
'''


def constant_val_filter(df, col, lower=None, upper=None):
    '''
    Will mark col tag_approved as 'yes' or 'no' if vals fall within upper
    or lower.
    NOTE: It should really have a default, not just assume 'yes' for other vals.

    Parameters
    ----------
    df : pd.DataFrame
    col : str
        Col on which the comparison to the upper and lower is made.
    lower : np.float or None
        If vals are below this they approved will be 'no'
    upper : np.float or None
        If vals are above this they approved will be 'no'

    Returns
    -------
    pd.DataFrame
    '''

    assert ((lower is not None) or (upper is not None)), "You must filter either upper or lower bounds"
    # Assume that all data points will be approved or not (not none)
    df.loc[df['tag_approved'] == 'none', 'tag_approved'] = 'yes'
    if upper is not None:
        df.loc[df[col] > upper, 'tag_approved'] = 'no'
    if lower is not None:
        df.loc[df[col] < lower, 'tag_approved'] = 'no'

    return df
