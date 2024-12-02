import polars as pl


def rank_pct(
    rank_col: str, patition_by: str | list = None, descending: bool = True
) -> pl.Expr:
    """
    计算百分位排名

    Parameters
    ----------
    rank_col : str
        排名列
    patition_by : str | list, optional
        分组列, by default None
    descending : bool, optional
        是否降序, by default True

    Returns
    -------
    pl.Expr
        百分位排名
    """
    rank_expr = pl.col(rank_col).rank(descending=descending).cast(pl.UInt32)
    count_expr = pl.col(rank_col).count().cast(pl.UInt32)
    return 100 * ((rank_expr - 1) / (count_expr - 1)).over(patition_by)


def rank_str(
    rank_col: str, patition_by: str | list = None, descending: bool = True
) -> pl.Expr:
    """
    计算百分位排名

    Parameters
    ----------
    rank_col : str
        排名列
    patition_by : str | list, optional
        分组列, by default None
    descending : bool, optional
            是否降序, by default True

    Returns
    -------
    pl.Expr
        百分位排名
    """
    rank_expr = pl.col(rank_col).rank(descending=descending).cast(pl.UInt32)
    count_expr = pl.col(rank_col).count().cast(pl.UInt32)
    return (
        rank_expr.cast(pl.String).over(patition_by)
        + "/"
        + count_expr.cast(pl.String).over(patition_by)
    )
