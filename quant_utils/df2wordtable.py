import numpy as np
import pandas as pd
from docx import Document, oxml
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Inches, Pt, RGBColor


class DF2WordTable:
    def __infi__(self):
        pass

    def set_table_boarder(self, table, **kwargs):
        """
        Set table`s border
        Usage:
        set_table_border(
            cell,
            top={"sz": 12, "val": "single", "color": "#FF0000"},
            bottom={"sz": 12, "color": "#00FF00", "val": "single"},
            left={"sz": 24, "val": "dashed"},
            right={"sz": 12, "val": "dashed"},
        )
        """
        borders = OxmlElement("w:tblBorders")
        for tag in ("bottom", "top", "left", "right", "insideV", "insideH"):
            edge_data = kwargs.get(tag)
            if edge_data:
                any_border = OxmlElement(f"w:{tag}")
                for key in ["sz", "val", "color", "space", "shadow"]:
                    if key in edge_data:
                        any_border.set(qn(f"w:{key}"), str(edge_data[key]))
                borders.append(any_border)
                table._tbl.tblPr.append(borders)
        return table

    def set_table_singleBoard(self, table):
        """为表格添加边框"""
        return self.set_table_boarder(
            table,
            top={"sz": 4, "val": "single", "color": "#000000"},
            bottom={"sz": 4, "val": "single", "color": "#000000"},
            left={"sz": 4, "val": "single", "color": "#000000"},
            right={"sz": 4, "val": "single", "color": "#000000"},
            insideV={"sz": 4, "val": "single", "color": "#000000"},
            insideH={"sz": 4, "val": "single", "color": "#000000"},
        )

    def convert_df_to_table(
        self, document, dataframe: pd.DataFrame, index_list=None, column_list=None
    ):
        """把table转为dataframe
        :param document: 文档对象
        :param dataframe: dataframe格式数据
        :param index_list: 最左边一列显示的内容
        :param column_list: （第一行）列名称需要显示的内容
        """
        rows = dataframe.shape[0]
        cols = dataframe.shape[1]
        if index_list is not None:
            cols += 1
        if column_list is not None:
            rows += 1
        table = document.add_table(rows=rows, cols=cols)
        row_i = 0
        col_i = 0
        if index_list is not None:
            raise
        if column_list is not None:
            hdr_cells = table.rows[row_i].cells
            for _col_i, _v in enumerate(column_list):
                hdr_cells[_col_i].text = str(_v)
            row_i += 1
        for _i, series_info in enumerate(dataframe.iterrows()):
            series = series_info[1]
            hdr_cells = table.rows[row_i + _i].cells
            for _c_i, _cell_value in enumerate(series):
                if isinstance(_cell_value, float):
                    if np.isnan(_cell_value):
                        hdr_cells[col_i + _c_i].text = ""
                    else:
                        hdr_cells[col_i + _c_i].text = f"{_cell_value:.2f}"
                else:
                    hdr_cells[col_i + _c_i].text = str(_cell_value)
        return table

    def main(self, doc: Document, df: pd.DataFrame):
        table = self.convert_df_to_table(doc, df, column_list=df.columns.tolist())
        table = self.set_table_singleBoard(table)  # 表格添加边框
        return table
