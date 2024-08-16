import os
import time
from typing import Union

import numpy as np
import pandas as pd
import pythoncom
import xlwings as xw
from PIL import ImageGrab


def get_column_letter(column_number):
    if column_number < 1:
        raise ValueError("Column index must be greater than or equal to 1.")

    column_label = ""
    while column_number > 0:
        column_number, remainder = divmod(column_number - 1, 26)
        column_label = chr(65 + remainder) + column_label
    return column_label


class ExcelWrapper:
    """
    Wrapper class for xlwings
    """

    def __init__(
        self, filename: str = None, visible: bool = False, sheet_name: str | int = 0
    ):

        pythoncom.CoInitialize()
        self.app = xw.App(visible=visible, add_book=False)
        self.app.display_alerts = False  # 不显示警告信息
        self.app.screen_updating = False  # 关闭屏幕更新
        # 如果没有提供filepath，则新建工作簿
        if filename:
            self.workbook = self.app.books.open(filename)
        else:
            self.workbook = self.app.books.add()
        # 假设默认选择Sheet1
        self.select_sheet(sheet_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def used_range(self) -> xw.Range:
        return self.worksheet.used_range

    @property
    def shape(self) -> tuple:
        return self.used_range.shape

    def select_sheet(self, sheet_name: Union[str, int] = 0):
        self.worksheet = self.workbook.sheets[sheet_name]

    def get_all_sheets(self):
        return self.workbook.sheets

    def clear_sheet(self):
        self.worksheet.clear()

    def save(self, filename=None):
        if filename:
            self.workbook.save(filename)
        else:
            self.workbook.save()

    def close(self):
        if self.workbook:
            self.workbook.close()
        if self.app:
            self.app.quit()
        pythoncom.CoUninitialize()

    def select_range(
        self,
        range_address: str = None,
        start_row: int = None,
        end_row: int = None,
        start_column: int = None,
        end_column: int = None,
        expand: str = None,
    ) -> xw.Range:
        """
        选取excel中得单元格
        (1)如果range_address不为空，则直接选取range_address，
        (2)否则选取start_row,end_row,start_column,end_column
        如果四个参数都不为空，则选取start_row,end_row,start_column,end_column
        (3)如果start_row, start_column都不为空，则选取start_row,start_column

        Parameters
        ----------
        range_address : str, optional
            range的地址,可以是A1也可以是A1:C3, by default None
        start_row : int, optional
            开始的行, by default None
        end_row : int, optional
            结束的行, by default None
        start_column : int, optional
            开始的列, by default None
        end_column : int, optional
            结束的列, by default None
        expand : str, optional
            扩展方式, by default None

        Returns
        -------
        xw.Range
            返回一个xlwings.Range对象，可以进行操作
        """
        range_obj = None
        if range_address:
            range_obj = self.worksheet.range(range_address)

        if start_row and end_row and start_column and end_column:
            range_obj = self.worksheet.range(
                f"{get_column_letter(start_column)}{start_row}:{get_column_letter(end_column)}{end_row}"
            )

        if start_column and start_row:
            range_obj = self.worksheet.range(
                f"{get_column_letter(start_column)}{start_row}"
            )

        if range_obj is None:
            raise ValueError("Invalid range parameters")

        if expand is not None:
            range_obj = range_obj.expand(expand)
        return range_obj

    def get_data(
        self,
        range_address: str = None,
        start_row: int = None,
        end_row: int = None,
        start_column: int = None,
        end_column: int = None,
        convert: object = None,
        **options,
    ):
        """
        选取excel中得单元格的值
        (1)如果range_address不为空，则直接选取range_address，
        (2)否则选取start_row,end_row,start_column,end_column
        如果四个参数都不为空，则选取start_row,end_row,start_column,end_column
        (3)如果start_row, start_column都不为空，则选取start_row,start_column

        Parameters
        ----------
        range_address : str, optional
            range的地址,可以是A1也可以是A1:C3, by default None
        start_row : int, optional
            开始的行, by default None
        end_row : int, optional
            结束的行, by default None
        start_column : int, optional
            开始的列, by default None
        end_column : int, optional
            结束的列, by default None

        Keyword Arguments
        -----------------
        ndim : int, default None
            number of dimensions

        numbers : type, default None
            type of numbers, e.g. ``int``

        dates : type, default None
            e.g. ``datetime.date`` defaults to ``datetime.datetime``

        empty : object, default None
            transformation of empty cells

        transpose : Boolean, default False
            transpose values

        expand : str, default None
            One of ``'table'``, ``'down'``, ``'right'``

        chunksize : int
            Use a chunksize, e.g. ``10000`` to prevent timeout or memory issues when
            reading or writing large amounts of data. Works with all formats, including
            DataFrames, NumPy arrays, and list of lists.

        err_to_str : Boolean, default False
            If ``True``, will include cell errors such as ``#N/A`` as strings. By
            default, they will be converted to ``None``.
        Returns
        -------
        object
            单元格的值
        """
        range_obj = self.select_range(
            range_address=range_address,
            start_row=start_row,
            end_row=end_row,
            start_column=start_column,
            end_column=end_column,
        )
        return range_obj.options(convert=convert, **options).value

    def write_data(
        self,
        data: Union[str, list, np.array],
        start_row: int = 1,
        start_column: int = 1,
        orientation: str = "row",
    ):
        """
        将数据按照行写入excel中

        Parameters
        ----------
        data : Union[str, list, np.array]
            需要写入的数据,可以是str 或者list或者numpy.ndarray
        start_row : int, optional
            起始的行, by default 1
        start_column : int, optional
            起始的列, by default 1
        orientation : str, optional
            写入的方式,可以是'row'或者'column', by default 'row'
        """
        if isinstance(data, str):
            data = [data]
        if not isinstance(data, (str, list, np.ndarray)):
            raise ValueError("array_data must be a str or a list or a numpy.ndarray.")

        range_obj = self.select_range(start_row=start_row, start_column=start_column)
        data = np.array(data)
        ndim = data.ndim
        if ndim > 2:
            raise ValueError("data must be a 1D or 2D array.")

        if orientation not in ["row", "column"]:
            raise ValueError("orientation must be 'row' or 'column'.")

        if orientation == "row":
            range_obj.value = data
        else:
            if ndim == 1:
                range_obj.options(transpose=True).value = data
            else:
                range_obj.options(transpose=True).value = data.T

    def write_dataframe(
        self,
        df: pd.DataFrame,
        start_row: int = 1,
        start_column: int = 1,
        if_write_index: bool = False,
        if_write_header: bool = False,
    ):
        """
        将DataFrame写入Excel,默认从A1开始写入,不写入index与 header

        Parameters
        ----------
        df : pd.DataFrame
            要写入的DataFrame
        start_row : int, optional
            开始写入的行, by default 1
        start_column : int, optional
            开始写入的列, by default 1
        if_write_index : bool, optional
            是否写入索引, by default False
        if_write_header : bool, optional
            是否写入列名, by default False
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("df must be a pandas.DataFrame.")
        range_obj = self.select_range(start_row=start_row, start_column=start_column)
        range_obj.options(index=if_write_index, header=if_write_header).value = df

    def set_cell_style(
        self,
        range_obj: xw.Range,
        if_default: bool = False,
        **kwargs,
    ):
        """
        设置单元格样式,如果使用默认模版

        Parameters
        ----------
        range_obj : xw.Range
            要设置样式的单元格范围
        if_default : bool, optional
            是否使用默认样式, by default False
        Keyword Arguments
        -----------------
        font_name : str, optional
            字体名称, by default None
            可选项: Arial, Calibri, 微软雅黑, ....
            默认Arial
        font_size : int, optional
            字体大小, by default None
            默认12
        font_color : str or tuple, optional
            字体颜色, by default None
            默认#000000
        bold : bool, optional
            是否加粗, by default None
            默认False
        italic : bool, optional
            是否斜体, by default None
            默认False
        text_wrap : bool, optional
            是否自动换行, by default None
            默认False
        bg_color : str or tuple, optional
            背景颜色, by default None
            默认#FFFFFF
        horizontal_alignment : str, optional
            水平对齐方式, by default None
            可选项: Left, Center, Right, Fill, Justify, CenterContinuous, Distributed
            默认Center
        vertical_alignment : str, optional
            垂直对齐方式, by default None
            可选项: Top, Center, Bottom, Justify, Distributed
            默认Center
        number_format : str, optional
            数字格式, by default None
            可选项: General, 0, 0.00,.....
            默认General

        Examples
        --------
        >>> from excel_wrapper import ExcelWrapper
        >>> excel_wrapper = ExcelWrapper()
        >>> excel_wrapper.set_cell_style(
            range_address="A1:C3",
            font_name="Arial",
            font_size=14,
            font_color="#FF0000",
            bold=True,
            italic=True,
            text_wrap=True,
            bg_color="#FFFF00",
            horizontal_alignment="Center",
            vertical_alignment="Center",
            number_format="0.00%"
        )
        """
        # kwargs的默认值
        valid_props = {
            "font_name": ("name", str, "Arial"),
            "font_size": ("size", int, 12),
            "font_color": ("color", Union[str, tuple], "#000000"),
            "bold": ("bold", bool, False),
            "italic": ("italic", bool, False),
            "text_wrap": ("WrapText", bool, False),
            "bg_color": ("color", Union[str, tuple], "#FFFFFF"),
            "horizontal_alignment": (
                "HorizontalAlignment",
                xw.constants.HAlign,
                "Center",
            ),
            "vertical_alignment": ("VerticalAlignment", xw.constants.VAlign, "Center"),
            "number_format": ("NumberFormat", str, "General"),
        }
        # 检查参数是否合法
        for key in kwargs:
            if key not in valid_props:
                raise ValueError(f"Invalid keyword argument: {key}")

        font = range_obj.font
        # font字段
        font_key_list = [
            "font_name",
            "font_size",
            "font_color",
            "bold",
            "italic",
        ]
        # 对齐字段
        alignment_list = [
            "horizontal_alignment",
            "vertical_alignment",
        ]
        # 如果需要设置字体样式,则需要设置字体样式
        for key, (attr_name, attr_type, default_value) in valid_props.items():

            if if_default:
                value = kwargs[key] if key in kwargs else default_value

            else:

                if all(kwarg is None for kwarg in kwargs):
                    raise ValueError("At least one property must be specified.")

                if key not in kwargs:
                    continue
                value = kwargs[key]

            if key in font_key_list:
                setattr(font, attr_name, value)
            elif key == "bg_color":
                setattr(range_obj, attr_name, value)
            elif key in alignment_list:
                setattr(
                    range_obj.api,
                    attr_name,
                    getattr(attr_type, f"xl{attr_type.__name__}{value}"),
                )
            else:
                setattr(range_obj.api, attr_name, value)

    def rename_sheet(self, sheet_name: str):
        self.worksheet.name = sheet_name

    def autofit(self):
        self.worksheet.autofit()

    def format_painter(
        self,
        source_range: xw.Range,
        target_range: xw.Range,
    ):
        source_range.api.Copy()
        target_range.api.PasteSpecial(-4122)

    def set_border(
        self,
        range_obj: xw.Range,
        border_position: str = None,
        border_style: str = "continuous",
        border_weight: str = "medium",
        border_color: str = "#000000",
    ):
        """
        设置单元格边框

        Parameters
        ----------
        range_obj : xw.Range
            范围
        border_position : str, optional
            边框位置, by default None
        border_style : str, optional
            边框风格, by default "continuous"
        border_weight : str, optional
            边框粗细, by default "medium"
        border_color : str, optional
            边框颜色, by default "#000000"
        """
        border_positions = {
            "down": xw.constants.BordersIndex.xlDiagonalDown,
            "up": xw.constants.BordersIndex.xlDiagonalUp,
            "bottom": xw.constants.BordersIndex.xlEdgeBottom,
            "left": xw.constants.BordersIndex.xlEdgeLeft,
            "right": xw.constants.BordersIndex.xlEdgeRight,
            "top": xw.constants.BordersIndex.xlEdgeTop,
            "horizontal": xw.constants.BordersIndex.xlInsideHorizontal,
            "vertical": xw.constants.BordersIndex.xlInsideVertical,
        }
        border_weights = {
            "medium": xw.constants.BorderWeight.xlMedium,
            "thin": xw.constants.BorderWeight.xlThin,
            "hairline": xw.constants.BorderWeight.xlHairline,
            "thick": xw.constants.BorderWeight.xlThick,
        }
        border_styles = {
            "continuous": xw.constants.LineStyle.xlContinuous,
            "dash": xw.constants.LineStyle.xlDash,
            "dashdot": xw.constants.LineStyle.xlDashDot,
            "double": xw.constants.LineStyle.xlDouble,
        }
        # 如果需要设置边框样式,则需要设置边框样式
        if border_position is not None:
            range_obj.api.Borders(border_positions[border_position]).LineStyle = (
                border_styles[border_style]
            )
            range_obj.api.Borders(border_positions[border_position]).Weight = (
                border_weights[border_weight]
            )
            range_obj.api.Borders(border_positions[border_position]).Color = (
                border_color
            )

        else:
            for position in [
                "bottom",
                "top",
                "left",
                "right",
                "horizontal",
                "vertical",
            ]:
                range_obj.api.Borders(border_positions[position]).LineStyle = (
                    border_styles[border_style]
                )
                range_obj.api.Borders(border_positions[position]).Weight = (
                    border_weights[border_weight]
                )
                range_obj.api.Borders(border_positions[position]).Color = border_color

    def save_as_image(self, image_path):
        # 保存为图片
        range_obj = self.used_range
        range_obj.copy_picture()
        time.sleep(3)
        image = ImageGrab.grabclipboard()
        image.save(image_path)


if __name__ == "__main__":

    print(get_column_letter(26))
