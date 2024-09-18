import os

import emoji
import pandas as pd
from docx.shared import Pt, RGBColor
from PIL import Image, ImageDraw, ImageFont

from quant_utils.constant_varialbles import TODAY
from quant_utils.utils import make_dirs
from wrapper.wx_wrapper import WxWrapper

font = ImageFont.truetype("C:\\Windows\Fonts\\simhei.TTF", 70)
fontbold = ImageFont.truetype("C:\\Windows\Fonts\\simhei.TTF", 60)
import time


class GoodNewsSender:
    def __init__(
        self, trade_time: str = TODAY, min_amount: int = 20, black_list: list = None
    ):
        self.trade_time = trade_time
        self.min_amount = min_amount
        self.black_list = [] if black_list is None else black_list
        self.template_path = "E:/基金投顾自动化/"
        self.data = self._prepare_data()
        self.big_data = self.data.query(f"委托金额 >= {self.min_amount}")
        self.big_data = self.big_data[~self.big_data["组合名称"].isin(self.black_list)]
        self.wx = WxWrapper()

    def _prepare_data(self):
        data = pd.read_excel(
            self.template_path + "基金投顾业务实时数据.xlsx",
            sheet_name="基金投顾客户级明细报表",
            engine="openpyxl",
        )
        # data = data[~data["组合名称"].isin(self.black_list)]
        data["委托时间"] = pd.to_datetime(data["委托时间"]).dt.strftime("%Y%m%d")
        data["委托金额"] = data["委托金额"] / 10000
        data = data[data["委托时间"] == self.trade_time]
        data = data[
            data["交易类型"].isin(
                [
                    "首次签约",
                    "追加投资",
                    "定投",
                    "预约购买",
                    "份额追加投资",
                ]
            )
        ]
        # data = data.query(f"委托金额 >= {self.min_amount}")
        data = (
            data.groupby(by=["分公司", "委托时间", "客户编号", "组合名称"])["委托金额"]
            .sum()
            .reset_index()
            .sort_values(by="委托金额", ascending=False)
        )
        data.index = range(0, len(data))
        return data

    def get_image_file_list(self, file_path):
        return [
            os.path.join(file_path, i)
            for i in os.listdir(file_path)
            if i.endswith(".png")
        ]

    def draw_image(
        self,
        image: Image,
        text: str,
        font: ImageFont.FreeTypeFont,
        fill: RGBColor,
        y: int,
    ):
        """
        绘制文本,文本居中

        Parameters
        ----------
        image_draw : ImageDraw.ImageDraw
            绘制对象
        text : str
            文本内容
        font : ImageFont.FreeTypeFont
            字体
        fill : RGBColor
            文字颜色
        y : int
            文字坐标
        """
        width = image.width
        image_draw = ImageDraw.Draw(image)
        text_bbox = image_draw.textbbox(
            (0, 0),
            text,
            font=font,
        )
        text_width = text_bbox[2] - text_bbox[0]
        image_draw.text(
            ((width - text_width) / 2, y),
            text,
            font=font,
            fill=fill,
        )

    def creat_image(
        self, idx, client_code, company, amount, portfolio_name, trade_time
    ):
        im = Image.open(f"{self.template_path}喜报模板.png")
        jine = f"下单{amount:.0f}万"
        make_dirs(f"{self.template_path}结果/喜报/{trade_time}")
        name = f"{self.template_path}结果/喜报/{trade_time}/{company}-{client_code}-{idx}.png"
        color = RGBColor(237, 203, 131)
        # 基金投顾业务
        self.draw_image(image=im, text="基金投顾业务", font=font, fill=color, y=650)
        # 分公司
        self.draw_image(image=im, text=company, font=font, fill=color, y=760)
        # 金额
        self.draw_image(image=im, text=jine, font=font, fill=color, y=875)
        # 组合
        self.draw_image(
            image=im, text=portfolio_name, font=fontbold, fill=color, y=1000
        )

        im.save(name)

    def draw(self):
        for idx, val in self.big_data.iterrows():
            self.creat_image(
                idx=idx,
                client_code=val["客户编号"],
                company=val["分公司"],
                amount=val["委托金额"],
                portfolio_name=val["组合名称"],
                trade_time=val["委托时间"],
            )

    def send_good_news(self):
        self.draw()
        if not os.path.exists(f"{self.template_path}结果/喜报/{self.trade_time}"):
            image_file_list = []
        else:
            image_file_list = self.get_image_file_list(
                f"{self.template_path}结果/喜报/{self.trade_time}"
            )

        if image_file_list:
            # 发送今天的喜报
            for image_file in image_file_list:
                self.wx.send_image(image_file)
        else:
            print("没有找到图片")
            self.wx.send_text(content="今日无喜报")

        at_person_df = pd.read_excel(
            self.template_path + "艾特名单.xlsx", engine="openpyxl"
        )
        sum_data = self.big_data.groupby("分公司")["委托金额"].sum().reset_index()
        sum_data = sum_data.sort_values(by="委托金额", ascending=False)
        sum_list = []
        for i in sum_data["分公司"]:
            temp = at_person_df.query("分公司 == @i")
            num = self.big_data.query("分公司 == @i").shape[0]
            amout = sum_data.query("分公司 == @i")["委托金额"].values[0]
            sum_str = f'{i}-{num}单-{amout:.2f}万元: @{temp["财富总"].values[0]} @{temp["基金投顾专员"].values[0]}'
            sum_list.append(sum_str)

        sum_str = "\n".join(sum_list)

        # 发送总结
        self.wx.send_text(content=sum_str)

        mention_mobile_list = self.wx.get_mentioned_moble_list_by_name("陆天琦")
        self.wx.send_text(
            content=f"{emoji.emojize('❣')}陆老板,您的喜报来了,请给好评哦!{emoji.emojize('❣')}",
            mentioned_mobile_list=mention_mobile_list,
        )
        sum_amount = self.data["委托金额"].sum()
        sum_list = [
            f"{emoji.emojize('🚀')}基金基金投顾完成销量{sum_amount:.2f}万元,感谢支持!"
        ]
        df = self.data.groupby(by="组合名称")["委托金额"].sum().reset_index()
        df.sort_values(by="委托金额", ascending=False, inplace=True)
        for _, val in df.iterrows():
            str_temp = f"{val['组合名称']}:{val['委托金额']:.2f}万元"
            sum_list.append(str_temp)
        sum_str = "\n".join(sum_list)

        self.wx.send_text(content=sum_str)
        print("喜报发送完成了")


if __name__ == "__main__":
    good_news = GoodNewsSender(min_amount=5, black_list=[])
    good_news.send_good_news()
