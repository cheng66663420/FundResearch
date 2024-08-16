import base64
import hashlib
import io
import json
import time

import requests
from PIL import Image

ROBOT_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=395ae222-24cd-4f07-bb76-4303dd959823"


def get_picture_base64_data(image_path, fmt="PNG"):
    img = Image.open(image_path).convert("RGB")
    output = io.BytesIO()
    img.save(output, format=fmt)
    byte_data = output.getvalue()
    base64_str = base64.b64encode(byte_data).decode("utf-8")
    return base64_str


def get_picture_md5(image_path, fmt="PNG"):
    img = Image.open(image_path).convert("RGB")
    output = io.BytesIO()
    img.save(output, format=fmt)
    byte_data = output.getvalue()
    md = hashlib.md5()
    md.update(byte_data)
    return md.hexdigest()


class WxWrapper:
    MOBILE_LIST = {
        "陆天琦": "13636500170",
        "陈天成": "18521792057",
        "陈恺寅": "18621020328",
        "陈娇君": "13482511910",
        "程钰杨": "17601228768",
        "魏立": "13774239907",
    }

    def __init__(self, webhook_url=ROBOT_URL):
        """
        初始化企业微信机器人客户端。

        :param webhook_url: 企业微信机器人的Webhook地址
        """
        self.webhook_url = webhook_url

    def get_mentioned_moble_list_by_name(
        self, mentioned_name: str | list = None
    ) -> list[str]:
        """
        根据名字获取提醒手机号

        Parameters
        ----------
        mentioned_name : str | list, optional
            提醒人的姓名字符串或者列表, by default None

        Returns
        -------
        list[str]
            提醒人的手机号列表
        """
        if mentioned_name is None:
            return []
        if isinstance(mentioned_name, str):
            return [self.MOBILE_LIST[mentioned_name]]
        if isinstance(mentioned_name, list):
            return [self.MOBILE_LIST[name] for name in mentioned_name]

    def send_text(
        self,
        content: str,
        mentioned_list: list = None,
        mentioned_mobile_list: list = None,
        mention_all: bool = False,
    ):
        """
        发送文本消息。

        Parameters
        ----------
        content : str
            文本内容
        mentioned_list : list, optional
            提醒ID列表, by default None
        mentioned_mobile_list : list, optional
            提醒手机号列表, by default None
        mention_all : bool, optional
            提醒全部人, by default False
        """
        data = {
            "msgtype": "text",
            "text": {
                "content": content,
                "mentioned_list": mentioned_list or [],
                "mentioned_mobile_list": mentioned_mobile_list,
                "mention_all": mention_all,
            },
        }
        self._send(data)

    def send_image(
        self,
        image_path: str,
        mentioned_list: list = None,
        mentioned_mobile_list: list = None,
        mention_all: list = False,
    ):
        """
        发送图片消息。

        Parameters
        ----------
        image_path : _type_
            图片路径
        mentioned_list : list, optional
            提醒ID列表, by default None
        mentioned_mobile_list : list, optional
            提醒手机号列表, by default None
        mention_all : bool, optional
            提醒全部人, by default False
        """
        pic_base64 = get_picture_base64_data(image_path)
        pic_md5 = get_picture_md5(image_path)
        data = {
            "msgtype": "image",
            "image": {"base64": pic_base64, "md5": pic_md5},
            "mentioned_list": mentioned_list,
            "mentioned_mobile_list": mentioned_mobile_list,
            "mention_all": mention_all,
        }
        self._send(data)

    def upload_file(self, file_path: str, file_type: str = "file"):
        upload_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key=395ae222-24cd-4f07-bb76-4303dd959823&type={file_type}"
        with open(file_path, "rb") as file:
            files = {"file": file}
            response = requests.post(upload_url, files=files)
            response_json = response.json()
            if response_json.get("errcode") == 0:
                return response_json["media_id"]
            else:
                print(
                    f"文件上传失败，错误码：{response_json['errcode']}, 错误信息：{response_json['errmsg']}"
                )
                return None

    def send_file(
        self,
        file_path: str,
        mentioned_list: list = None,
        mentioned_mobile_list: list = None,
        mention_all: bool = False,
    ):
        media_id = self.upload_file(file_path)
        if media_id is not None:
            data = {
                "msgtype": "file",
                "file": {"media_id": media_id},
                "mentioned_list": mentioned_list,
                "mentioned_mobile_list": mentioned_mobile_list,
                "mention_all": mention_all,
            }
            self._send(data)
        else:
            print("文件上传失败")

    def _send(self, data):
        """
        发送消息的内部方法。

        :param data: 消息数据
        """
        time.sleep(1)
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            self.webhook_url, headers=headers, data=json.dumps(data)
        )

        if response.status_code == 200:
            print("消息发送成功")
        else:
            print(
                f"消息发送失败，响应码：{response.status_code}, 响应内容：{response.text}"
            )
        time.sleep(1)


if __name__ == "__main__":

    # mention_moble = [val for _, val in moblie_dict.items()]
    robot = WxWrapper()
