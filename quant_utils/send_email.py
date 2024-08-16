import os
import smtplib
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from quant_utils.constant import EMAIL_CONFIG


class MailSender:
    def __init__(
        self,
        email_fig: dict = EMAIL_CONFIG,
    ) -> None:

        self.email_config = email_fig

    def message_config(
        self, from_name: str, subject: str, file_path: str, content: str = ""
    ) -> None:  # sourcery skip: class-extract-method
        """
        邮件配置信息

        Parameters
        ----------
        from_name : str
            发件人姓名
        subject : str
            邮件标题
        file_path : str
            附件内容地址
        content : str
            邮件正文
        """
        body = MIMEText(content, "plain", "utf-8")
        self.message = MIMEMultipart()  # 多个MIME对象
        self.message.attach(body)  # 添加内容
        self.message["From"] = Header(from_name)  # 发件人

        self.message["Subject"] = Header(subject, "utf-8")  # 主题

        # 添加Excel类型附件
        attended_file_name = file_path  # 文件名
        attended_file_path = os.path.join(attended_file_name)  # 文件路径
        (filepath, tempfilename) = os.path.split(file_path)

        xlsx = MIMEApplication(
            open(attended_file_path, "rb").read()
        )  # 打开Excel,读取Excel文件
        xlsx["Content-Type"] = "application/octet-stream"  # 设置内容类型

        xlsx.add_header(
            "Content-Disposition", "attachment", filename=tempfilename
        )  # 添加到header信息
        self.message.attach(xlsx)

    def send_mail(self, receivers: list = None) -> None:
        """
        发送邮件

        Parameters
        ----------
        receivers : list, optional
            收件人列表, by default None
        """
        if receivers is None:
            receivers = self.email_config["receivers"]

        try:
            smtpObj = smtplib.SMTP_SSL(
                self.email_config["mail_host"]
            )  # 使用SSL连接邮箱服务器
            # 登录服务器
            smtpObj.login(
                self.email_config["mail_user"], self.email_config["mail_pass"]
            )
            # 发送邮件
            smtpObj.sendmail(
                self.email_config["sender"],
                receivers,
                self.message.as_string(),
            )
            print("邮件发送成功")
        except Exception as e:
            print(e)

    def message_config_dataframe(
        self, from_name: str, subject: str, df_to_mail: str = "", file_path: str = None
    ) -> None:
        """
        邮件配置信息

        Parameters
        ----------
        from_name : str
            发件人姓名
        subject : str
            邮件标题
        file_path : str
            附件内容地址
        content : str
            邮件正文
        """
        df_html = df_to_mail.to_html()
        body = MIMEText(df_html, "html", "utf-8")
        self.message = MIMEMultipart()  # 多个MIME对象
        self.message.attach(body)  # 添加内容
        self.message["From"] = Header(from_name)  # 发件人

        self.message["Subject"] = Header(subject, "utf-8")  # 主题
        # 添加Excel类型附件
        if file_path:
            attended_file_name = file_path  # 文件名
            attended_file_path = os.path.join(attended_file_name)  # 文件路径
            (filepath, tempfilename) = os.path.split(file_path)

            xlsx = MIMEApplication(
                open(attended_file_path, "rb").read()
            )  # 打开Excel,读取Excel文件
            xlsx["Content-Type"] = "application/octet-stream"  # 设置内容类型

            xlsx.add_header(
                "Content-Disposition", "attachment", filename=tempfilename
            )  # 添加到header信息
            self.message.attach(xlsx)

    def message_config_content(
        self, from_name: str, subject: str, content: str = ""
    ) -> None:
        """
        邮件配置信息

        Parameters
        ----------
        from_name : str
            发件人姓名
        subject : str
            邮件标题
        file_path : str
            附件内容地址
        content : str
            邮件正文
        """
        body = MIMEText(content, "plain", "utf-8")
        self.message = MIMEMultipart()  # 多个MIME对象
        self.message.attach(body)  # 添加内容
        self.message["From"] = Header(from_name)  # 发件人
        self.message["Subject"] = Header(subject, "utf-8")  # 主题


if __name__ == "__main__":
    mail_sender = MailSender()
    mail_sender.message_config_content(from_name="chentiancheng", subject="测试邮件")
    mail_sender.send_mail(receivers=["569253615@qq.com"])
