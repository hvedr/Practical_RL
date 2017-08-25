# encoding=utf-8

from __future__ import print_function
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase


class MyNotification:
    """
    Отправляем отчеты по e-mail.
    """

    def __init__(self, login, encrypted_password, mail_from, smtp='smtp.tcsbank.ru'):
        """
        :param string login: Логин.
        :param byte_string encrypted_password: Пароль, зашифрованный base64.
        :param string mail_from: От кого письмо.
        :param string smtp: SMTP-сервер.
        :return:
        """

        import base64

        self.login = login
        self.password = base64.b64decode(encrypted_password).decode('utf-8')
        self.mail_from = mail_from
        self.smtp = smtp

    def sent_letter_with_file(self, mail_to, subject='Test', html_content='', attachment=None):
        """
        Отправляем письмо с результатами
        -------------------------------------------------------
        Аргументы:
           mail_to : адреса получателей
           subject: Тема письма
           html_content: Тело письма в формате HTML
           attachment: Название файла с результатами отчета
               для вложения к письму
        -------------------------------------------------------
        Пример:

        mail_to = ["a.golova@tinkoff.ru"]
        html_text = '''
        <html>
         <head></head>
         <body>
           <p>Hi!<br>
              How are you?<br>
           </p>
         </body>
        </html>
        '''
       -------------------------------------------------------
       """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.mail_from
        msg['To'] = ", ".join(mail_to)

        # Задаем вложение с результатом отчета
        if attachment:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(attachment, "rb").read())
            part.add_header('Content-Disposition', 'attachment; filename="%s"'
                            % os.path.basename(attachment))
            msg.attach(part)

        # Формируем тело письма
        part2 = MIMEText(html_content, 'html')
        msg.attach(part2)

        # Отправляем
        s = smtplib.SMTP(self.smtp)
        s.login(self.login, self.password)
        s.sendmail(self.mail_from, mail_to, msg.as_string())
        s.quit()

        return


if __name__ == '__main__':

    # создаем тестовый файл
    import datetime

    filename = 'report' + str(datetime.datetime.now().date()) + '.txt'
    file = open(filename, 'tw')
    for i in range(10):
        print(str(i), file=file)
    file.close()

    # создаем текст письма
    html_text = """
    <html>
        <head>Тестовый отчет</head>
        <body>
            <p>
                Привет!<br>
                This is a test report!<br>
            </p>
        </body>
    </html>
    """

    # посылаем письмо
    notification = MyNotification(login='d.m.korzhenkov',
                                  encrypted_password=b'Sm9sRlRXWjEy',
                                  mail_from='d.m.korzhenkov@tinkoff.ru'
                                  )
    notification.sent_letter_with_file(mail_to=['d.m.korzhenkov@tinkoff.ru'], subject='Test Report',
                                       html_content=html_text, attachment=filename
                                       )
