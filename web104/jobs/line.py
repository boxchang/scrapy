from linebot import LineBotApi
from linebot.models import TextSendMessage

def callBoxLine(message):
    line_bot_api = LineBotApi('1NCFjtzWjkieeW0FPyF0DUzwLtJ+kfd+rtoqFRygSlPaN8NItmXbXLfXtcvIFfLGWOClsROt8sM12WF2MlKPi+5Au8r02oEYhfMst6i1/JD49uVUK3pvyjgymAHKp5nK6rrBaGoyPeyLPTzr3yR22AdB04t89/1O/w1cDnyilFU=')
    #push message to one user
    line_bot_api.push_message('U1b5568ddb39e914c141e2bf5768601c3',
        TextSendMessage(text=message))
    #push message to multiple users
    # line_bot_api.multicast(['U1b5568ddb39e914c141e2bf5768601c3', 'user_id2'],
    #     TextSendMessage(text='Hello World!'))