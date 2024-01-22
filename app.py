from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import FollowEvent, UnfollowEvent, JoinEvent, MemberLeftEvent
import configparser
import pandas as pd

# key and token set
path = '/home/shenyun/python/config.cfg'
config = configparser.ConfigParser()
config.read(path)
key_info = config["API_KEY"]
LINE_BOT = key_info['LINE_BOT']
LINE_CHANNEL_SECRET = key_info['LINE_CHANNEL_SECRET']

# get current user id & group id
user = pd.read_csv('/home/shenyun/python/user_list.csv',encoding='utf-8')
user_list = user['UserId'].values.tolist()
group = pd.read_csv('/home/shenyun/python/group_list.csv',encoding='utf-8')
group_list = group['GroupId'].values.tolist()

app = Flask(__name__)

# api
line_bot_api = LineBotApi(LINE_BOT)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# set Webhook URL
webhook_url_path = "/callback"

# define Webhook route
@app.route(webhook_url_path, methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'

# FollowEvent: get new user id
@handler.add(FollowEvent)
def handle_follow(event):
    user_id = event.source.user_id
    print(f"有使用者 {user_id} 加入 LINE BOT 作為好友")
    user_list.append(user_id)
    df_user2 = pd.DataFrame(list(set(user_list)), columns=['UserId'])
    df_user2.to_csv('/home/shenyun/python/user_list.csv',encoding='utf-8',index=False)

'''
# 處理 UnfollowEvent
@handler.add(UnfollowEvent)
def handle_unfollow(event):
    user_id = event.source.user_id
    print(f"有使用者 {user_id} 移除 LINE BOT 作為好友")
'''

# JoinEvent: get group id
@handler.add(JoinEvent)
def handle_join(event):
    group_id = event.source.group_id
    print(f"有群組 {group_id} 加入 LINE BOT ")
    group_list.append(group_id)
    df_group2 = pd.DataFrame(list(set(group_list)), columns=['GroupId'])
    df_group2.to_csv('/home/shenyun/python/group_list.csv',encoding='utf-8',index=False)

'''
# 處理 UnjoinEvent
@handler.add(MemberLeftEvent)
def handle_unjoin(event):
    group_id = event.source.group_id
    print(f"有群組 {group_id} 移除 LINE BOT")
'''

if __name__ == "__main__":
    # 啟動 Flask 應用程式
    app.run(port=5000)