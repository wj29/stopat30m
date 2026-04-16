"""Notification channel sender implementations."""

from stopat30m.notification.senders.astrbot import AstrbotSender
from stopat30m.notification.senders.custom_webhook import CustomWebhookSender
from stopat30m.notification.senders.discord import DiscordSender
from stopat30m.notification.senders.email import EmailSender
from stopat30m.notification.senders.feishu import FeishuSender
from stopat30m.notification.senders.pushover import PushoverSender
from stopat30m.notification.senders.pushplus import PushplusSender
from stopat30m.notification.senders.serverchan3 import Serverchan3Sender
from stopat30m.notification.senders.slack import SlackSender
from stopat30m.notification.senders.telegram import TelegramSender
from stopat30m.notification.senders.wechat import WECHAT_IMAGE_MAX_BYTES, WechatSender

__all__ = [
    "AstrbotSender",
    "CustomWebhookSender",
    "DiscordSender",
    "EmailSender",
    "FeishuSender",
    "PushoverSender",
    "PushplusSender",
    "Serverchan3Sender",
    "SlackSender",
    "TelegramSender",
    "WECHAT_IMAGE_MAX_BYTES",
    "WechatSender",
]
