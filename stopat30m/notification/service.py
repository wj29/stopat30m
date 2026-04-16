# -*- coding: utf-8 -*-
"""
通知层：报告生成与多渠道推送（从 daily_stock_analysis 迁移并适配 stopat30m 配置）。
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional

from loguru import logger

from stopat30m.config import get as cfg_get
from stopat30m.notification.report_i18n import (
    get_localized_stock_name,
    get_report_labels,
    get_signal_level,
    localize_chip_health,
    localize_operation_advice,
    localize_trend_prediction,
    normalize_report_language,
)
from stopat30m.notification.senders import (
    AstrbotSender,
    CustomWebhookSender,
    DiscordSender,
    EmailSender,
    FeishuSender,
    PushoverSender,
    PushplusSender,
    Serverchan3Sender,
    SlackSender,
    TelegramSender,
    WechatSender,
)
from stopat30m.notification.types import AnalysisResult, normalize_model_used


class NotificationChannel(Enum):
    """通知渠道类型"""

    WECHAT = "wechat"
    FEISHU = "feishu"
    TELEGRAM = "telegram"
    EMAIL = "email"
    PUSHPLUS = "pushplus"
    CUSTOM = "custom"
    DISCORD = "discord"
    SLACK = "slack"
    PUSHOVER = "pushover"
    SERVERCHAN3 = "serverchan3"
    ASTRBOT = "astrbot"
    UNKNOWN = "unknown"


class ChannelDetector:
    """渠道检测器 — 根据配置判断渠道类型。"""

    @staticmethod
    def get_channel_name(channel: NotificationChannel) -> str:
        """获取渠道中文名称"""
        names = {
            NotificationChannel.WECHAT: "企业微信",
            NotificationChannel.FEISHU: "飞书",
            NotificationChannel.TELEGRAM: "Telegram",
            NotificationChannel.EMAIL: "邮件",
            NotificationChannel.PUSHPLUS: "PushPlus",
            NotificationChannel.CUSTOM: "自定义Webhook",
            NotificationChannel.DISCORD: "Discord机器人",
            NotificationChannel.SLACK: "Slack",
            NotificationChannel.PUSHOVER: "Pushover",
            NotificationChannel.SERVERCHAN3: "Server酱3",
            NotificationChannel.ASTRBOT: "ASTRBOT机器人",
            NotificationChannel.UNKNOWN: "未知渠道",
        }
        return names.get(channel, "未知渠道")


class NotificationService(
    AstrbotSender,
    CustomWebhookSender,
    DiscordSender,
    EmailSender,
    FeishuSender,
    PushoverSender,
    PushplusSender,
    Serverchan3Sender,
    SlackSender,
    TelegramSender,
    WechatSender,
):
    """通知服务：生成 Markdown 报告并向已配置渠道推送。"""

    def __init__(self) -> None:
        AstrbotSender.__init__(self)
        CustomWebhookSender.__init__(self)
        DiscordSender.__init__(self)
        EmailSender.__init__(self)
        FeishuSender.__init__(self)
        PushoverSender.__init__(self)
        PushplusSender.__init__(self)
        Serverchan3Sender.__init__(self)
        SlackSender.__init__(self)
        TelegramSender.__init__(self)
        WechatSender.__init__(self)

        self._report_summary_only = bool(cfg_get("notification.report_summary_only", default=False))

        self._available_channels = self._detect_all_channels()
        if not self._available_channels:
            logger.warning("未配置有效的通知渠道，将不发送推送通知")
        else:
            channel_names = [ChannelDetector.get_channel_name(ch) for ch in self._available_channels]
            logger.info(f"已配置 {len(channel_names)} 个通知渠道：{', '.join(channel_names)}")

    def _get_report_language(self, payload: Optional[Any] = None) -> str:
        """Resolve report language from result payload or global config."""
        if isinstance(payload, list):
            for item in payload:
                language = getattr(item, "report_language", None)
                if language:
                    return normalize_report_language(language)
        elif payload is not None:
            language = getattr(payload, "report_language", None)
            if language:
                return normalize_report_language(language)
        return normalize_report_language(cfg_get("notification.report_language", default="zh"))

    def _get_display_name(self, result: AnalysisResult, language: Optional[str] = None) -> str:
        report_language = normalize_report_language(language or self._get_report_language(result))
        return self._escape_md(
            get_localized_stock_name(result.name, result.code, report_language)
        )

    def _detect_all_channels(self) -> List[NotificationChannel]:
        channels: List[NotificationChannel] = []
        if self._wechat_url:
            channels.append(NotificationChannel.WECHAT)
        if self._feishu_url:
            channels.append(NotificationChannel.FEISHU)
        if self._is_telegram_configured():
            channels.append(NotificationChannel.TELEGRAM)
        if self._is_email_configured():
            channels.append(NotificationChannel.EMAIL)
        if self._pushplus_token:
            channels.append(NotificationChannel.PUSHPLUS)
        if self._is_pushover_configured():
            channels.append(NotificationChannel.PUSHOVER)
        if self._serverchan3_sendkey:
            channels.append(NotificationChannel.SERVERCHAN3)
        if self._custom_webhook_urls:
            channels.append(NotificationChannel.CUSTOM)
        if self._is_discord_configured():
            channels.append(NotificationChannel.DISCORD)
        if self._is_slack_configured():
            channels.append(NotificationChannel.SLACK)
        if self._is_astrbot_configured():
            channels.append(NotificationChannel.ASTRBOT)
        return channels

    def is_available(self) -> bool:
        """检查通知服务是否可用（至少有一个渠道）"""
        return len(self._available_channels) > 0

    def get_available_channels(self) -> List[NotificationChannel]:
        """获取所有已配置的渠道"""
        return self._available_channels

    def get_channel_names(self) -> str:
        """获取所有已配置渠道的名称"""
        names = [ChannelDetector.get_channel_name(ch) for ch in self._available_channels]
        return ", ".join(names)

    def generate_aggregate_report(
        self,
        results: List[AnalysisResult],
        report_type: Any,
        report_date: Optional[str] = None,
    ) -> str:
        """Generate the aggregate report content used by merge/save/push paths."""
        normalized = report_type
        if hasattr(normalized, "value"):
            normalized = getattr(normalized, "value", normalized)
        norm_str = str(normalized or "").strip().lower()
        if norm_str == "brief":
            return self.generate_brief_report(results, report_date=report_date)
        return self.generate_dashboard_report(results, report_date=report_date)

    def _collect_models_used(self, results: List[AnalysisResult]) -> List[str]:
        models: List[str] = []
        for result in results:
            model = normalize_model_used(getattr(result, "model_used", None))
            if model:
                models.append(model)
        return list(dict.fromkeys(models))

    def generate_daily_report(
        self,
        results: List[AnalysisResult],
        report_date: Optional[str] = None,
    ) -> str:
        """生成 Markdown 格式的日报（详细版）"""
        if report_date is None:
            report_date = datetime.now().strftime("%Y-%m-%d")
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)

        report_lines = [
            f"# 📅 {report_date} {labels['report_title']}",
            "",
            f"> {labels['analyzed_prefix']} **{len(results)}** {labels['stock_unit']} | "
            f"{labels['generated_at_label']}：{datetime.now().strftime('%H:%M:%S')}",
            "",
            "---",
            "",
        ]

        sorted_results = sorted(
            results,
            key=lambda x: x.sentiment_score,
            reverse=True,
        )

        buy_count = sum(1 for r in results if getattr(r, "decision_type", "") == "buy")
        sell_count = sum(1 for r in results if getattr(r, "decision_type", "") == "sell")
        hold_count = sum(1 for r in results if getattr(r, "decision_type", "") in ("hold", ""))
        avg_score = sum(r.sentiment_score for r in results) / len(results) if results else 0

        report_lines.extend(
            [
                f"## 📊 {labels['summary_heading']}",
                "",
                "| 指标 | 数值 |",
                "|------|------|",
                f"| 🟢 {labels['buy_label']} | **{buy_count}** {labels['stock_unit_compact']} |",
                f"| 🟡 {labels['watch_label']} | **{hold_count}** {labels['stock_unit_compact']} |",
                f"| 🔴 {labels['sell_label']} | **{sell_count}** {labels['stock_unit_compact']} |",
                f"| 📈 {labels['avg_score_label']} | **{avg_score:.1f}** |",
                "",
                "---",
                "",
            ]
        )

        if self._report_summary_only:
            report_lines.extend([f"## 📊 {labels['summary_heading']}", ""])
            for r in sorted_results:
                _, emoji, _ = self._get_signal_level(r)
                report_lines.append(
                    f"{emoji} **{self._get_display_name(r, report_language)}({r.code})**: "
                    f"{localize_operation_advice(r.operation_advice, report_language)} | "
                    f"{labels['score_label']} {r.sentiment_score} | "
                    f"{localize_trend_prediction(r.trend_prediction, report_language)}"
                )
        else:
            report_lines.extend([f"## 📈 {labels['report_title']}", ""])
            for result in sorted_results:
                _, emoji, _ = self._get_signal_level(result)
                confidence_stars = (
                    result.get_confidence_stars()
                    if hasattr(result, "get_confidence_stars")
                    else "⭐⭐"
                )

                report_lines.extend(
                    [
                        f"### {emoji} {self._get_display_name(result, report_language)} ({result.code})",
                        "",
                        f"**{labels['action_advice_label']}：{localize_operation_advice(result.operation_advice, report_language)}** | "
                        f"**{labels['score_label']}：{result.sentiment_score}** | "
                        f"**{labels['trend_label']}：{localize_trend_prediction(result.trend_prediction, report_language)}** | "
                        f"**Confidence：{confidence_stars}**",
                        "",
                    ]
                )

                self._append_market_snapshot(report_lines, result)

                if hasattr(result, "key_points") and result.key_points:
                    report_lines.extend(
                        [
                            f"**🎯 核心看点**：{result.key_points}",
                            "",
                        ]
                    )

                if hasattr(result, "buy_reason") and result.buy_reason:
                    report_lines.extend(
                        [
                            f"**💡 操作理由**：{result.buy_reason}",
                            "",
                        ]
                    )

                if hasattr(result, "trend_analysis") and result.trend_analysis:
                    report_lines.extend(
                        [
                            "#### 📉 走势分析",
                            f"{result.trend_analysis}",
                            "",
                        ]
                    )

                outlook_lines = []
                if hasattr(result, "short_term_outlook") and result.short_term_outlook:
                    outlook_lines.append(f"- **短期（1-3日）**：{result.short_term_outlook}")
                if hasattr(result, "medium_term_outlook") and result.medium_term_outlook:
                    outlook_lines.append(f"- **中期（1-2周）**：{result.medium_term_outlook}")
                if outlook_lines:
                    report_lines.extend(
                        [
                            "#### 🔮 市场展望",
                            *outlook_lines,
                            "",
                        ]
                    )

                tech_lines = []
                if result.technical_analysis:
                    tech_lines.append(f"**综合**：{result.technical_analysis}")
                if hasattr(result, "ma_analysis") and result.ma_analysis:
                    tech_lines.append(f"**均线**：{result.ma_analysis}")
                if hasattr(result, "volume_analysis") and result.volume_analysis:
                    tech_lines.append(f"**量能**：{result.volume_analysis}")
                if hasattr(result, "pattern_analysis") and result.pattern_analysis:
                    tech_lines.append(f"**形态**：{result.pattern_analysis}")
                if tech_lines:
                    report_lines.extend(
                        [
                            "#### 📊 技术面分析",
                            *tech_lines,
                            "",
                        ]
                    )

                fund_lines = []
                if hasattr(result, "fundamental_analysis") and result.fundamental_analysis:
                    fund_lines.append(result.fundamental_analysis)
                if hasattr(result, "sector_position") and result.sector_position:
                    fund_lines.append(f"**板块地位**：{result.sector_position}")
                if hasattr(result, "company_highlights") and result.company_highlights:
                    fund_lines.append(f"**公司亮点**：{result.company_highlights}")
                if fund_lines:
                    report_lines.extend(
                        [
                            "#### 🏢 基本面分析",
                            *fund_lines,
                            "",
                        ]
                    )

                news_lines = []
                if result.news_summary:
                    news_lines.append(f"**新闻摘要**：{result.news_summary}")
                if hasattr(result, "market_sentiment") and result.market_sentiment:
                    news_lines.append(f"**市场情绪**：{result.market_sentiment}")
                if hasattr(result, "hot_topics") and result.hot_topics:
                    news_lines.append(f"**相关热点**：{result.hot_topics}")
                if news_lines:
                    report_lines.extend(
                        [
                            "#### 📰 消息面/情绪面",
                            *news_lines,
                            "",
                        ]
                    )

                if result.analysis_summary:
                    report_lines.extend(
                        [
                            "#### 📝 综合分析",
                            result.analysis_summary,
                            "",
                        ]
                    )

                if hasattr(result, "risk_warning") and result.risk_warning:
                    report_lines.extend(
                        [
                            f"⚠️ **风险提示**：{result.risk_warning}",
                            "",
                        ]
                    )

                if hasattr(result, "search_performed") and result.search_performed:
                    report_lines.append("*🔍 已执行联网搜索*")
                if hasattr(result, "data_sources") and result.data_sources:
                    report_lines.append(f"*📋 数据来源：{result.data_sources}*")

                if not result.success and result.error_message:
                    report_lines.extend(
                        [
                            "",
                            f"❌ **分析异常**：{result.error_message[:100]}",
                        ]
                    )

                report_lines.extend(
                    [
                        "",
                        "---",
                        "",
                    ]
                )

        report_lines.extend(
            [
                "",
                f"*{labels['generated_at_label']}：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            ]
        )

        return "\n".join(report_lines)

    @staticmethod
    def _escape_md(name: str) -> str:
        """Escape markdown special characters in stock names (e.g. *ST → \\*ST)."""
        return name.replace("*", r"\*") if name else name

    @staticmethod
    def _clean_sniper_value(value: Any) -> str:
        """Normalize sniper point values and remove redundant label prefixes."""
        if value is None:
            return "N/A"
        if isinstance(value, (int, float)):
            return str(value)
        if not isinstance(value, str):
            return str(value)
        if not value or value == "N/A":
            return value
        prefixes = [
            "理想买入点：",
            "次优买入点：",
            "止损位：",
            "目标位：",
            "理想买入点:",
            "次优买入点:",
            "止损位:",
            "目标位:",
            "Ideal Entry:",
            "Secondary Entry:",
            "Stop Loss:",
            "Target:",
        ]
        for prefix in prefixes:
            if value.startswith(prefix):
                return value[len(prefix) :]
        return value

    def _get_signal_level(self, result: AnalysisResult) -> tuple:
        """Get localized signal level and color based on operation advice."""
        return get_signal_level(
            result.operation_advice,
            result.sentiment_score,
            self._get_report_language(result),
        )

    def generate_dashboard_report(
        self,
        results: List[AnalysisResult],
        report_date: Optional[str] = None,
    ) -> str:
        """生成决策仪表盘格式的日报（详细版）"""
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)
        reason_label = "Rationale" if report_language == "en" else "操作理由"
        risk_warning_label = "Risk Warning" if report_language == "en" else "风险提示"
        technical_heading = "Technicals" if report_language == "en" else "技术面"
        ma_label = "Moving Averages" if report_language == "en" else "均线"
        volume_analysis_label = "Volume" if report_language == "en" else "量能"
        news_heading = "News Flow" if report_language == "en" else "消息面"

        if report_date is None:
            report_date = datetime.now().strftime("%Y-%m-%d")

        sorted_results = sorted(results, key=lambda x: x.sentiment_score, reverse=True)

        buy_count = sum(1 for r in results if getattr(r, "decision_type", "") == "buy")
        sell_count = sum(1 for r in results if getattr(r, "decision_type", "") == "sell")
        hold_count = sum(1 for r in results if getattr(r, "decision_type", "") in ("hold", ""))

        report_lines = [
            f"# 🎯 {report_date} {labels['dashboard_title']}",
            "",
            f"> {labels['analyzed_prefix']} **{len(results)}** {labels['stock_unit']} | "
            f"🟢{labels['buy_label']}:{buy_count} 🟡{labels['watch_label']}:{hold_count} 🔴{labels['sell_label']}:{sell_count}",
            "",
        ]

        if results:
            report_lines.extend(
                [
                    f"## 📊 {labels['summary_heading']}",
                    "",
                ]
            )
            for r in sorted_results:
                _, signal_emoji, _ = self._get_signal_level(r)
                display_name = self._get_display_name(r, report_language)
                report_lines.append(
                    f"{signal_emoji} **{display_name}({r.code})**: "
                    f"{localize_operation_advice(r.operation_advice, report_language)} | "
                    f"{labels['score_label']} {r.sentiment_score} | "
                    f"{localize_trend_prediction(r.trend_prediction, report_language)}"
                )
            report_lines.extend(
                [
                    "",
                    "---",
                    "",
                ]
            )

        if not self._report_summary_only:
            for result in sorted_results:
                signal_text, signal_emoji, _signal_tag = self._get_signal_level(result)
                dashboard = result.dashboard if hasattr(result, "dashboard") and result.dashboard else {}

                stock_name = self._get_display_name(result, report_language)

                report_lines.extend(
                    [
                        f"## {signal_emoji} {stock_name} ({result.code})",
                        "",
                    ]
                )

                intel = dashboard.get("intelligence", {}) if dashboard else {}
                if intel:
                    report_lines.extend(
                        [
                            f"### 📰 {labels['info_heading']}",
                            "",
                        ]
                    )
                    if intel.get("sentiment_summary"):
                        report_lines.append(
                            f"**💭 {labels['sentiment_summary_label']}**: {intel['sentiment_summary']}"
                        )
                    if intel.get("earnings_outlook"):
                        report_lines.append(
                            f"**📊 {labels['earnings_outlook_label']}**: {intel['earnings_outlook']}"
                        )
                    risk_alerts = intel.get("risk_alerts", [])
                    if risk_alerts:
                        report_lines.append("")
                        report_lines.append(f"**🚨 {labels['risk_alerts_label']}**:")
                        for alert in risk_alerts:
                            report_lines.append(f"- {alert}")
                    catalysts = intel.get("positive_catalysts", [])
                    if catalysts:
                        report_lines.append("")
                        report_lines.append(f"**✨ {labels['positive_catalysts_label']}**:")
                        for cat in catalysts:
                            report_lines.append(f"- {cat}")
                    if intel.get("latest_news"):
                        report_lines.append("")
                        report_lines.append(f"**📢 {labels['latest_news_label']}**: {intel['latest_news']}")
                    report_lines.append("")

                core = dashboard.get("core_conclusion", {}) if dashboard else {}
                one_sentence = core.get("one_sentence", result.analysis_summary)
                time_sense = core.get("time_sensitivity", labels["default_time_sensitivity"])
                pos_advice = core.get("position_advice", {})

                report_lines.extend(
                    [
                        f"### 📌 {labels['core_conclusion_heading']}",
                        "",
                        f"**{signal_emoji} {signal_text}** | {localize_trend_prediction(result.trend_prediction, report_language)}",
                        "",
                        f"> **{labels['one_sentence_label']}**: {one_sentence}",
                        "",
                        f"⏰ **{labels['time_sensitivity_label']}**: {time_sense}",
                        "",
                    ]
                )
                if pos_advice:
                    report_lines.extend(
                        [
                            f"| {labels['position_status_label']} | {labels['action_advice_label']} |",
                            "|---------|---------|",
                            f"| 🆕 **{labels['no_position_label']}** | {pos_advice.get('no_position', localize_operation_advice(result.operation_advice, report_language))} |",
                            f"| 💼 **{labels['has_position_label']}** | {pos_advice.get('has_position', labels['continue_holding'])} |",
                            "",
                        ]
                    )

                self._append_market_snapshot(report_lines, result)

                data_persp = dashboard.get("data_perspective", {}) if dashboard else {}
                if data_persp:
                    trend_data = data_persp.get("trend_status", {})
                    price_data = data_persp.get("price_position", {})
                    vol_data = data_persp.get("volume_analysis", {})
                    chip_data = data_persp.get("chip_structure", {})

                    report_lines.extend(
                        [
                            f"### 📊 {labels['data_perspective_heading']}",
                            "",
                        ]
                    )
                    if trend_data:
                        is_bullish = (
                            f"✅ {labels['yes_label']}"
                            if trend_data.get("is_bullish", False)
                            else f"❌ {labels['no_label']}"
                        )
                        report_lines.extend(
                            [
                                f"**{labels['ma_alignment_label']}**: {trend_data.get('ma_alignment', 'N/A')} | "
                                f"{labels['bullish_alignment_label']}: {is_bullish} | "
                                f"{labels['trend_strength_label']}: {trend_data.get('trend_score', 'N/A')}/100",
                                "",
                            ]
                        )
                    if price_data:
                        bias_status = price_data.get("bias_status", "N/A")
                        report_lines.extend(
                            [
                                f"| {labels['price_metrics_label']} | {labels['current_price_label']} |",
                                "|---------|------|",
                                f"| {labels['current_price_label']} | {price_data.get('current_price', 'N/A')} |",
                                f"| {labels['ma5_label']} | {price_data.get('ma5', 'N/A')} |",
                                f"| {labels['ma10_label']} | {price_data.get('ma10', 'N/A')} |",
                                f"| {labels['ma20_label']} | {price_data.get('ma20', 'N/A')} |",
                                f"| {labels['bias_ma5_label']} | {price_data.get('bias_ma5', 'N/A')}% {bias_status} |",
                                f"| {labels['support_level_label']} | {price_data.get('support_level', 'N/A')} |",
                                f"| {labels['resistance_level_label']} | {price_data.get('resistance_level', 'N/A')} |",
                                "",
                            ]
                        )
                    if vol_data:
                        report_lines.extend(
                            [
                                f"**{labels['volume_label']}**: {labels['volume_ratio_label']} {vol_data.get('volume_ratio', 'N/A')} ({vol_data.get('volume_status', '')}) | "
                                f"{labels['turnover_rate_label']} {vol_data.get('turnover_rate', 'N/A')}%",
                                f"💡 *{vol_data.get('volume_meaning', '')}*",
                                "",
                            ]
                        )
                    if chip_data:
                        chip_health = localize_chip_health(chip_data.get("chip_health", "N/A"), report_language)
                        report_lines.extend(
                            [
                                f"**{labels['chip_label']}**: {chip_data.get('profit_ratio', 'N/A')} | {chip_data.get('avg_cost', 'N/A')} | "
                                f"{chip_data.get('concentration', 'N/A')} {chip_health}",
                                "",
                            ]
                        )

                battle = dashboard.get("battle_plan", {}) if dashboard else {}
                if battle:
                    report_lines.extend(
                        [
                            f"### 🎯 {labels['battle_plan_heading']}",
                            "",
                        ]
                    )
                    sniper = battle.get("sniper_points", {})
                    if sniper:
                        report_lines.extend(
                            [
                                f"**📍 {labels['action_points_heading']}**",
                                "",
                                f"| {labels['action_points_heading']} | {labels['current_price_label']} |",
                                "|---------|------|",
                                f"| 🎯 {labels['ideal_buy_label']} | {self._clean_sniper_value(sniper.get('ideal_buy', 'N/A'))} |",
                                f"| 🔵 {labels['secondary_buy_label']} | {self._clean_sniper_value(sniper.get('secondary_buy', 'N/A'))} |",
                                f"| 🛑 {labels['stop_loss_label']} | {self._clean_sniper_value(sniper.get('stop_loss', 'N/A'))} |",
                                f"| 🎊 {labels['take_profit_label']} | {self._clean_sniper_value(sniper.get('take_profit', 'N/A'))} |",
                                "",
                            ]
                        )
                    position = battle.get("position_strategy", {})
                    if position:
                        report_lines.extend(
                            [
                                f"**💰 {labels['suggested_position_label']}**: {position.get('suggested_position', 'N/A')}",
                                f"- {labels['entry_plan_label']}: {position.get('entry_plan', 'N/A')}",
                                f"- {labels['risk_control_label']}: {position.get('risk_control', 'N/A')}",
                                "",
                            ]
                        )
                    checklist = battle.get("action_checklist", []) if battle else []
                    if checklist:
                        report_lines.extend(
                            [
                                f"**✅ {labels['checklist_heading']}**",
                                "",
                            ]
                        )
                        for item in checklist:
                            report_lines.append(f"- {item}")
                        report_lines.append("")

                if not dashboard:
                    br = getattr(result, "buy_reason", None)
                    if br:
                        report_lines.extend(
                            [
                                f"**💡 {reason_label}**: {br}",
                                "",
                            ]
                        )
                    rw = getattr(result, "risk_warning", None)
                    if rw:
                        report_lines.extend(
                            [
                                f"**⚠️ {risk_warning_label}**: {rw}",
                                "",
                            ]
                        )
                    ma = getattr(result, "ma_analysis", None)
                    vol = getattr(result, "volume_analysis", None)
                    if ma or vol:
                        report_lines.extend(
                            [
                                f"### 📊 {technical_heading}",
                                "",
                            ]
                        )
                        if ma:
                            report_lines.append(f"**{ma_label}**: {ma}")
                        if vol:
                            report_lines.append(f"**{volume_analysis_label}**: {vol}")
                        report_lines.append("")
                    if result.news_summary:
                        report_lines.extend(
                            [
                                f"### 📰 {news_heading}",
                                f"{result.news_summary}",
                                "",
                            ]
                        )

                report_lines.extend(
                    [
                        "---",
                        "",
                    ]
                )

        report_lines.extend(
            [
                "",
                f"*{labels['generated_at_label']}：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            ]
        )

        return "\n".join(report_lines)

    def generate_wechat_dashboard(self, results: List[AnalysisResult]) -> str:
        """
        生成企业微信决策仪表盘精简版（控制在4000字符内）

        只保留核心结论和狙击点位

        Args:
            results: 分析结果列表

        Returns:
            精简版决策仪表盘
        """
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)

        report_date = datetime.now().strftime("%Y-%m-%d")

        # 按评分排序
        sorted_results = sorted(results, key=lambda x: x.sentiment_score, reverse=True)

        # 统计 - 使用 decision_type 字段准确统计
        buy_count = sum(1 for r in results if getattr(r, "decision_type", "") == "buy")
        sell_count = sum(1 for r in results if getattr(r, "decision_type", "") == "sell")
        hold_count = sum(1 for r in results if getattr(r, "decision_type", "") in ("hold", ""))

        lines = [
            f"## 🎯 {report_date} {labels['dashboard_title']}",
            "",
            f"> {len(results)} {labels['stock_unit']} | "
            f"🟢{labels['buy_label']}:{buy_count} 🟡{labels['watch_label']}:{hold_count} 🔴{labels['sell_label']}:{sell_count}",
            "",
        ]

        # Issue #262: summary_only 时仅输出摘要列表
        if self._report_summary_only:
            lines.append(f"**📊 {labels['summary_heading']}**")
            lines.append("")
            for r in sorted_results:
                _, signal_emoji, _ = self._get_signal_level(r)
                stock_name = self._get_display_name(r, report_language)
                lines.append(
                    f"{signal_emoji} **{stock_name}({r.code})**: "
                    f"{localize_operation_advice(r.operation_advice, report_language)} | "
                    f"{labels['score_label']} {r.sentiment_score} | "
                    f"{localize_trend_prediction(r.trend_prediction, report_language)}"
                )
        else:
            for result in sorted_results:
                signal_text, signal_emoji, _ = self._get_signal_level(result)
                dashboard = result.dashboard if hasattr(result, "dashboard") and result.dashboard else {}
                core = dashboard.get("core_conclusion", {}) if dashboard else {}
                battle = dashboard.get("battle_plan", {}) if dashboard else {}
                intel = dashboard.get("intelligence", {}) if dashboard else {}

                # 股票名称
                stock_name = self._get_display_name(result, report_language)

                # 标题行：信号等级 + 股票名称
                lines.append(f"### {signal_emoji} **{signal_text}** | {stock_name}({result.code})")
                lines.append("")

                # 核心决策（一句话）
                one_sentence = core.get("one_sentence", result.analysis_summary) if core else result.analysis_summary
                if one_sentence:
                    lines.append(f"📌 **{one_sentence[:80]}**")
                    lines.append("")

                # 重要信息区（舆情+基本面）
                info_lines = []

                # 业绩预期
                if intel.get("earnings_outlook"):
                    outlook = str(intel["earnings_outlook"])[:60]
                    info_lines.append(f"📊 {labels['earnings_outlook_label']}: {outlook}")
                if intel.get("sentiment_summary"):
                    sentiment = str(intel["sentiment_summary"])[:50]
                    info_lines.append(f"💭 {labels['sentiment_summary_label']}: {sentiment}")
                if info_lines:
                    lines.extend(info_lines)
                    lines.append("")

                # 风险警报（最重要，醒目显示）
                risks = intel.get("risk_alerts", []) if intel else []
                if risks:
                    lines.append(f"🚨 **{labels['risk_alerts_label']}**:")
                    for risk in risks[:2]:  # 最多显示2条
                        risk_str = str(risk)
                        risk_text = risk_str[:50] + "..." if len(risk_str) > 50 else risk_str
                        lines.append(f"   • {risk_text}")
                    lines.append("")

                # 利好催化
                catalysts = intel.get("positive_catalysts", []) if intel else []
                if catalysts:
                    lines.append(f"✨ **{labels['positive_catalysts_label']}**:")
                    for cat in catalysts[:2]:  # 最多显示2条
                        cat_str = str(cat)
                        cat_text = cat_str[:50] + "..." if len(cat_str) > 50 else cat_str
                        lines.append(f"   • {cat_text}")
                    lines.append("")

                # 狙击点位
                sniper = battle.get("sniper_points", {}) if battle else {}
                if sniper:
                    ideal_buy = str(sniper.get("ideal_buy", ""))
                    stop_loss = str(sniper.get("stop_loss", ""))
                    take_profit = str(sniper.get("take_profit", ""))
                    points = []
                    if ideal_buy:
                        points.append(f"🎯{labels['ideal_buy_label']}:{ideal_buy[:15]}")
                    if stop_loss:
                        points.append(f"🛑{labels['stop_loss_label']}:{stop_loss[:15]}")
                    if take_profit:
                        points.append(f"🎊{labels['take_profit_label']}:{take_profit[:15]}")
                    if points:
                        lines.append(" | ".join(points))
                        lines.append("")

                # 持仓建议
                pos_advice = core.get("position_advice", {}) if core else {}
                if pos_advice:
                    no_pos = str(pos_advice.get("no_position", ""))
                    has_pos = str(pos_advice.get("has_position", ""))
                    if no_pos:
                        lines.append(f"🆕 {labels['no_position_label']}: {no_pos[:50]}")
                    if has_pos:
                        lines.append(f"💼 {labels['has_position_label']}: {has_pos[:50]}")
                    lines.append("")

                # 检查清单简化版
                checklist = battle.get("action_checklist", []) if battle else []
                if checklist:
                    # 只显示不通过的项目
                    failed_checks = [str(c) for c in checklist if str(c).startswith("❌") or str(c).startswith("⚠️")]
                    if failed_checks:
                        lines.append(f"**{labels['failed_checks_heading']}**:")
                        for check in failed_checks[:3]:
                            lines.append(f"   {check[:40]}")
                        lines.append("")

                lines.append("---")
                lines.append("")

        # 底部
        lines.append(f"*{labels['report_time_label']}: {datetime.now().strftime('%H:%M')}*")
        models = self._collect_models_used(results)
        if models:
            lines.append(f"*{labels['analysis_model_label']}: {', '.join(models)}*")

        content = "\n".join(lines)

        return content

    def generate_wechat_summary(self, results: List[AnalysisResult]) -> str:
        """
        生成企业微信精简版日报（控制在4000字符内）

        Args:
            results: 分析结果列表

        Returns:
            精简版 Markdown 内容
        """
        report_date = datetime.now().strftime("%Y-%m-%d")
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)

        # 按评分排序
        sorted_results = sorted(results, key=lambda x: x.sentiment_score, reverse=True)

        # 统计 - 使用 decision_type 字段准确统计
        buy_count = sum(1 for r in results if getattr(r, "decision_type", "") == "buy")
        sell_count = sum(1 for r in results if getattr(r, "decision_type", "") == "sell")
        hold_count = sum(1 for r in results if getattr(r, "decision_type", "") in ("hold", ""))
        avg_score = sum(r.sentiment_score for r in results) / len(results) if results else 0

        lines = [
            f"## 📅 {report_date} {labels['report_title']}",
            "",
            f"> {labels['analyzed_prefix']} **{len(results)}** {labels['stock_unit_compact']} | "
            f"🟢{labels['buy_label']}:{buy_count} 🟡{labels['watch_label']}:{hold_count} 🔴{labels['sell_label']}:{sell_count} | "
            f"{labels['avg_score_label']}:{avg_score:.0f}",
            "",
        ]

        # 每只股票精简信息（控制长度）
        for result in sorted_results:
            _, emoji, _ = self._get_signal_level(result)

            # 核心信息行
            lines.append(f"### {emoji} {self._get_display_name(result, report_language)}({result.code})")
            lines.append(
                f"**{localize_operation_advice(result.operation_advice, report_language)}** | "
                f"{labels['score_label']}:{result.sentiment_score} | "
                f"{localize_trend_prediction(result.trend_prediction, report_language)}"
            )

            # 操作理由（截断）
            if hasattr(result, "buy_reason") and result.buy_reason:
                reason = result.buy_reason[:80] + "..." if len(result.buy_reason) > 80 else result.buy_reason
                lines.append(f"💡 {reason}")

            # 核心看点
            if hasattr(result, "key_points") and result.key_points:
                points = result.key_points[:60] + "..." if len(result.key_points) > 60 else result.key_points
                lines.append(f"🎯 {points}")

            # 风险提示（截断）
            if hasattr(result, "risk_warning") and result.risk_warning:
                risk = result.risk_warning[:50] + "..." if len(result.risk_warning) > 50 else result.risk_warning
                lines.append(f"⚠️ {risk}")

            lines.append("")

        # 底部（模型行在 --- 之前，Issue #528）
        models = self._collect_models_used(results)
        if models:
            lines.append(f"*{labels['analysis_model_label']}: {', '.join(models)}*")
        lines.extend(
            [
                "---",
                f"*{labels['not_investment_advice']}*",
                f"*{labels['details_report_hint']} reports/report_{report_date.replace('-', '')}.md*",
            ]
        )

        content = "\n".join(lines)

        return content

    def generate_brief_report(
        self,
        results: List[AnalysisResult],
        report_date: Optional[str] = None,
    ) -> str:
        """
        Generate brief report (3-5 sentences per stock) for mobile/push.

        Args:
            results: Analysis results list (use [result] for single stock).
            report_date: Report date (default: today).

        Returns:
            Brief markdown content.
        """
        if report_date is None:
            report_date = datetime.now().strftime("%Y-%m-%d")
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)
        # Fallback: brief summary from dashboard report
        if not results:
            return f"# {report_date} {labels['brief_title']}\n\n{labels['no_results']}"
        sorted_results = sorted(results, key=lambda x: x.sentiment_score, reverse=True)
        buy_count = sum(1 for r in results if getattr(r, "decision_type", "") == "buy")
        sell_count = sum(1 for r in results if getattr(r, "decision_type", "") == "sell")
        hold_count = sum(1 for r in results if getattr(r, "decision_type", "") in ("hold", ""))
        lines = [
            f"# {report_date} {labels['brief_title']}",
            "",
            f"> {len(results)} {labels['stock_unit_compact']} | 🟢{buy_count} 🟡{hold_count} 🔴{sell_count}",
            "",
        ]
        for r in sorted_results:
            _, emoji, _ = self._get_signal_level(r)
            name = self._get_display_name(r, report_language)
            dash = r.dashboard or {}
            core = dash.get("core_conclusion", {}) or {}
            one = (core.get("one_sentence") or r.analysis_summary or "")[:60]
            lines.append(
                f"**{name}({r.code})** {emoji} "
                f"{localize_operation_advice(r.operation_advice, report_language)} | "
                f"{labels['score_label']} {r.sentiment_score} | {one}"
            )
        lines.append("")
        lines.append(f"*{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        return "\n".join(lines)

    def generate_single_stock_report(self, result: AnalysisResult) -> str:
        """生成单只股票的分析报告（用于单股推送模式）"""
        report_date = datetime.now().strftime("%Y-%m-%d %H:%M")
        report_language = self._get_report_language(result)
        labels = get_report_labels(report_language)
        signal_text, signal_emoji, _ = self._get_signal_level(result)
        dashboard = result.dashboard if hasattr(result, "dashboard") and result.dashboard else {}
        core = dashboard.get("core_conclusion", {}) if dashboard else {}
        battle = dashboard.get("battle_plan", {}) if dashboard else {}
        intel = dashboard.get("intelligence", {}) if dashboard else {}

        stock_name = self._get_display_name(result, report_language)

        lines = [
            f"## {signal_emoji} {stock_name} ({result.code})",
            "",
            f"> {report_date} | {labels['score_label']}: **{result.sentiment_score}** | {localize_trend_prediction(result.trend_prediction, report_language)}",
            "",
        ]

        self._append_market_snapshot(lines, result)

        one_sentence = core.get("one_sentence", result.analysis_summary) if core else result.analysis_summary
        if one_sentence:
            lines.extend(
                [
                    f"### 📌 {labels['core_conclusion_heading']}",
                    "",
                    f"**{signal_text}**: {one_sentence}",
                    "",
                ]
            )

        info_added = False
        if intel:
            if intel.get("earnings_outlook"):
                if not info_added:
                    lines.append(f"### 📰 {labels['info_heading']}")
                    lines.append("")
                    info_added = True
                lines.append(f"📊 **{labels['earnings_outlook_label']}**: {str(intel['earnings_outlook'])[:100]}")

            if intel.get("sentiment_summary"):
                if not info_added:
                    lines.append(f"### 📰 {labels['info_heading']}")
                    lines.append("")
                    info_added = True
                lines.append(f"💭 **{labels['sentiment_summary_label']}**: {str(intel['sentiment_summary'])[:80]}")

            risks = intel.get("risk_alerts", [])
            if risks:
                if not info_added:
                    lines.append(f"### 📰 {labels['info_heading']}")
                    lines.append("")
                    info_added = True
                lines.append("")
                lines.append(f"🚨 **{labels['risk_alerts_label']}**:")
                for risk in risks[:3]:
                    lines.append(f"- {str(risk)[:60]}")

            catalysts = intel.get("positive_catalysts", [])
            if catalysts:
                lines.append("")
                lines.append(f"✨ **{labels['positive_catalysts_label']}**:")
                for cat in catalysts[:3]:
                    lines.append(f"- {str(cat)[:60]}")

        if info_added:
            lines.append("")

        sniper = battle.get("sniper_points", {}) if battle else {}
        if sniper:
            lines.extend(
                [
                    f"### 🎯 {labels['action_points_heading']}",
                    "",
                    f"| {labels['ideal_buy_label']} | {labels['stop_loss_label']} | {labels['take_profit_label']} |",
                    "|------|------|------|",
                ]
            )
            ideal_buy = sniper.get("ideal_buy", "-")
            stop_loss = sniper.get("stop_loss", "-")
            take_profit = sniper.get("take_profit", "-")
            lines.append(f"| {ideal_buy} | {stop_loss} | {take_profit} |")
            lines.append("")

        pos_advice = core.get("position_advice", {}) if core else {}
        if pos_advice:
            lines.extend(
                [
                    f"### 💼 {labels['position_advice_heading']}",
                    "",
                    f"- 🆕 **{labels['no_position_label']}**: {pos_advice.get('no_position', localize_operation_advice(result.operation_advice, report_language))}",
                    f"- 💼 **{labels['has_position_label']}**: {pos_advice.get('has_position', labels['continue_holding'])}",
                    "",
                ]
            )

        lines.append("---")
        model_used = normalize_model_used(getattr(result, "model_used", None))
        if model_used:
            lines.append(f"*{labels['analysis_model_label']}: {model_used}*")
        lines.append(f"*{labels['not_investment_advice']}*")

        return "\n".join(lines)

    _SOURCE_DISPLAY_NAMES = {
        "tencent": {"zh": "腾讯财经", "en": "Tencent Finance"},
        "akshare_em": {"zh": "东方财富", "en": "Eastmoney"},
        "akshare_sina": {"zh": "新浪财经", "en": "Sina Finance"},
        "akshare_qq": {"zh": "腾讯财经", "en": "Tencent Finance"},
        "efinance": {"zh": "东方财富(efinance)", "en": "Eastmoney (efinance)"},
        "tushare": {"zh": "Tushare Pro", "en": "Tushare Pro"},
        "sina": {"zh": "新浪财经", "en": "Sina Finance"},
        "stooq": {"zh": "Stooq", "en": "Stooq"},
        "longbridge": {"zh": "长桥", "en": "Longbridge"},
        "fallback": {"zh": "降级兜底", "en": "Fallback"},
    }

    def _get_source_display_name(self, source: Any, language: Optional[str]) -> str:
        raw_source = str(source or "N/A")
        mapping = self._SOURCE_DISPLAY_NAMES.get(raw_source)
        if not mapping:
            return raw_source
        return mapping[normalize_report_language(language)]

    def _append_market_snapshot(self, lines: List[str], result: AnalysisResult) -> None:
        snapshot = getattr(result, "market_snapshot", None)
        if not snapshot:
            return

        report_language = self._get_report_language(result)
        labels = get_report_labels(report_language)

        lines.extend(
            [
                f"### 📈 {labels['market_snapshot_heading']}",
                "",
                f"| {labels['close_label']} | {labels['prev_close_label']} | {labels['open_label']} | {labels['high_label']} | {labels['low_label']} | {labels['change_pct_label']} | {labels['change_amount_label']} | {labels['amplitude_label']} | {labels['volume_label']} | {labels['amount_label']} |",
                "|------|------|------|------|------|-------|-------|------|--------|--------|",
                f"| {snapshot.get('close', 'N/A')} | {snapshot.get('prev_close', 'N/A')} | "
                f"{snapshot.get('open', 'N/A')} | {snapshot.get('high', 'N/A')} | "
                f"{snapshot.get('low', 'N/A')} | {snapshot.get('pct_chg', 'N/A')} | "
                f"{snapshot.get('change_amount', 'N/A')} | {snapshot.get('amplitude', 'N/A')} | "
                f"{snapshot.get('volume', 'N/A')} | {snapshot.get('amount', 'N/A')} |",
            ]
        )

        if "price" in snapshot:
            display_source = self._get_source_display_name(snapshot.get("source", "N/A"), report_language)
            lines.extend(
                [
                    "",
                    f"| {labels['current_price_label']} | {labels['volume_ratio_label']} | {labels['turnover_rate_label']} | {labels['source_label']} |",
                    "|-------|------|--------|----------|",
                    f"| {snapshot.get('price', 'N/A')} | {snapshot.get('volume_ratio', 'N/A')} | "
                    f"{snapshot.get('turnover_rate', 'N/A')} | {display_source} |",
                ]
            )

        lines.append("")

    def send(
        self,
        content: str,
        email_stock_codes: Optional[List[str]] = None,
        email_send_to_all: bool = False,
    ) -> bool:
        """向所有已配置的渠道发送 Markdown 内容（文本模式）。"""
        if not self._available_channels:
            logger.warning("通知服务不可用，跳过推送")
            return False

        channel_names = self.get_channel_names()
        logger.info(f"正在向 {len(self._available_channels)} 个渠道发送通知：{channel_names}")

        success_count = 0
        fail_count = 0

        for channel in self._available_channels:
            channel_name = ChannelDetector.get_channel_name(channel)
            try:
                if channel == NotificationChannel.WECHAT:
                    result = self.send_to_wechat(content)
                elif channel == NotificationChannel.FEISHU:
                    result = self.send_to_feishu(content)
                elif channel == NotificationChannel.TELEGRAM:
                    result = self.send_to_telegram(content)
                elif channel == NotificationChannel.EMAIL:
                    receivers = None
                    if email_send_to_all and self._stock_email_groups:
                        receivers = self.get_all_email_receivers()
                    elif email_stock_codes and self._stock_email_groups:
                        receivers = self.get_receivers_for_stocks(email_stock_codes)
                    result = self.send_to_email(content, receivers=receivers)
                elif channel == NotificationChannel.PUSHOVER:
                    result = self.send_to_pushover(content)
                elif channel == NotificationChannel.PUSHPLUS:
                    result = self.send_to_pushplus(content)
                elif channel == NotificationChannel.SERVERCHAN3:
                    result = self.send_to_serverchan3(content)
                elif channel == NotificationChannel.CUSTOM:
                    result = self.send_to_custom(content)
                elif channel == NotificationChannel.DISCORD:
                    result = self.send_to_discord(content)
                elif channel == NotificationChannel.SLACK:
                    result = self.send_to_slack(content)
                elif channel == NotificationChannel.ASTRBOT:
                    result = self.send_to_astrbot(content)
                else:
                    logger.warning(f"不支持的通知渠道: {channel}")
                    result = False

                if result:
                    success_count += 1
                else:
                    fail_count += 1

            except Exception as e:
                logger.error(f"{channel_name} 发送失败: {e}")
                fail_count += 1

        logger.info(f"通知发送完成：成功 {success_count} 个，失败 {fail_count} 个")
        return success_count > 0

    def save_report_to_file(self, content: str, filename: Optional[str] = None) -> str:
        """保存日报到项目根目录 ``reports/`` 下。"""
        if filename is None:
            date_str = datetime.now().strftime("%Y%m%d")
            filename = f"report_{date_str}.md"

        reports_dir = Path(__file__).resolve().parent.parent.parent / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        filepath = reports_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"日报已保存到: {filepath}")
        return str(filepath)


class NotificationBuilder:
    """
    通知消息构建器

    提供便捷的消息构建方法
    """

    @staticmethod
    def build_simple_alert(
        title: str,
        content: str,
        alert_type: str = "info",
    ) -> str:
        """
        构建简单的提醒消息

        Args:
            title: 标题
            content: 内容
            alert_type: 类型（info, warning, error, success）
        """
        emoji_map = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌",
            "success": "✅",
        }
        emoji = emoji_map.get(alert_type, "📢")

        return f"{emoji} **{title}**\n\n{content}"

    @staticmethod
    def build_stock_summary(results: List[AnalysisResult]) -> str:
        """
        构建股票摘要（简短版）

        适用于快速通知
        """
        report_language = normalize_report_language(
            next(
                (
                    getattr(result, "report_language", None)
                    for result in results
                    if getattr(result, "report_language", None)
                ),
                None,
            )
        )
        labels = get_report_labels(report_language)
        lines = [f"📊 **{labels['summary_heading']}**", ""]

        for r in sorted(results, key=lambda x: x.sentiment_score, reverse=True):
            _, emoji, _ = get_signal_level(r.operation_advice, r.sentiment_score, report_language)
            name = get_localized_stock_name(r.name, r.code, report_language)
            lines.append(
                f"{emoji} {name}({r.code}): {localize_operation_advice(r.operation_advice, report_language)} | "
                f"{labels['score_label']} {r.sentiment_score}"
            )

        return "\n".join(lines)


def get_notification_service() -> NotificationService:
    """获取通知服务实例"""
    return NotificationService()


def send_daily_report(results: List[AnalysisResult]) -> bool:
    """
    发送每日报告的快捷方式

    自动识别渠道并推送
    """
    service = get_notification_service()

    # 生成报告
    report = service.generate_daily_report(results)

    # 保存到本地
    service.save_report_to_file(report)

    # 推送到配置的渠道（自动识别）
    return service.send(report)
