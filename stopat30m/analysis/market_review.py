# -*- coding: utf-8 -*-
"""
大盘复盘编排（A 股）

职责：
1. 执行大盘复盘分析并生成复盘报告
2. 保存和发送复盘报告
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from loguru import logger

from stopat30m.analysis.llm_analyzer import LLMAnalyzer
from stopat30m.analysis.market_analyzer import MarketAnalyzer
from stopat30m.analysis.search_service import SearchService
from stopat30m.config import get
from stopat30m.notification.service import NotificationService


def _normalize_report_language(raw: str | None) -> str:
    v = (raw or "zh").strip().lower()
    return "en" if v in ("en", "english", "us") else "zh"


def _get_market_review_text(language: str) -> dict[str, str]:
    normalized = _normalize_report_language(language)
    if normalized == "en":
        return {
            "root_title": "# 🎯 Market Review",
            "push_title": "🎯 Market Review",
        }
    return {
        "root_title": "# 🎯 大盘复盘",
        "push_title": "🎯 大盘复盘",
    }


def run_market_review(
    notifier: NotificationService,
    analyzer: Optional[LLMAnalyzer] = None,
    search_service: Optional[SearchService] = None,
    send_notification: bool = True,
    merge_notification: bool = False,
) -> Optional[str]:
    """
    执行大盘复盘分析（A 股）

    Args:
        notifier: 通知服务
        analyzer: AI分析器（可选，默认新建 LLMAnalyzer）
        search_service: 搜索服务（可选）
        send_notification: 是否发送通知
        merge_notification: 是否合并推送（跳过本次推送，由上层合并后统一发送）

    Returns:
        复盘报告正文（不含根标题）；失败时 None
    """
    logger.info("开始执行大盘复盘分析（A股）...")
    lang = get("analysis", "report_language") or get("project", "report_language") or "zh"
    review_text = _get_market_review_text(str(lang))

    try:
        market_analyzer = MarketAnalyzer(search_service=search_service, analyzer=analyzer)
        review_report = market_analyzer.run_daily_review()

        if review_report:
            date_str = datetime.now().strftime("%Y%m%d")
            report_filename = f"market_review_{date_str}.md"
            filepath = notifier.save_report_to_file(
                f"{review_text['root_title']}\n\n{review_report}",
                report_filename,
            )
            logger.info(f"大盘复盘报告已保存: {filepath}")

            if merge_notification and send_notification:
                logger.info("合并推送模式：跳过大盘复盘单独推送，将在合并后统一发送")
            elif send_notification and notifier.is_available():
                report_content = f"{review_text['push_title']}\n\n{review_report}"
                success = notifier.send(report_content, email_send_to_all=True)
                if success:
                    logger.info("大盘复盘推送成功")
                else:
                    logger.warning("大盘复盘推送失败")
            elif not send_notification:
                logger.info("已跳过推送通知")

            return review_report

    except Exception as e:
        logger.error(f"大盘复盘分析失败: {e}")

    return None
