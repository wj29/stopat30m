"""A rebalance plan's old price must never become a fresh execution quote."""
from datetime import date
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

import pandas as pd

from stopat30m.trading.broker import paper
from stopat30m.trading.executor import execute_plan
from stopat30m.trading.rebalancer import RebalancePlan


class PaperExecutionQuoteTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.broker = paper.PaperBroker.init_account(10000, self.temp.name)
        self.clock = patch.object(paper, '_now', return_value='2026-10-19 10:00:00')
        self.day = patch.object(paper, '_today', return_value=date(2026, 10, 19))
        self.clock_mock = self.clock.start()
        self.day.start()
        self.plan = RebalancePlan(pd.DataFrame([{
            'instrument': 'SH600000', 'direction': 'BUY', 'quantity': 100,
            'price': 3, 'reason': 'old-plan',
        }]))

    def tearDown(self):
        self.clock.stop()
        self.day.stop()
        self.temp.cleanup()

    def execute(self, quotes, risk_manager=None):
        with patch('stopat30m.data.realtime.fetch_spot_quotes', return_value=quotes) as fetch:
            result = execute_plan(self.plan, self.broker, risk_manager=risk_manager)
        fetch.assert_called_once_with(['SH600000'], ttl=0)
        return result

    def test_old_plan_and_recent_synthetic_cache_cannot_replace_missing_quote(self):
        self.broker.update_prices({'SH600000': 5})
        result = self.execute({})
        self.assertEqual(result.total_filled, 0)
        self.assertEqual(result.total_rejected, 1)
        self.assertIn('拒绝使用计划价', result.rejected[0].reject_reason)
        self.assertEqual(self.broker.get_account().cash, 10000)
        self.assertEqual(len(self.broker.get_orders()), 1)
        self.assertIsNone(self.broker._get_market_price('SH600000'))

    def test_source_time_is_required_and_realtime_flag_cannot_resurrect_old_time(self):
        invalid = [
            {'price': 10, 'is_realtime': True, 'as_of': None},
            {'price': 10, 'is_realtime': True, 'as_of': '2026-10-19T09:00:00+08:00'},
            {'price': 10, 'is_realtime': True, 'as_of': '2026-10-19T10:00:00'},
            {'price': 10, 'is_realtime': False, 'as_of': '2026-10-19T10:00:00+08:00'},
            {'price': 10, 'is_realtime': True, 'as_of': '2026-10-19T10:01:00+08:00'},
        ]
        for quote in invalid:
            with self.subTest(quote=quote):
                self.assertEqual(self.execute({'SH600000': quote}).total_filled, 0)
        self.assertEqual(self.broker.get_account().cash, 10000)

    def test_fresh_quote_preserves_provider_time_and_drives_risk_price(self):
        quote = {'price': 11, 'is_realtime': True, 'status': 'available',
                 'source': 'offline-test', 'as_of': '2026-10-19T01:59:30+00:00'}
        risk = Mock()
        risk.check_order.return_value = (True, 'approved')
        result = self.execute({'SH600000': quote}, risk)
        self.assertEqual(result.total_filled, 1)
        self.assertEqual(result.filled[0].avg_fill_price, 11)
        self.assertEqual(risk.check_order.call_args.kwargs['price'], 11)
        self.assertEqual(self.broker._price_timestamps['SH600000'], quote['as_of'])
        self.assertEqual(self.broker.get_account().cash, 8894.99)
        reloaded = paper.PaperBroker(self.temp.name)
        self.assertEqual(reloaded._price_timestamps['SH600000'], quote['as_of'])
        self.clock_mock.return_value = '2026-10-19 10:02:00'
        self.assertIsNone(reloaded._get_market_price('SH600000'))
