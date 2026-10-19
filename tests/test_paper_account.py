"""Offline simulated broker regressions; never contacts a market or broker."""
from contextlib import contextmanager
from datetime import date
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from stopat30m.trading.broker import paper
from stopat30m.trading.models import Order, OrderDirection, OrderStatus, OrderType
from stopat30m.trading.risk import RiskManager


@contextmanager
def exchange_day(day):
    with patch.object(paper, '_today', return_value=date.fromisoformat(day)), \
         patch.object(paper, '_now', return_value=day + ' 10:00:00'):
        yield


def order(direction='BUY', key=None, quantity=100, limit_price=None):
    options = {} if key is None else {'id': key}
    return Order('SH600000', OrderDirection(direction),
                 OrderType.LIMIT if limit_price is not None else OrderType.MARKET,
                 quantity, limit_price=limit_price, **options)


class PaperAccountTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.broker = paper.PaperBroker.init_account(10000, self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_first_purchase_quote_and_restart(self):
        with exchange_day('2026-10-16'):
            self.broker.update_prices({'SH600000': 10})
            fresh = paper.PaperBroker(self.temp.name)
            result = fresh.submit_order(order())
            self.assertEqual(result.status, OrderStatus.FILLED)
            self.assertEqual(fresh.get_account().cash, 8994.99)
            self.assertEqual(fresh.get_positions()['SH600000'].total_cost, 1005.01)

    def test_same_day_settlement_does_not_release_t_plus_one(self):
        with exchange_day('2026-10-16'):
            self.broker.update_prices({'SH600000': 10})
            self.broker.submit_order(order())
            first = self.broker.settle_day('2026-10-16')
            second = self.broker.settle_day('2026-10-16')
            self.assertEqual(first.date, second.date)
            self.assertEqual(len(self.broker.get_nav_history()), 1)
            self.assertEqual(self.broker.get_positions()['SH600000'].available_quantity, 0)
            result = self.broker.submit_order(order('SELL'))
            self.assertEqual(result.status, OrderStatus.REJECTED)
            with self.assertRaises(ValueError):
                self.broker.settle_day('2026-10-19')
        with exchange_day('2026-10-19'):
            self.broker.update_prices({'SH600000': 10})
            result = self.broker.submit_order(order('SELL'))
            self.assertEqual(result.status, OrderStatus.FILLED)
            account = self.broker.get_account()
            self.assertAlmostEqual(account.total_pnl, -10.52)
            self.assertEqual(account.realized_pnl, -10.52)
            self.assertAlmostEqual(account.total_pnl, account.realized_pnl)

    def test_order_retries_are_persistently_idempotent(self):
        with exchange_day('2026-10-16'):
            self.broker.update_prices({'SH600000': 10})
            self.broker.submit_order(order(key='client-key'))
            fresh = paper.PaperBroker(self.temp.name)
            result = fresh.submit_order(order(key='client-key'))
            self.assertEqual(result.status, OrderStatus.FILLED)
            self.assertEqual(len(fresh.get_fills()), 1)
            self.assertEqual(fresh.get_positions()['SH600000'].quantity, 100)
            with self.assertRaises(ValueError):
                fresh.submit_order(order(key='client-key', quantity=200))

    def test_existing_account_and_invalid_capital_cannot_be_initialized(self):
        with self.assertRaises(ValueError):
            paper.PaperBroker.init_account(999999, self.temp.name)
        self.assertEqual(paper.PaperBroker(self.temp.name).get_account().cash, 10000)
        for capital in (0, -1, float('inf'), float('nan')):
            with self.subTest(capital=capital), self.assertRaises(ValueError):
                paper.PaperBroker.init_account(capital, self.temp.name)

    def test_two_instances_reload_state_and_executable_limit_gets_market_price(self):
        stale = paper.PaperBroker(self.temp.name)
        with exchange_day('2026-10-16'):
            self.broker.update_prices({'SH600000': 10, 'SZ000001': float('nan')})
            self.broker.submit_order(order())
            result = stale.submit_order(order(limit_price=11))
            self.assertEqual(result.avg_fill_price, 10)
            self.assertEqual(stale.get_positions()['SH600000'].quantity, 200)
            self.assertEqual(stale.get_account().cash, 7989.98)

    def test_risk_halt_allows_exits_but_still_stops_new_exposure(self):
        risk = RiskManager()
        risk.state.circuit_breaker_active = True
        risk.state.last_reset_date = '2026-10-19'
        allowed, _ = risk.check_order('SH600000', 'SELL', 100, 10, 9000,
                                      {'SH600000': 1000}, current_date='2026-10-19')
        self.assertTrue(allowed)
        allowed, _ = risk.check_order('SH600000', 'BUY', 100, 10, 9000,
                                      {'SH600000': 1000}, current_date='2026-10-19')
        self.assertFalse(allowed)

    def test_persisted_old_quote_does_not_authorize_a_new_fill(self):
        with exchange_day('2026-10-16'):
            self.broker.update_prices({'SH600000': 10})
        with exchange_day('2026-10-19'):
            fresh = paper.PaperBroker(self.temp.name)
            result = fresh.submit_order(order())
            self.assertEqual(result.status, OrderStatus.REJECTED)
            self.assertEqual(fresh.get_account().cash, 10000)
            fresh.update_prices({'SH600000': 11})
            result = fresh.submit_order(order())
            self.assertEqual(result.status, OrderStatus.FILLED)
            self.assertEqual(result.avg_fill_price, 11)

    def test_same_day_resettlement_updates_nav_without_duplicating_date(self):
        with exchange_day('2026-10-16'):
            self.broker.update_prices({'SH600000': 10})
            self.broker.submit_order(order())
            self.broker.settle_day('2026-10-16')
            self.broker.update_prices({'SH600000': 11})
            self.broker.submit_order(order())
            snapshot = self.broker.settle_day('2026-10-16')
            self.assertEqual(len(self.broker.get_nav_history()), 1)
            self.assertEqual(snapshot.equity, 10089.98)
            self.assertEqual(snapshot.total_fees, 10.02)
            self.assertEqual(self.broker.get_positions()['SH600000'].available_quantity, 0)
            self.assertAlmostEqual(snapshot.daily_return, (10089.98 - 10000) / 10000, places=6)
