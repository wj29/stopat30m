"""Offline monetary invariants for the personal SQLite account."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from stopat30m.storage.models import Base, PortfolioAccount, PortfolioExecution, TradeRecord, User
from stopat30m.trading import portfolio as p


class PortfolioAccountTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.engine = create_engine(
            'sqlite:///' + str(Path(self.temp.name) / 'account.db'),
            connect_args={'check_same_thread': False, 'timeout': 10},
        )
        @event.listens_for(self.engine, 'connect')
        def pragmas(connection, _):
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA busy_timeout=10000')
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.sessions.begin() as db:
            db.add_all([User(id=1, username='owner', password_hash='unused'),
                        User(id=2, username='other', password_hash='unused')])
        self.clock = patch.object(p, 'today', return_value=date(2026, 10, 19))
        self.clock.start()

    def tearDown(self):
        self.clock.stop()
        self.engine.dispose()
        self.temp.cleanup()

    def initialize(self, db, capital=10000, user=1):
        return p.initialize_account(db, user, capital, 'init', opened_date='2026-10-15')[0]

    def trade(self, db, key, direction='BUY', qty=100, price=10, day='2026-10-16', **kwargs):
        return p.record_trade(db, 1, '600000', direction, qty, price, key, trade_date=day, **kwargs)

    def test_round_trip_fees_and_external_cash_are_reconciled(self):
        with self.sessions.begin() as db:
            account = self.initialize(db)
            self.trade(db, 'buy')
            sell, _ = self.trade(db, 'sell', 'SELL', day='2026-10-19')
            self.assertEqual(sell.realized_pnl, Decimal('-10.52'))
            self.assertEqual(account.cash, Decimal('9989.48'))
            p.record_cash_flow(db, 1, 'DEPOSIT', 1000, 'deposit', effective_date='2026-10-19')
            p.record_cash_flow(db, 1, 'WITHDRAW', 500, 'withdraw', effective_date='2026-10-19')
            summary, positions = p.valuation(db, account)
            self.assertEqual(positions, [])
            self.assertEqual(summary['net_deposits'], 10500)
            self.assertEqual(summary['total_fees'], 10.52)
            self.assertAlmostEqual(summary['total_pnl'], -10.52)
            self.assertEqual(summary['realized_pnl'], summary['total_pnl'])

    def test_partial_sale_preserves_cost_and_profit_identity(self):
        with self.sessions.begin() as db:
            account = self.initialize(db)
            self.trade(db, 'buy', qty=300)
            self.trade(db, 'sell', 'SELL', price=11, day='2026-10-19')
            summary, positions = p.valuation(db, account, {'SH600000': {'price': 12, 'status': 'available'}})
            self.assertEqual(positions[0]['quantity'], 200)
            self.assertEqual(positions[0]['available_quantity'], 200)
            self.assertAlmostEqual(summary['total_pnl'], summary['realized_pnl'] + summary['unrealized_pnl'])

    def test_recorded_odd_share_execution_and_actual_fees_are_allowed(self):
        with self.sessions.begin() as db:
            self.initialize(db)
            row, _ = self.trade(db, 'odd', qty=37, commission=1.2, stamp_tax=0, transfer_fee=0)
            self.assertEqual(row.quantity, 37)
            self.assertEqual(row.net_amount, Decimal('-371.20'))

    def test_t_plus_one_cash_and_oversell(self):
        with self.sessions.begin() as db:
            self.initialize(db, capital=1005)
            with self.assertRaisesRegex(p.PortfolioError, '现金不足'):
                self.trade(db, 'too-expensive')
            self.trade(db, 'buy', price=9)
            with self.assertRaisesRegex(p.PortfolioError, 'T\\+1'):
                self.trade(db, 'same-day', 'SELL', price=9)
            with self.assertRaisesRegex(p.PortfolioError, '持仓不足'):
                self.trade(db, 'oversell', 'SELL', qty=200, day='2026-10-19')
            with self.assertRaisesRegex(p.PortfolioError, '现金不足'):
                p.record_cash_flow(db, 1, 'WITHDRAW', 1000, 'withdraw')
            self.assertEqual(db.query(PortfolioExecution).count(), 1)

    def test_idempotency_and_payload_conflicts(self):
        with self.sessions.begin() as db:
            account = self.initialize(db)
            first, replay = self.trade(db, 'same')
            second, replay = self.trade(db, 'same')
            self.assertTrue(replay)
            self.assertEqual(first.id, second.id)
            self.assertEqual(account.cash, Decimal('8994.99'))
            with self.assertRaisesRegex(p.PortfolioError, '幂等'):
                self.trade(db, 'same', qty=200)
            first, _ = p.record_cash_flow(db, 1, 'DEPOSIT', 200, 'fund')
            second, replay = p.record_cash_flow(db, 1, 'DEPOSIT', 200, 'fund')
            self.assertTrue(replay)
            self.assertEqual(first.id, second.id)
            with self.assertRaisesRegex(p.PortfolioError, '已初始化'):
                p.initialize_account(db, 1, 10000, 'another')

    def test_future_backdated_invalid_and_nonfinite_inputs(self):
        with self.sessions.begin() as db:
            self.initialize(db)
            self.trade(db, 'buy')
            for invalid in (float('inf'), float('nan'), -1, 0):
                with self.subTest(price=invalid), self.assertRaises(p.PortfolioError):
                    self.trade(db, str(invalid), price=invalid)
            for qty in (1.5, True, 0):
                with self.subTest(qty=qty), self.assertRaises(p.PortfolioError):
                    self.trade(db, str(qty), qty=qty)
            for day in ('2026-10-20', '2026-10-14', '2026-10-15', 'invalid'):
                with self.subTest(day=day), self.assertRaises(p.PortfolioError):
                    self.trade(db, day, day=day)
            with self.assertRaises(p.PortfolioError):
                self.trade(db, 'badfee', commission=float('nan'))
            with self.assertRaises(p.PortfolioError):
                p.record_trade(db, 1, 'garbage', 'BUY', 100, 1, 'badcode')
            self.assertEqual(db.query(PortfolioExecution).count(), 1)

    def test_missing_quotes_are_null_and_legacy_is_not_a_position(self):
        with self.sessions.begin() as db:
            account = self.initialize(db)
            db.add_all([TradeRecord(user_id=None, instrument='SH600001', direction='BUY', quantity=99, price=1, amount=99),
                        TradeRecord(user_id=2, instrument='SH600002', direction='BUY', quantity=99, price=1, amount=99)])
            self.trade(db, 'buy')
            summary, rows = p.valuation(db, account)
            self.assertFalse(summary['valuation_complete'])
            self.assertIsNone(summary['market_value'])
            self.assertIsNone(summary['equity'])
            self.assertIsNone(rows[0]['current_price'])
            self.assertIsNone(rows[0]['pnl_pct'])
            self.assertEqual(rows[0]['cost'], 1005.01)
            self.assertEqual(len(rows), 1)
            self.assertEqual(p.legacy_counts(db, 1), {'legacy_trade_count': 0, 'legacy_unassigned_count': 0})
            self.assertEqual(p.legacy_counts(db, 1, True)['legacy_unassigned_count'], 1)
            self.initialize(db, user=2)
            other = p.get_account(db, 2)
            self.assertEqual(p.holdings(db, other.id), {})

    def test_parallel_requests_cannot_double_spend(self):
        with self.sessions.begin() as db:
            self.initialize(db, capital=1000)
        def buy(key):
            try:
                with self.sessions.begin() as db:
                    self.trade(db, key, price=6, commission=0, stamp_tax=0, transfer_fee=0)
                return True
            except p.PortfolioError:
                return False
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(buy, ['parallel-1', 'parallel-2']))
        self.assertEqual(sorted(results), [False, True])
        with self.sessions() as db:
            self.assertEqual(p.get_account(db, 1).cash, Decimal('400.00'))
            self.assertEqual(db.query(PortfolioExecution).count(), 1)
