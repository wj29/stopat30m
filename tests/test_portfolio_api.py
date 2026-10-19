"""HTTP contract tests with local synthetic data and all market IO mocked."""
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from stopat30m.api.deps import get_db_session
from stopat30m.api.v1.endpoints.trading import router
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import Base, TradeRecord, User
from stopat30m.trading import portfolio


class PortfolioApiTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.engine = create_engine('sqlite:///' + str(Path(self.temp.name) / 'api.db'),
                                    connect_args={'check_same_thread': False})
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.sessions.begin() as db:
            db.add_all([User(id=1, username='owner', password_hash='unused'),
                        User(id=2, username='other', password_hash='unused')])
            for owner in (1, 2, None):
                db.add(TradeRecord(user_id=owner, trade_date=datetime(2026, 10, 15), instrument='SH600000',
                                   direction='BUY', quantity=99, price=1, amount=99))
        self.active_user = 1
        self.admin = False
        app = FastAPI()
        app.include_router(router, prefix='/api/v1')
        def db_session():
            # Match production's auto-commit dependency; endpoints may commit
            # financial writes before returning their durable receipt.
            with self.sessions() as db:
                try:
                    yield db
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
        app.dependency_overrides[get_db_session] = db_session
        app.dependency_overrides[get_current_user] = lambda: User(
            id=self.active_user, username='test', role='admin' if self.admin else 'user')
        self.client = TestClient(app)
        self.clock = patch.object(portfolio, 'today', return_value=date(2026, 10, 19))
        self.clock.start()
        self.quotes = patch('stopat30m.data.realtime.fetch_spot_quotes', return_value={})
        self.market = self.quotes.start()

    def tearDown(self):
        self.clock.stop()
        self.quotes.stop()
        self.client.close()
        self.engine.dispose()
        self.temp.cleanup()

    def post(self, path, payload):
        return self.client.post('/api/v1/trading/' + path, json=payload)

    def get(self, path):
        return self.client.get('/api/v1/trading/' + path)

    def initialize(self):
        response = self.post('account/initialize', {'initial_capital': 10000,
                            'opened_date': '2026-10-15', 'idempotency_key': 'init'})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def buy(self, key='buy'):
        response = self.post('trade', {'instrument': '600000', 'direction': 'BUY',
                             'quantity': 37, 'price': 10, 'trade_date': '2026-10-16',
                             'idempotency_key': key, 'commission': 1, 'stamp_tax': 0, 'transfer_fee': 0})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_initialization_and_uninitialized_contract(self):
        self.assertEqual(self.get('account').json()['initialized'], False)
        self.assertIsNone(self.get('positions').json()['total_value'])
        first = self.initialize()
        self.assertEqual(first['account']['cash'], 10000)
        self.assertEqual(first['legacy_trade_count'], 1)
        self.assertEqual(self.get('positions').json()['positions'], [])
        self.assertTrue(self.initialize()['replayed'])
        conflict = self.post('account/initialize', {'initial_capital': 20000, 'idempotency_key': 'other'})
        self.assertEqual(conflict.status_code, 409)
        self.assertIsInstance(conflict.json()['detail'], str)

    def test_trade_retry_pagination_and_missing_quote(self):
        self.initialize()
        first, second = self.buy(), self.buy()
        self.assertEqual(first['id'], second['id'])
        self.assertTrue(second['replayed'])
        trades = self.get('trades?limit=1&offset=0').json()
        self.assertEqual(trades['total'], 1)
        self.assertEqual(trades['limit'], 1)
        self.assertEqual(trades['items'][0]['net_amount'], -371)
        summary = self.get('account').json()['account']
        self.assertEqual(summary['cash'], 9629)
        self.assertIsNone(summary['equity'])
        self.assertFalse(summary['valuation_complete'])
        position = self.get('positions').json()['positions'][0]
        self.assertIsNone(position['current_price'])
        self.assertEqual(position['available_quantity'], 37)
        self.assertEqual(self.get('trades?offset=1').json()['items'], [])

    def test_cash_movement_scope_dates_and_numeric_validation(self):
        self.initialize()
        cash = self.post('cash-flows', {'direction': 'DEPOSIT', 'amount': 1000,
                         'effective_date': '2026-10-16', 'idempotency_key': 'same-key'})
        self.assertEqual(cash.status_code, 200)
        # Keys are local to an operation family, so a fill may independently use this key.
        self.buy('same-key')
        self.assertEqual(self.get('cash-flows').json()['total'], 2)
        self.assertEqual(self.get('account').json()['account']['net_deposits'], 11000)
        invalid = self.post('trade', {'instrument': '600000', 'direction': 'BUY',
                            'quantity': 1.5, 'price': 1, 'idempotency_key': 'fraction'})
        self.assertEqual(invalid.status_code, 422)
        backdate = self.post('cash-flows', {'direction': 'DEPOSIT', 'amount': 100,
                             'effective_date': '2026-10-15', 'idempotency_key': 'backdate'})
        self.assertEqual(backdate.status_code, 409)
        self.assertEqual(self.get('trades?limit=-1').status_code, 422)

    def test_personal_ownership_and_explicit_legacy_visibility(self):
        self.initialize()
        self.buy()
        self.active_user = 2
        self.assertFalse(self.get('account').json()['initialized'])
        self.assertEqual(self.get('trades').json()['total'], 0)
        self.assertEqual(self.get('legacy-trades').json()['total'], 1)
        self.assertEqual(self.get('legacy-trades?include_unassigned=true').status_code, 403)
        self.admin = True
        self.assertEqual(self.get('legacy-trades?include_unassigned=true').json()['total'], 2)
        self.assertEqual(self.get('account').json()['legacy_unassigned_count'], 1)

    def test_stale_quotes_do_not_become_current_equity(self):
        self.initialize()
        self.buy()
        self.market.return_value = {'SH600000': {'price': 10, 'status': 'stale', 'source': 'test',
                                               'as_of': '2026-10-16T15:00:00+08:00'}}
        response = self.get('positions').json()
        self.assertFalse(response['valuation_complete'])
        self.assertIsNone(response['total_value'])
        self.assertEqual(response['positions'][0]['last_known_price'], 10)
        self.assertEqual(response['positions'][0]['price_status'], 'stale')
        self.assertTrue(response['valuation_warnings'])

    def test_commit_failure_does_not_emit_success_or_change_money(self):
        self.initialize()
        with patch.object(Session, 'commit', side_effect=RuntimeError('simulated disk failure')):
            with self.assertRaisesRegex(RuntimeError, 'disk failure'):
                self.post('cash-flows', {'direction': 'WITHDRAW', 'amount': 100,
                          'idempotency_key': 'disk-failure'})
        self.assertEqual(self.get('account').json()['account']['cash'], 10000)
        self.assertEqual(self.get('cash-flows').json()['total'], 1)
