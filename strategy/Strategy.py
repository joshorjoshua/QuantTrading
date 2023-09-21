import time

from api.Kiwoom import *
from util.db_helper import *
from util.time_helper import *
from util.notifier import *
import math
import traceback


class Strategy(QThread):
    def __init__(self, strategy_name, get_universe, check_buy_signal, check_sell_signal):
        QThread.__init__(self)
        self.strategy_name = strategy_name
        self.kiwoom = Kiwoom()

        self.universe = {}

        self.round_deposit = 0
        self.deposit = 0

        self.order_wait = 60  # 체결대기시간 1분

        self.get_universe = get_universe
        self.check_buy_signal = check_buy_signal
        self.check_sell_signal = check_sell_signal

        self.is_init_success = False

        self.init_strategy()

    def init_strategy(self):
        try:
            # 유니버스 조회, 없으면 생성
            self.check_and_get_universe()

            # 가격 정보를 조회, 필요하면 생성
            self.check_and_get_price_data()

            # Kiwoom > 주문정보 확인
            self.kiwoom.get_order()

            # Kiwoom > 잔고 확인
            self.kiwoom.get_balance()

            # Kiwoom > 예수금 확인
            self.deposit = self.kiwoom.get_deposit()

            # 유니버스 실시간 체결정보 등록
            self.set_universe_real_time()

            self.is_init_success = True

        except Exception as e:
            print(traceback.format_exc())
            # LINE 메시지를 보내는 부분
            send_message(traceback.format_exc(), RSI_STRATEGY_MESSAGE_TOKEN)

    def run(self):
        """실질적 수행 역할을 하는 함수"""
        while self.is_init_success:
            try:
                # 장중인지 확인
                if not check_transaction_open():
                    print("장시간이 아니므로 5분간 대기합니다.")
                    time.sleep(5 * 60)
                    continue

                for idx, code in enumerate(self.universe.keys()):  # for each code in the universe
                    print(self.deposit)

                    print('[{}/{}_{}]'.format(idx + 1, len(self.universe), self.universe[code]['code_name']))
                    time.sleep(0.5)

                    if idx == 0:
                        self.round_deposit = self.deposit

                    # 접수한 주문이 있는지 확인
                    if code in self.kiwoom.order.keys():
                        # 미체결시간 초과시 주문 취소
                        print(self.kiwoom.order[code]['주문시간'])
                        if self.kiwoom.order[code]['미체결수량'] > 0 and (
                                datetime.now() - self.kiwoom.order[code]['datetime']).total_seconds() > self.order_wait:
                            if self.kiwoom.order[code]['주문구분'] == '매수':
                                self.cancel_buy_order(code)
                            elif self.kiwoom.order[code]['주문구분'] == '매도':
                                self.cancel_sell_order(code)
                    else:  # 접수한 주문이 없을 시
                        if code in self.kiwoom.universe_realtime_transaction_info.keys():
                            # 보유 종목인지 확인
                            if code in self.kiwoom.balance.keys():
                                # 매도
                                self.check_sell_signal_and_order(code)
                            # 매수
                            self.check_buy_signal_and_order(code)

            except Exception as e:
                print(traceback.format_exc())
                # LINE 메시지를 보내는 부분
                send_message(traceback.format_exc(), RSI_STRATEGY_MESSAGE_TOKEN)

    def check_and_get_universe(self):  # 유니버스 주기적으로 업데이트 하게 하는 코드 필요
        """유니버스가 존재하는지 확인하고 없으면 생성하는 함수"""
        if (not check_table_exist(self.strategy_name, 'universe')) or datetime.today().day == 1:
            universe_list = self.get_universe()
            print(universe_list)
            universe = {}
            # 오늘 날짜를 20210101 형태로 지정
            now = datetime.now().strftime("%Y%m%d")

            # KOSPI(0)에 상장된 모든 종목 코드를 가져와 kospi_code_list에 저장
            kospi_code_list = self.kiwoom.get_code_list_by_market("0")

            # KOSDAQ(10)에 상장된 모든 종목 코드를 가져와 kosdaq_code_list에 저장
            kosdaq_code_list = self.kiwoom.get_code_list_by_market("10")

            for code in kospi_code_list + kosdaq_code_list:
                # 모든 종목 코드를 바탕으로 반복문 수행
                code_name = self.kiwoom.get_master_code_name(code)

                # 얻어온 종목명이 유니버스에 포함되어 있다면 딕셔너리에 추가
                if code_name in universe_list:
                    universe[code] = code_name

            # 코드, 종목명, 생성일자자를 열로 가지는 DataFrame 생성
            universe_df = pd.DataFrame({
                'code': universe.keys(),
                'code_name': universe.values(),
                'created_at': [now] * len(universe.keys())
            })

            # universe라는 테이블명으로 Dataframe을 DB에 저장함
            insert_df_to_db(self.strategy_name, 'universe', universe_df)

        sql = "select * from universe"
        cur = execute_sql(self.strategy_name, sql)
        universe_list = cur.fetchall()
        for item in universe_list:
            idx, code, code_name, created_at = item
            self.universe[code] = {
                'code_name': code_name
            }
        print(self.universe)

    def check_and_get_price_data(self):
        """일봉 데이터가 존재하는지 확인하고 없다면 생성하는 함수"""
        for idx, code in enumerate(self.universe.keys()):
            print("({}/{}) {}".format(idx + 1, len(self.universe), code))

            if check_table_exist(self.strategy_name, code):
                if check_transaction_closed():
                    print("장 종료 시간입니다. 데이터베이스 업데이트를 시작합니다.")
                    # 저장된 데이터의 가장 최근 일자를 조회
                    sql = "select max(`{}`) from `{}`".format('index', code)

                    cur = execute_sql(self.strategy_name, sql)

                    # 일봉 데이터를 저장한 가장 최근 일자를 조회
                    last_date = cur.fetchone()

                    # 오늘 날짜를 20210101 형태로 지정
                    now = datetime.now().strftime("%Y%m%d")

                    # 최근 저장 일자가 오늘이 아닌지 확인
                    if last_date[0] != now:
                        price_df = self.kiwoom.get_price_data(code)
                        time.sleep(0.5)
                        # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                        insert_df_to_db(self.strategy_name, code, price_df)
                        self.universe[code]['price_df'] = price_df
                else:
                    print("데이터베이스에서 일봉데이터를 불러옵니다.")
                    sql = "select * from `{}`".format(code)
                    cur = execute_sql(self.strategy_name, sql)
                    cols = [column[0] for column in cur.description]

                    # 데이터베이스에서 조회한 데이터를 DataFrame으로 변환해서 저장
                    price_df = pd.DataFrame.from_records(data=cur.fetchall(), columns=cols)
                    price_df = price_df.set_index('index')
                    # 가격 데이터를 self.universe에서 접근할 수 있도록 저장
                    self.universe[code]['price_df'] = price_df
            else:
                if check_transaction_closed():
                    print("장 종료 시간입니다. 금일 데이터 포함 일봉 정보를 다운로드합니다.")
                    # API를 이용해 조회한 가격 데이터 price_df에 저장
                    price_df = self.kiwoom.get_price_data(code)
                    # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                    insert_df_to_db(self.strategy_name, code, price_df)
                    time.sleep(0.5)
                else:
                    print("장 종료 시간 전입니다. 금일 데이터만 제외한 일봉 정보를 다운로드합니다.")
                    # API를 이용해 조회한 가격 데이터 price_df에 저장
                    price_df = self.kiwoom.get_price_data(code)
                    # 금일 데이터 제외
                    now = datetime.now().strftime("%Y%m%d")
                    if now in price_df.index:
                        price_df.drop(now)

                    # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                    insert_df_to_db(self.strategy_name, code, price_df)
                    time.sleep(0.5)

    def cancel_buy_order(self, code):
        print('미체결 시간이 초과되어 주문이 취소됩니다')

        quantity = self.kiwoom.order[code]['주문수량']
        bid = self.kiwoom.order[code]['주문가격']
        origin_order_number = self.kiwoom.order[code]['원주문번호']

        order_result = self.kiwoom.send_order('cancel_buy_order', '1011', 3, code, quantity, 0, '00', origin_order_number)

        # LINE 메시지를 보내는 부분
        message = "[{}]buy order cancelled. quantity:{}, bid:{}, order_result:{}, deposit:{}, get_balance_count:{}, get_buy_order_count:{}, balance_len:{}".format(
            code, quantity, bid, order_result, self.deposit, self.get_balance_count(), self.get_buy_order_count(),
            len(self.kiwoom.balance))
        send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

    def cancel_sell_order(self, code):
        print('미체결 시간이 초과되어 주문이 취소됩니다')

        quantity = self.kiwoom.order[code]['주문수량']
        ask = self.kiwoom.order[code]['주문가격']
        origin_order_number = self.kiwoom.order[code]['원주문번호']

        order_result = self.kiwoom.send_order('cancel_sell_order', '1011', 4, code, quantity, 0, '00', origin_order_number)

        # LINE 메시지를 보내는 부분
        message = "[{}]sell order is cancelled. quantity:{}, ask:{}, order_result:{}".format(code, quantity, ask,
                                                                                             order_result)
        send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

    def order_buy(self, code, quantity):
        if quantity < 1:
            return

        bid = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매수호가']

        # 현재 예수금에서 수수료를 곱한 실제 투입금액(주문 수량 * 주문 가격)을 제외해서 계산
        amount = quantity * bid
        new_deposit = math.floor(self.deposit - amount * 1.00015)

        # 예수금이 0보다 작아질 정도로 주문할 수는 없으므로 체크
        if new_deposit < 0:
            return
        else:
            self.deposit = new_deposit

        # 계산을 바탕으로 지정가 매수 주문 접수
        order_result = self.kiwoom.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')

        # _on_chejan_slot가 늦게 동작할 수도 있기 때문에 미리 약간의 정보를 넣어둠
        self.kiwoom.order[code] = {'주문구분': '매수', '미체결수량': quantity}

        # LINE 메시지를 보내는 부분
        message = "[{}]buy order is done! quantity:{}, bid:{}, order_result:{}, deposit:{}, get_balance_count:{}, get_buy_order_count:{}, balance_len:{}".format(
            code, quantity, bid, order_result, self.deposit, self.get_balance_count(), self.get_buy_order_count(),
            len(self.kiwoom.balance))
        send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

    def order_sell(self, code, quantity):
        if quantity < 1:
            return

        # 보유수량보다 많이 팔수는 없음
        if self.kiwoom.balance[code]['보유수량'] < quantity:
            return self.kiwoom.balance[code]['보유수량']

        # 최우선 매도 호가 확인
        ask = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매도호가']

        order_result = self.kiwoom.send_order('send_sell_order', '1001', 2, code, quantity, ask, '00')

        # LINE 메시지를 보내는 부분
        message = "[{}]sell order is done! quantity:{}, ask:{}, order_result:{}".format(code, quantity, ask,
                                                                                        order_result)
        send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)

    def check_buy_signal_and_order(self, code):
        quantity = self.check_buy_signal(code)
        if quantity > 0:
            self.order_buy(code, quantity)

    def check_sell_signal_and_order(self, code):
        quantity = self.check_sell_signal(code)
        if quantity > 0:
            self.order_sell(code, quantity)

    def set_universe_real_time(self):
        """유니버스 실시간 체결정보 수신 등록하는 함수"""
        # 임의의 fid를 하나 전달하기 위한 코드(아무 값의 fid라도 하나 이상 전달해야 정보를 얻어올 수 있음)
        fids = get_fid("체결시간")

        # 장운영구분을 확인하고 싶으면 사용할 코드
        # self.kiwoom.set_real_reg("1000", "", get_fid("장운영구분"), "0")

        # universe 딕셔너리의 key값들은 종목코드들을 의미
        codes = self.universe.keys()

        # 종목코드들을 ';'을 기준으로 묶어주는 작업
        codes = ";".join(map(str, codes))

        # 화면번호 9999에 종목코드들의 실시간 체결정보 수신을 요청
        self.kiwoom.set_real_reg("9999", codes, fids, "0")

    def get_balance_count(self):
        """매도 주문이 접수되지 않은 보유 종목 수를 계산하는 함수"""
        balance_count = len(self.kiwoom.balance)
        # kiwoom balance에 존재하는 종목이 매도 주문 접수되었다면 보유 종목에서 제외시킴
        for code in self.kiwoom.order.keys():
            if code in self.kiwoom.balance and self.kiwoom.order[code]['주문구분'] == "매도" and self.kiwoom.order[code][
                '미체결수량'] == 0:
                balance_count = balance_count - 1
        return balance_count

    def get_buy_order_count(self):
        """매수 주문 종목 수를 계산하는 함수"""
        buy_order_count = 0
        # 아직 체결이 완료되지 않은 매수 주문
        for code in self.kiwoom.order.keys():
            if code not in self.kiwoom.balance and self.kiwoom.order[code]['주문구분'] == "매수":
                buy_order_count = buy_order_count + 1
        return buy_order_count
