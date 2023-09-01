from strategy.Strategy import *
from util.make_up_universe import *
from api.Kiwoom import *
import pandas as pd
import talib


class BollingerStrategy():

    def __init__(self):
        # investment must be made proportional to the weight
        self.s = Strategy("BollingerStrategy", self.get_universe, self.check_buy_signal, self.check_sell_signal)
        self.s.start()

        self.weight = {}
        self.universe_close = {}

        self.invest_rate = 0.6

        self.trade_when = True

        self.init()

    def get_universe(self):
        # 크롤링 결과를 얻어옴
        df = execute_crawler()

        mapping = {',': '', 'N/A': '0'}
        df.replace(mapping, regex=True, inplace=True)

        # 사용할 column들 설정
        cols = ['거래량', '매출액', '매출액증가율', 'ROE', 'PER']

        # column들을 숫자타입으로 변환(Naver Finance를 크롤링해온 데이터는 str 형태)
        df[cols] = df[cols].astype(float)

        # 유니버스 구성 조건 (1)~(4)를 만족하는 데이터 가져오기
        df = df[(df['거래량'] > 0) & (df['매출액'] > 0) & (df['매출액증가율'] > 0) & (df['ROE'] > 0) & (df['PER'] > 0)]

        # PER의 역수
        df['1/PER'] = 1 / df['PER']

        # ROE의 순위 계산
        df['RANK_ROE'] = df['ROE'].rank(method='max', ascending=False)

        # 1/PER의 순위 계산
        df['RANK_1/PER'] = df['1/PER'].rank(method='max', ascending=False)

        # ROE 순위, 1/PER 순위 합산한 랭킹
        df['RANK_VALUE'] = (df['RANK_ROE'] + df['RANK_1/PER']) / 2

        # RANK_VALUE을 기준으로 정렬
        df = df.sort_values(by=['RANK_VALUE'])

        # 필터링한 데이터프레임의 index 번호를 새로 매김
        df.reset_index(inplace=True, drop=True)

        # 상위 200개만 추출
        df = df.loc[:199]

        # 유니버스 생성 결과를 엑셀 출력
        df.to_excel('universe.xlsx')
        return df['종목명'].tolist()

    def init(self):
        for code in self.s.universe.keys():
            self.weight[code] = 0.005
            if code in self.s.kiwoom.universe_realtime_transaction_info.keys():
                self.universe_close[code] = self.s.kiwoom.universe_realtime_transaction_info[code]['현재가']

    def alpha(self, code):

        universe_item = self.s.universe[code]

        # Fetch data
        # 현재 체결정보가 존재하지 않는지 확인
        if code not in self.s.kiwoom.universe_realtime_transaction_info.keys():
            # 존재하지 않다면 더이상 진행하지 않고 함수 종료
            return

        # 실시간 체결 정보가 존재하면 현 시점의 시가 / 고가 / 저가 / 현재가 / 누적 거래량이 저장되어 있음
        open = self.s.kiwoom.universe_realtime_transaction_info[code]['시가']
        high = self.s.kiwoom.universe_realtime_transaction_info[code]['고가']
        low = self.s.kiwoom.universe_realtime_transaction_info[code]['저가']
        close = self.s.kiwoom.universe_realtime_transaction_info[code]['현재가']
        volume = self.s.kiwoom.universe_realtime_transaction_info[code]['누적거래량']

        self.universe_close[code] = close

        # 오늘 가격 데이터를 과거 가격 데이터(DataFrame)의 행으로 추가하기 위해 리스트로 만듦
        today_price_data = [open, high, low, close, volume]

        df = universe_item['price_df'].copy()

        # 과거 가격 데이터에 금일 날짜로 데이터 추가
        now = datetime.now().strftime('%Y%m%d')
        df.loc[now] = today_price_data

        upper_2sd, mid_2sd, lower_2sd = talib.BBANDS(df['close'],
                                                     nbdevup=2,
                                                     nbdevdn=2,
                                                     timeperiod=20)

        # Update weight
        if close > upper_2sd or close < lower_2sd:
            self.weight[code] = (mid_2sd - close) / close
        else:
            self.weight[code] = 0

    def get_quantity(self, code):
        deposit = self.s.kiwoom.get_deposit()

        avg_close = np.mean(list(self.universe_close.values()))

        avg_balance_count = deposit / avg_close

        self.alpha(code)  # update the weight for code

        normalized_weight = self.normalized_weight(code)

        quantity = avg_balance_count * normalized_weight * self.invest_rate

        return quantity

    def normalized_weight(self, code):

        w = np.array(self.weight.values())

        w[w < 0] = 0

        s = w.sum()

        ans = self.weight[code] / s

        return ans

    def check_buy_signal(self, code):
        """매수 대상인지 확인하는 함수"""
        # 매수 가능 시간 확인
        if not check_transaction_open():
            print("장 시간이 아닙니다")
            return 0

        optimal_quantity = self.get_quantity(code)
        current_quantity = self.s.kiwoom.order[code]['주문수량'] + self.s.kiwoom.balance[code]['보유수량']

        # 이 방법으로 너무 수수료 손실이 클 경우 변화폭이 클 경우에만 거래하도록 코드 수정.
        if optimal_quantity > current_quantity and self.trade_when:
            return optimal_quantity - current_quantity
        else:
            return 0

    def check_sell_signal(self, code):
        """매도대상인지 확인하는 함수"""
        # 매도 가능 시간 확인
        if not check_transaction_open():
            print("장 시간이 아닙니다")
            return 0

        optimal_quantity = self.get_quantity(code)
        current_quantity = self.s.kiwoom.order[code]['주문수량'] + self.s.kiwoom.balance[code]['보유수량']

        if optimal_quantity < current_quantity and self.trade_when:
            return current_quantity - optimal_quantity
        else:
            return 0