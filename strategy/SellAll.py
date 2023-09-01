from api.Kiwoom import *
from util.notifier import *
class SellAll(QThread):
    def __init__(self):
        QThread.__init__(self)
        self.strategy_name = "SellAll"
        self.kiwoom = Kiwoom()

    def run(self):
        for code in self.kiwoom.balance.keys():
            self.order_sell(code)

    def order_sell(self, code):
        """매도 주문 접수 함수"""
        # 보유 수량 확인(전량 매도 방식으로 보유한 수량을 모두 매도함)
        quantity = self.kiwoom.balance[code]['보유수량']

        # 최우선 매도 호가 확인
        ask = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매도호가']

        order_result = self.kiwoom.send_order('send_sell_order', '1001', 2, code, quantity, ask, '00')

        # LINE 메시지를 보내는 부분
        message = "[{}]sell order is done! quantity:{}, ask:{}, order_result:{}".format(code, quantity, ask,
                                                                                        order_result)
        send_message(message, RSI_STRATEGY_MESSAGE_TOKEN)
