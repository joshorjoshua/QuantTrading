from strategy.Tutorial_RSIStrategy import *
from strategy.BollingerStrategy import *
from api.Kiwoom import *
import sys


# pip install pyqt5 pandas requests bs4 lxml openpyxl numpy

app = QApplication(sys.argv)

strategy = BollingerStrategy()

# sell_all = SellAll()
# sell_all.start()

# kiwoom = Kiwoom()
# kiwoom.get_deposit()

app.exec_()

"""
class A:
    def __init__(self):
        self.b = B(self.f)

    def f(self):
        self.b.z()
        print("야호")


class B:
    def __init__(self, f):
        self.f = f

    def y(self):
        x = self.f()
        print(x)

    def z(self):
        print("왜불러")


a = A()
a.f()
"""
