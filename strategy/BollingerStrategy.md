# Bollinger Strategy

## get_universe
Function that selects universe. Is called within self.s.

## alpha(code)
Function that takes code as input, and updates weight[code]. 

This function is the core of the strategy, and is unique to all strategies

## check_buy_signal(code), check_sell_signal(code)
Function that takes code as input, and returns the quantity to be bought/sold.

This function is called within self.s.

It calculates the optimal quantity by calling the get_quantity function.

Then it calculates the amount to be bought/sold to reach the optimal quantity.

## get_quantity
Function that takes code as input, and returns the optimal quantity.

The optimal quantity is calculated based on weights set by alpha, deposit, invest_rate,
and average number of stocks in balance with the current deposit.

If weight is positive, return the appropriate value based on the calculations. ()

If weight is negative, return 0. (for now, but may implement scale-out strategy to reduce mistake)

## weight
Dictionary that stores a float value for each code.

weight must always take the value between -1 and 1. (-1 <= weight[code] <= 1)

0 is the neutral position, and negative and positive values each correspond to short and long positions.

## init
Initializes the weight and universe_close 