//@version=5
strategy("Hull Suite BUY ONLY - V3 NO PINE CANCEL", shorttitle="HULL BOT V3", overlay=true, initial_capital=100, pyramiding=20, commission_type=strategy.commission.percent, commission_value=0, process_orders_on_close=true, calc_on_order_fills=true, calc_on_every_tick=true)

// Converted to BUY ONLY framework.
// Auto buffer added: Manual Ticks or Auto ATR % with min/max protection.
// Original strategy take-profit added beside Fixed RR.
// Original entry/exit logic is preserved. Bot layer only adds webhook JSON + safety fields. V2 shortens webhook reasons for DB safety.
// Original entry logic is used only for BUY signals. SELL/SHORT entries are removed.

//====================================================
// 00 - SIGNAL SOURCE / RENKO
//====================================================
grpRenko = "00 - SIGNAL SOURCE / RENKO"
signalSource = input.string("Chart Renko", "Signal Source", options=["Chart Renko", "Normal Chart", "Renko ATR", "Renko Traditional"], group=grpRenko)
renkoATRLength = input.int(14, "Renko ATR Length", minval=1, group=grpRenko)
renkoTraditionalBox = input.float(10.0, "Renko Traditional Box Size", minval=0.0000001, step=0.1, group=grpRenko)
renkoWicks = input.bool(true, "Renko Wicks", group=grpRenko)
renkoCalculationSource = input.string("Close", "Renko Calculation Source", options=["Close", "Open", "High", "Low", "HL2", "HLC3", "OHLC4"], group=grpRenko)
renkoSignalTiming = input.string("Confirmed Bar", "Renko Signal Timing", options=["Confirmed Bar", "Live Bar"], group=grpRenko)
renkoEntryExecution = input.string("Same Bar", "Renko Entry Execution", options=["Same Bar", "Next Bar"], group=grpRenko)
renkoEntryLimit = input.string("One Signal Per Renko Brick", "Renko Entry Limit", options=["One Signal Per Renko Brick", "Every Signal"], group=grpRenko)
stopLossPriceSource = input.string("Normal Chart", "Stop Loss Price Source", options=["Normal Chart", "Signal Source"], group=grpRenko)
maxRenkoStoredBricks = input.int(5000, "Max Renko Stored Bricks", minval=100, group=grpRenko)

// Chart Renko = read the current TradingView Renko chart OHLC directly.
// Renko ATR / Renko Traditional = internal ticker.renko() mode, kept optional only.
useInternalRenko = signalSource == "Renko ATR" or signalSource == "Renko Traditional"
useChartRenko = signalSource == "Chart Renko"
useRenko = useChartRenko or useInternalRenko
renkoTicker = signalSource == "Renko ATR" ? ticker.renko(syminfo.tickerid, "ATR", renkoATRLength) : ticker.renko(syminfo.tickerid, "Traditional", renkoTraditionalBox)
[rOpenRaw, rHighRaw, rLowRaw, rCloseRaw] = request.security(renkoTicker, timeframe.period, [open, high, low, close], gaps=barmerge.gaps_off, lookahead=barmerge.lookahead_off)

rOpen = useInternalRenko ? rOpenRaw : open
rHigh = useInternalRenko ? rHighRaw : high
rLow = useInternalRenko ? rLowRaw : low
rClose = useInternalRenko ? rCloseRaw : close

sigOpen = useRenko ? rOpen : open
sigHigh = useRenko ? (renkoWicks ? rHigh : math.max(rOpen, rClose)) : high
sigLow = useRenko ? (renkoWicks ? rLow : math.min(rOpen, rClose)) : low
sigClose = useRenko ? rClose : close

f_src(_o, _h, _l, _c, _mode) =>
    switch _mode
        "Open" => _o
        "High" => _h
        "Low" => _l
        "HL2" => (_h + _l) / 2.0
        "HLC3" => (_h + _l + _c) / 3.0
        "OHLC4" => (_o + _h + _l + _c) / 4.0
        => _c

signalPrice = f_src(sigOpen, sigHigh, sigLow, sigClose, renkoCalculationSource)
signalHL2 = (sigHigh + sigLow) / 2.0
signalHLC3 = (sigHigh + sigLow + sigClose) / 3.0
signalOHLC4 = (sigOpen + sigHigh + sigLow + sigClose) / 4.0
renkoTimingOK = renkoSignalTiming == "Live Bar" ? true : barstate.isconfirmed

//====================================================
// 05 - STRATEGY RESULT
//====================================================
grpResult = "05 - STRATEGY RESULT"
stopLossMode = input.string("Below Touch Candle", "Stop Loss Mode", options=["Below Touch Candle", "Below Previous Low X Candles", "Below Hull Band"], group=grpResult)
previousLowXCandles = input.int(3, "Previous Low X Candles", minval=1, group=grpResult)
slBufferTicks = input.int(30, "Manual SL Buffer Ticks", minval=0, group=grpResult)
slBufferMode = input.string("Auto ATR %", "SL Buffer Mode", options=["Manual Ticks", "Auto ATR %"], group=grpResult)
autoSLBufferAtrPercent = input.float(10.0, "Auto SL Buffer ATR %", minval=0.0, step=0.5, group=grpResult)
autoSLBufferMinTicks = input.int(2, "Auto SL Buffer Min Ticks", minval=0, group=grpResult)
autoSLBufferMaxAtrPercent = input.float(25.0, "Auto SL Buffer Max ATR %", minval=0.0, step=0.5, group=grpResult)
takeProfitRR = input.float(2.0, "Take Profit RR", minval=0.1, step=0.1, group=grpResult)
takeProfitMode = input.string("Fixed RR", "Take Profit Mode", options=["Fixed RR", "Original Strategy Exit", "Fixed RR or Original Strategy Exit"], group=grpResult)
showOriginalTPExitSignals = input.bool(false, "Show Original Strategy Exit Signal", group=grpResult)
sameCandleRule = input.string("SL First", "If TP And SL Hit Same Candle", options=["SL First", "TP First"], group=grpResult)
allowExitOnEntryCandle = input.bool(true, "Allow Exit On Entry Candle", group=grpResult)
allowOverlappingTrades = input.bool(true, "Allow Overlapping Trades", group=grpResult)
maxOpenTrades = input.int(3, "Max Open Trades", minval=1, maxval=20, group=grpResult)
entryMode = input.string("At Trend Signal", "Entry Mode", options=["At Trend Signal", "Next Bar"], group=grpResult)

//====================================================
// 06 - MONEY / SIZING
//====================================================
grpMoney = "06 - MONEY / SIZING"
initialCapitalInput = input.float(100.0, "Initial Capital", minval=1, step=1, group=grpMoney)
tradeValueMode = input.string("Equity %", "Trade Value Mode", options=["Fixed $", "Equity %"], group=grpMoney)
fixedTradeValue = input.float(10.0, "Fixed Trade Value $", minval=0.01, step=1, group=grpMoney)
equityPercentTradeValue = input.float(100.0, "Equity % Trade Value", minval=0.01, maxval=1000, step=1, group=grpMoney)
useCompounding = input.bool(true, "Use Compounding For Equity % Mode", group=grpMoney)
maxLossCapON = input.bool(true, "Max Loss Cap ON", group=grpMoney)
maxLossMode = input.string("% of Equity", "Max Loss Mode", options=["Fixed $", "% of Trade Value", "% of Equity"], group=grpMoney)
maxLossFixed = input.float(5.0, "Max Loss Fixed $", minval=0.01, step=1, group=grpMoney)
maxLossPercent = input.float(5.0, "Max Loss %", minval=0.01, maxval=100, step=0.1, group=grpMoney)
maxLossAction = input.string("Reduce Size", "Max Loss Action", options=["Reduce Size", "Skip Trade"], group=grpMoney)

//====================================================
// 07 - COST / EXECUTION
//====================================================
grpCost = "07 - COST / EXECUTION"
costPreset = input.string("Smart Auto", "Cost Preset", options=["Smart Auto", "Binance Spot Regular", "Binance Spot BNB Discount", "Rain Pro", "Crypto Custom", "Stock/ETF Auto Tight", "Stock/ETF Auto Normal", "Stock/ETF Auto Wide", "Stock/ETF Custom", "No Cost"], group=grpCost)
executionProtection = input.string("Easy", "Execution Protection", options=["Easy", "Normal", "Strict"], group=grpCost)
cryptoExecutionType = input.string("Taker", "Crypto Execution Type", options=["Maker", "Taker"], group=grpCost)
accountFeeDiscountPercent = input.float(0.0, "Account Fee Discount %", minval=0, maxval=100, step=0.1, group=grpCost)
customCryptoMakerFee = input.float(0.10, "Custom Crypto Maker Fee % Per Side", minval=0, step=0.001, group=grpCost)
customCryptoTakerFee = input.float(0.10, "Custom Crypto Taker Fee % Per Side", minval=0, step=0.001, group=grpCost)
customStockCommission = input.float(0.0, "Custom Stock/ETF Commission $ Per Side", minval=0, step=0.01, group=grpCost)
customStockSpreadTicks = input.float(1.0, "Custom Stock/ETF Spread Ticks", minval=0, step=0.1, group=grpCost)

isCrypto = syminfo.type == "crypto"
float feePctRaw = switch costPreset
    "Smart Auto" => isCrypto ? 0.10 : 0.00
    "Binance Spot Regular" => 0.10
    "Binance Spot BNB Discount" => 0.075
    "Rain Pro" => 0.10
    "Crypto Custom" => cryptoExecutionType == "Maker" ? customCryptoMakerFee : customCryptoTakerFee
    "No Cost" => 0.00
    => 0.00
feePctPerSide = math.max(feePctRaw * (1.0 - accountFeeDiscountPercent / 100.0), 0.0)

float baseSpreadTicks = switch costPreset
    "Smart Auto" => 1.0
    "Binance Spot Regular" => 1.0
    "Binance Spot BNB Discount" => 1.0
    "Rain Pro" => 1.0
    "Crypto Custom" => 1.0
    "Stock/ETF Auto Tight" => 1.0
    "Stock/ETF Auto Normal" => 2.0
    "Stock/ETF Auto Wide" => 4.0
    "Stock/ETF Custom" => customStockSpreadTicks
    "No Cost" => 0.0
    => 1.0
float protectionMultiplier = switch executionProtection
    "Easy" => 1.0
    "Normal" => 1.5
    "Strict" => 2.0
float slippageTicksPerSide = switch executionProtection
    "Easy" => 0.0
    "Normal" => 1.0
    "Strict" => 2.0
spreadTicksEstimate = costPreset == "No Cost" ? 0.0 : baseSpreadTicks * protectionMultiplier
isStockCostPreset = costPreset == "Stock/ETF Auto Tight" or costPreset == "Stock/ETF Auto Normal" or costPreset == "Stock/ETF Auto Wide" or costPreset == "Stock/ETF Custom"

//====================================================
// 08 - REAL EXECUTION SAFETY
//====================================================
grpSafety = "08 - REAL EXECUTION SAFETY"
exposureCapON = input.bool(true, "Exposure Cap ON", group=grpSafety)
maxTotalOpenExposurePercent = input.float(100.0, "Max Total Open Exposure % of Equity", minval=1, maxval=1000, step=1, group=grpSafety)
sessionFilterON = input.bool(false, "Session Filter ON", group=grpSafety)
allowedSession = input.session("0000-2359", "Allowed Session", group=grpSafety)
sessionOK = sessionFilterON ? not na(time(timeframe.period, allowedSession)) : true

//====================================================
// 09 - PERIOD / DAILY GOAL
//====================================================
grpPeriod = "09 - PERIOD / DAILY GOAL"
dailyTargetPercent = input.float(2.0, "Daily Target %", minval=0.0, step=0.1, group=grpPeriod)

//====================================================
// 10 - BREAK EVEN STEP
//====================================================
grpBE = "10 - BREAK EVEN STEP"
useBreakEvenStep = input.bool(true, "Use Break Even Step", group=grpBE)
moveStepInFavorR = input.float(0.5, "Move Step In Favor R", minval=0.1, step=0.1, group=grpBE)
lockProfitEachStepR = input.float(0.1, "Lock Profit Each Step R", minval=0.0, step=0.1, group=grpBE)
beCalculationSource = input.string("Close", "BE Calculation Source", options=["Close", "High"], group=grpBE)
preventUnrealisticBELock = input.bool(true, "Prevent Unrealistic BE Lock", group=grpBE)

//====================================================
// 11 - COLORS
//====================================================
grpColors = "11 - COLORS"
buySignalColor = input.color(color.lime, "Buy Signal Color", group=grpColors)

//====================================================
// 12 - BOT WEBHOOK / RAILWAY
//====================================================
grpBot = "12 - BOT WEBHOOK / RAILWAY"
enableBotAlerts = input.bool(true, "Enable Bot Alerts", group=grpBot)
botSymbolMode = input.string("Auto", "Symbol Mode", options=["Auto", "Manual"], group=grpBot)
manualBotSymbol = input.string("", "Manual Symbol", group=grpBot)
sendCancelOnOriginalExit = false  // V3: disabled. New setup replaces old pending in the bot; no Pine cancel spam.

botSymbolRaw = botSymbolMode == "Manual" and str.length(manualBotSymbol) > 0 ? manualBotSymbol : syminfo.ticker
botSymbol = str.upper(str.replace_all(botSymbolRaw, " ", ""))

f_num(_x) =>
    str.tostring(_x, "#.##########")

f_entry_json(_id, _entry, _sl, _tp, _qty) =>
    "{\"action\":\"PLACE_BUY_STOP\",\"symbol\":\"" + botSymbol + "\",\"entry\":" + f_num(_entry) + ",\"backup_sl\":" + f_num(_sl) + ",\"tp\":" + f_num(_tp) + ",\"qty\":" + f_num(_qty) + ",\"strategy\":\"HULL_SUITE_V1\",\"trade_id\":\"" + _id + "\"}"

f_exit_json(_reason, _price) =>
    "{\"action\":\"EXIT\",\"symbol\":\"" + botSymbol + "\",\"exit_reason\":\"" + _reason + "\",\"exit_price\":" + f_num(_price) + ",\"pnl\":0}"

f_update_sl_json(_sl) =>
    "{\"action\":\"UPDATE_BACKUP_SL\",\"symbol\":\"" + botSymbol + "\",\"backup_sl\":" + f_num(_sl) + "}"

f_cancel_json(_reason) =>
    "{\"action\":\"CANCEL_PENDING\",\"symbol\":\"" + botSymbol + "\",\"reason\":\"" + _reason + "\"}"

//====================================================
// 01 - ORIGINAL HULL SUITE LOGIC
//====================================================
grpOriginal = "01 - ORIGINAL HULL SUITE"
modeSwitch = input.string("Hma", "Hull Variation", options=["Hma", "Thma", "Ehma"], group=grpOriginal)
length = input.int(55, "Length", minval=1, group=grpOriginal)
hullEntryMode = input.string("Trend Cross", "Hull Entry Mode", options=["Trend Cross", "Trend State"], group=grpOriginal)
switchColor = input.bool(true, "Color Hull According To Trend?", group=grpOriginal)
candleCol = input.bool(false, "Color Candles Based On Hull Trend?", group=grpOriginal)
visualSwitch = input.bool(true, "Show As A Band?", group=grpOriginal)
thicknesSwitch = input.int(1, "Line Thickness", minval=1, group=grpOriginal)
transpSwitch = input.int(40, "Band Transparency", minval=0, maxval=100, step=5, group=grpOriginal)
showBuySignals = input.bool(true, "Show Buy Signals", group=grpOriginal)

src = signalPrice
f_hma(_src, _length) =>
    ta.wma(2.0 * ta.wma(_src, math.max(1, int(_length / 2))) - ta.wma(_src, _length), math.max(1, int(math.round(math.sqrt(_length)))))
f_ehma(_src, _length) =>
    ta.ema(2.0 * ta.ema(_src, math.max(1, int(_length / 2))) - ta.ema(_src, _length), math.max(1, int(math.round(math.sqrt(_length)))))
f_thma(_src, _length) =>
    ta.wma(ta.wma(_src, math.max(1, int(_length / 3))) * 3.0 - ta.wma(_src, math.max(1, int(_length / 2))) - ta.wma(_src, _length), _length)
f_mode(_mode, _src, _len) =>
    switch _mode
        "Hma" => f_hma(_src, _len)
        "Ehma" => f_ehma(_src, _len)
        "Thma" => f_thma(_src, math.max(1, int(_len / 2)))
        => f_hma(_src, _len)
HULL = f_mode(modeSwitch, src, length)
MHULL = HULL
SHULL = HULL[2]
hullUp = MHULL > SHULL
rawBuySignalBase = hullEntryMode == "Trend State" ? hullUp : ta.crossover(MHULL, SHULL)
rawOriginalExitSignalBase = hullEntryMode == "Trend State" ? not hullUp : ta.crossunder(MHULL, SHULL)
hullColor = switchColor ? (hullUp ? color.lime : color.red) : color.orange
Fi1 = plot(MHULL, title="MHULL", color=color.new(hullColor, 50), linewidth=thicknesSwitch)
Fi2 = plot(visualSwitch ? SHULL : na, title="SHULL", color=color.new(hullColor, 50), linewidth=thicknesSwitch)
fill(Fi1, Fi2, title="Band Filler", color=color.new(hullColor, transpSwitch))
barcolor(candleCol ? hullColor : na)

//====================================================
// BUY ONLY EXECUTION ENGINE
//====================================================
rawBuySignal = rawBuySignalBase and renkoTimingOK
rawOriginalTPExitSignal = rawOriginalExitSignalBase and renkoTimingOK
originalTPExitSignal = useRenko and renkoEntryExecution == "Next Bar" ? rawOriginalTPExitSignal[1] : rawOriginalTPExitSignal
useFixedTakeProfit = takeProfitMode == "Fixed RR" or takeProfitMode == "Fixed RR or Original Strategy Exit"
useOriginalStrategyTakeProfit = takeProfitMode == "Original Strategy Exit" or takeProfitMode == "Fixed RR or Original Strategy Exit"

var float lastSignalRenkoOpen = na
var float lastSignalRenkoClose = na
sameRenkoBrickAsLastSignal = useRenko and renkoEntryLimit == "One Signal Per Renko Brick" and not na(lastSignalRenkoOpen) and not na(lastSignalRenkoClose) and rOpen == lastSignalRenkoOpen and rClose == lastSignalRenkoClose
limitedBuySignal = rawBuySignal and not sameRenkoBrickAsLastSignal
if limitedBuySignal and useRenko and renkoEntryLimit == "One Signal Per Renko Brick"
    lastSignalRenkoOpen := rOpen
    lastSignalRenkoClose := rClose

entrySignal1 = entryMode == "Next Bar" ? limitedBuySignal[1] : limitedBuySignal
buySignal = useRenko and renkoEntryExecution == "Next Bar" ? entrySignal1[1] : entrySignal1

plotshape(showBuySignals and buySignal, title="BUY", style=shape.labelup, text="BUY", location=location.belowbar, color=buySignalColor, textcolor=color.black, size=size.tiny)
alertcondition(buySignal, "BUY Signal", "BUY signal")
plotshape(showOriginalTPExitSignals and originalTPExitSignal, title="Original Strategy Exit", style=shape.labeldown, text="TP/EXIT", location=location.abovebar, color=color.orange, textcolor=color.black, size=size.tiny)
alertcondition(originalTPExitSignal, "Original Strategy Exit Signal", "Original strategy TP/exit signal")
if enableBotAlerts and sendCancelOnOriginalExit and originalTPExitSignal
    alert(f_cancel_json("ORIG_EXIT"), alert.freq_once_per_bar_close)

// Estimated cost tracking for dashboard.
var float totalEstimatedCosts = 0.0
var int lastClosedTradesCount = 0
if strategy.closedtrades > lastClosedTradesCount
    for t = lastClosedTradesCount to strategy.closedtrades - 1
        tradeSize = math.abs(strategy.closedtrades.size(t))
        entryValue = strategy.closedtrades.entry_price(t) * tradeSize
        exitValue = strategy.closedtrades.exit_price(t) * tradeSize
        feeCost = (entryValue + exitValue) * feePctPerSide / 100.0
        spreadCost = tradeSize * syminfo.mintick * spreadTicksEstimate
        slippageCost = tradeSize * syminfo.mintick * slippageTicksPerSide * 2.0
        commissionCost = isStockCostPreset ? customStockCommission * 2.0 : 0.0
        tradeCost = costPreset == "No Cost" ? 0.0 : feeCost + spreadCost + slippageCost + commissionCost
        totalEstimatedCosts += tradeCost
    lastClosedTradesCount := strategy.closedtrades

grossNetProfit = strategy.netprofit
netAfterCosts = grossNetProfit - totalEstimatedCosts
dashboardEquity = initialCapitalInput + netAfterCosts

var float dayStartNetAfterCost = 0.0
var float dayStartEquity = initialCapitalInput
newDay = ta.change(time("D")) != 0
if barstate.isfirst
    dayStartNetAfterCost := netAfterCosts
    dayStartEquity := initialCapitalInput
if newDay
    dayStartNetAfterCost := netAfterCosts
    dayStartEquity := math.max(dashboardEquity, 0.01)
dailyNet = netAfterCosts - dayStartNetAfterCost
dailyPnlPercent = dayStartEquity > 0 ? dailyNet / dayStartEquity * 100.0 : 0.0
dailyTargetOK = dailyTargetPercent <= 0 ? true : dailyPnlPercent < dailyTargetPercent

var int tradeCounter = 0
var string[] tradeIds = array.new_string()
var float[] tradeEntries = array.new_float()
var float[] tradeBaseStops = array.new_float()
var float[] tradeRisks = array.new_float()
var int[] tradeEntryBars = array.new_int()
var float[] tradeLastStops = array.new_float()

f_isOpenTrade(_id) =>
    bool found = false
    if strategy.opentrades > 0
        for i = 0 to strategy.opentrades - 1
            if strategy.opentrades.entry_id(i) == _id
                found := true
    found

normalTouchLow = low
normalPrevLow = ta.lowest(low, previousLowXCandles)
signalTouchLow = sigLow
signalPrevLow = ta.lowest(sigLow, previousLowXCandles)
slTouchSource = stopLossPriceSource == "Normal Chart" ? normalTouchLow : signalTouchLow
slPreviousSource = stopLossPriceSource == "Normal Chart" ? normalPrevLow : signalPrevLow
bufferATR = ta.atr(14)
manualSLBuffer = slBufferTicks * syminfo.mintick
autoSLBufferRaw = bufferATR * autoSLBufferAtrPercent / 100.0
autoSLBufferMin = autoSLBufferMinTicks * syminfo.mintick
autoSLBufferMax = bufferATR * autoSLBufferMaxAtrPercent / 100.0
autoSLBufferLimited = math.max(autoSLBufferMin, math.min(autoSLBufferRaw, autoSLBufferMax))
slBuffer = slBufferMode == "Manual Ticks" ? manualSLBuffer : autoSLBufferLimited
slBufferTicksApprox = syminfo.mintick > 0 ? slBuffer / syminfo.mintick : na
slBufferAtrPercentNow = bufferATR > 0 ? slBuffer / bufferATR * 100.0 : na
strategySpecificStop = math.min(MHULL, SHULL) - slBuffer
candidateStop = stopLossMode == "Below Touch Candle" ? slTouchSource - slBuffer : stopLossMode == "Below Previous Low X Candles" ? slPreviousSource - slBuffer : strategySpecificStop
entryPriceEstimate = close
riskPerUnit = entryPriceEstimate - candidateStop
validStop = not na(candidateStop) and candidateStop < entryPriceEstimate and riskPerUnit > syminfo.mintick

sizingEquityBase = useCompounding and tradeValueMode == "Equity %" ? dashboardEquity : initialCapitalInput
sizingEquity = math.max(sizingEquityBase, 0.0)
wantedTradeValue = tradeValueMode == "Fixed $" ? fixedTradeValue : sizingEquity * equityPercentTradeValue / 100.0
currentExposureValue = math.abs(strategy.position_size) * close
maxExposureValue = sizingEquity * maxTotalOpenExposurePercent / 100.0
remainingExposureValue = math.max(maxExposureValue - currentExposureValue, 0.0)
exposureAdjustedTradeValue = exposureCapON ? math.min(wantedTradeValue, remainingExposureValue) : wantedTradeValue
initialQty = close > 0 ? exposureAdjustedTradeValue / close : 0.0
initialRiskDollars = validStop ? riskPerUnit * initialQty : na
maxLossCapDollars = maxLossMode == "Fixed $" ? maxLossFixed : maxLossMode == "% of Trade Value" ? exposureAdjustedTradeValue * maxLossPercent / 100.0 : sizingEquity * maxLossPercent / 100.0
qtyAfterMaxLoss = initialQty
skipByMaxLoss = false
if maxLossCapON and validStop and not na(initialRiskDollars) and initialRiskDollars > maxLossCapDollars
    if maxLossAction == "Reduce Size"
        qtyAfterMaxLoss := initialQty * maxLossCapDollars / initialRiskDollars
    else
        skipByMaxLoss := true

overlapOK = allowOverlappingTrades ? true : strategy.position_size == 0
openTradesOK = strategy.opentrades < maxOpenTrades
qtyOK = qtyAfterMaxLoss > 0
exposureOK = exposureAdjustedTradeValue > 0
canEnter = buySignal and validStop and qtyOK and exposureOK and not skipByMaxLoss and overlapOK and openTradesOK and sessionOK and dailyTargetOK

if canEnter
    tradeCounter += 1
    newId = "L_" + str.tostring(tradeCounter)
    takeProfitForMsg = entryPriceEstimate + riskPerUnit * takeProfitRR
    entryMsg = f_entry_json(newId, entryPriceEstimate, candidateStop, takeProfitForMsg, qtyAfterMaxLoss)
    strategy.entry(id=newId, direction=strategy.long, qty=qtyAfterMaxLoss, comment="BUY", alert_message=entryMsg)
    if enableBotAlerts
        alert(entryMsg, alert.freq_once_per_bar_close)
    array.push(tradeIds, newId)
    array.push(tradeEntries, entryPriceEstimate)
    array.push(tradeBaseStops, candidateStop)
    array.push(tradeRisks, riskPerUnit)
    array.push(tradeEntryBars, bar_index)
    array.push(tradeLastStops, candidateStop)

int activeBECount = 0
tradeArraySize = array.size(tradeIds)
if tradeArraySize > 0
    for loopIndex = 0 to tradeArraySize - 1
        idx = tradeArraySize - 1 - loopIndex
        id = array.get(tradeIds, idx)
        entryP = array.get(tradeEntries, idx)
        baseSL = array.get(tradeBaseStops, idx)
        riskP = array.get(tradeRisks, idx)
        entryBar = array.get(tradeEntryBars, idx)
        isOpen = f_isOpenTrade(id)
        if not isOpen and bar_index > entryBar
            array.remove(tradeIds, idx)
            array.remove(tradeEntries, idx)
            array.remove(tradeBaseStops, idx)
            array.remove(tradeRisks, idx)
            array.remove(tradeEntryBars, idx)
            array.remove(tradeLastStops, idx)
        else
            takeProfitPrice = entryP + riskP * takeProfitRR
            limitTakeProfit = useFixedTakeProfit ? takeProfitPrice : na
            dynamicStop = baseSL
            if useBreakEvenStep and riskP > 0
                beSourcePrice = beCalculationSource == "High" ? high : close
                moveInR = (beSourcePrice - entryP) / riskP
                stepsCompleted = moveStepInFavorR > 0 ? math.floor(moveInR / moveStepInFavorR) : 0
                if stepsCompleted >= 1
                    lockedR = stepsCompleted * lockProfitEachStepR
                    beCandidate = entryP + lockedR * riskP
                    realisticOK = preventUnrealisticBELock ? (beCalculationSource == "High" ? beCandidate < low : beCandidate < close) : true
                    if realisticOK
                        dynamicStop := math.max(dynamicStop, beCandidate)
            if useFixedTakeProfit
                dynamicStop := math.min(dynamicStop, takeProfitPrice - syminfo.mintick)
            if dynamicStop >= entryP
                activeBECount += 1
            exitAllowed = allowExitOnEntryCandle ? true : bar_index > entryBar
            if exitAllowed
                exitReason = useFixedTakeProfit ? "TP_SL_BE" : "SL_BE"
                exitMsg = f_exit_json(exitReason, close)
                strategy.exit(id="X_" + id, from_entry=id, stop=dynamicStop, limit=limitTakeProfit, comment=useFixedTakeProfit ? "TP/SL/BE" : "SL/BE", alert_message=exitMsg)
                lastStopSent = array.get(tradeLastStops, idx)
                if enableBotAlerts and dynamicStop > lastStopSent
                    alert(f_update_sl_json(dynamicStop), alert.freq_once_per_bar_close)
                    array.set(tradeLastStops, idx, dynamicStop)
                if useOriginalStrategyTakeProfit and originalTPExitSignal
                    closeMsg = f_exit_json("ORIG_EXIT", close)
                    strategy.close(id, comment="Original Strategy TP/Exit", alert_message=closeMsg)
                    if enableBotAlerts
                        alert(closeMsg, alert.freq_once_per_bar_close)

//====================================================
// WHITE DASHBOARD
//====================================================
var table dash = table.new(position.top_right, 2, 19, bgcolor=color.white, border_width=1, border_color=color.gray)
f_cell(_row, _col, _txt, _bg, _txtColor) =>
    table.cell(dash, _col, _row, _txt, text_color=_txtColor, bgcolor=_bg, text_size=size.small)

closedTrades = strategy.closedtrades
winningTrades = strategy.wintrades
losingTrades = strategy.losstrades
winRate = closedTrades > 0 ? winningTrades / closedTrades * 100.0 : 0.0
openExposurePercent = sizingEquity > 0 ? currentExposureValue / sizingEquity * 100.0 : 0.0
grossProfit = strategy.grossprofit
grossLoss = strategy.grossloss

if barstate.islast
    headerBg = color.rgb(235, 235, 235)
    goodBg = color.rgb(220, 255, 220)
    badBg = color.rgb(255, 225, 225)
    neutralBg = color.white
    f_cell(0, 0, "BUY DASHBOARD", headerBg, color.black)
    f_cell(0, 1, "WHITE", headerBg, color.black)
    f_cell(1, 0, "Signal Source", neutralBg, color.black)
    f_cell(1, 1, signalSource, neutralBg, color.black)
    f_cell(2, 0, "Closed Trades", neutralBg, color.black)
    f_cell(2, 1, str.tostring(closedTrades), neutralBg, color.black)
    f_cell(3, 0, "Wins / Losses", neutralBg, color.black)
    f_cell(3, 1, str.tostring(winningTrades) + " / " + str.tostring(losingTrades), neutralBg, color.black)
    f_cell(4, 0, "Win Rate", neutralBg, color.black)
    f_cell(4, 1, str.tostring(winRate, "#.##") + "%", winRate >= 50 ? goodBg : badBg, color.black)
    f_cell(5, 0, "Gross Profit", neutralBg, color.black)
    f_cell(5, 1, str.tostring(grossProfit, "#.##"), goodBg, color.black)
    f_cell(6, 0, "Gross Loss", neutralBg, color.black)
    f_cell(6, 1, str.tostring(grossLoss, "#.##"), badBg, color.black)
    f_cell(7, 0, "Raw Net", neutralBg, color.black)
    f_cell(7, 1, str.tostring(grossNetProfit, "#.##"), grossNetProfit >= 0 ? goodBg : badBg, color.black)
    f_cell(8, 0, "Est. Costs", neutralBg, color.black)
    f_cell(8, 1, str.tostring(totalEstimatedCosts, "#.##"), neutralBg, color.black)
    f_cell(9, 0, "Net After Costs", neutralBg, color.black)
    f_cell(9, 1, str.tostring(netAfterCosts, "#.##"), netAfterCosts >= 0 ? goodBg : badBg, color.black)
    f_cell(10, 0, "Dashboard Equity", neutralBg, color.black)
    f_cell(10, 1, str.tostring(dashboardEquity, "#.##"), dashboardEquity >= initialCapitalInput ? goodBg : badBg, color.black)
    f_cell(11, 0, "Daily P/L", neutralBg, color.black)
    f_cell(11, 1, str.tostring(dailyPnlPercent, "#.##") + "%", dailyPnlPercent >= 0 ? goodBg : badBg, color.black)
    f_cell(12, 0, "Open Trades", neutralBg, color.black)
    f_cell(12, 1, str.tostring(strategy.opentrades) + " / " + str.tostring(maxOpenTrades), neutralBg, color.black)
    f_cell(13, 0, "Exposure", neutralBg, color.black)
    f_cell(13, 1, str.tostring(openExposurePercent, "#.##") + "%", openExposurePercent <= maxTotalOpenExposurePercent ? goodBg : badBg, color.black)
    f_cell(14, 0, "BE Locked Trades", neutralBg, color.black)
    f_cell(14, 1, str.tostring(activeBECount), activeBECount > 0 ? goodBg : neutralBg, color.black)
    f_cell(15, 0, "Fee / Side", neutralBg, color.black)
    f_cell(15, 1, str.tostring(feePctPerSide, "#.###") + "%", neutralBg, color.black)
    f_cell(16, 0, "SL Buffer Mode", neutralBg, color.black)
    f_cell(16, 1, slBufferMode, neutralBg, color.black)
    f_cell(17, 0, "Buffer / ATR", neutralBg, color.black)
    f_cell(17, 1, str.tostring(slBufferAtrPercentNow, "#.##") + "% / " + str.tostring(slBufferTicksApprox, "#.##") + " ticks", neutralBg, color.black)
    f_cell(18, 0, "TP Mode", neutralBg, color.black)
    f_cell(18, 1, takeProfitMode, neutralBg, color.black)
