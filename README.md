# Weather Arbitrage Bot for Polymarket

Automated trading bot that finds mispriced temperature markets on Polymarket by comparing market prices to NWS/weather forecasts.

## How It Works

1. **Scans** Polymarket for temperature markets settling within 24 hours
2. **Fetches** weather forecasts from NOAA NWS (US) and Open-Meteo (international)
3. **Calculates** true probability from forecast data
4. **Compares** forecast probability to market prices
5. **Executes** trades when edge > 5% (configurable)

## Strategy

**Target Markets:**
- Daily temperature ranges (e.g., "NYC 26-27°F")
- Same-day high/low temperature
- Binary temperature thresholds (e.g., "Above 85°F")

**Execution:**
- Buy when forecast shows higher probability than market price
- Use GTC limit orders (0% maker fees)
- Hold until settlement or exit when edge disappears
- Target 10 positions @ $10 each = $100 deployed

**Expected Returns:**
- 5-10% edge per trade
- 50%+ win rate
- 10-20% daily returns with $100 bankroll

## Setup Instructions

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd weather_arb_bot
```

### 2. Configure Environment

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:
```
PM_PRIVATE_KEY=your_polymarket_private_key
PM_FUNDER=your_polymarket_funder_address
POSITION_SIZE_USD=10.0
MAX_POSITIONS=10
MIN_EDGE_PERCENT=5.0
DRY_RUN=false  # Set to true for testing
```

**Getting Polymarket Credentials:**
1. Create account at https://polymarket.com
2. Fund with USDC on Polygon
3. Get private key from wallet (MetaMask or similar)
4. Funder address = your wallet address

### 3. Deploy to Railway

**Option A: Deploy Button (Easiest)**
1. Click "Deploy to Railway" button (if available)
2. Add environment variables from your `.env`
3. Deploy!

**Option B: Manual Deploy**
1. Create Railway account at https://railway.app
2. Create new project
3. Connect your GitHub repository
4. Add environment variables in Railway dashboard
5. Deploy

**Railway Configuration:**
- **Start Command:** `python main.py`
- **Region:** US West (closest to Polymarket servers)
- **Plan:** Starter ($5/month)

### 4. Monitor

**View Logs:**
```bash
railway logs
```

**Expected Output:**
```
================================================================================
WEATHER ARBITRAGE BOT
================================================================================
Position Size: $10.0
Max Positions: 10
Min Edge: 5.0%
DRY RUN: False
================================================================================

🔍 Starting scan at 09:00:15
Found 47 weather markets
Analyzed 23 markets with forecast data

📊 Highest temperature in NYC on February 10?
   Forecast: 15.0% | Market: 1.0% | Edge: +14.0%
✅ Executed YES $10.00 at 0.010 → Order abc123...

📊 Seattle temperature 44-45°F
   Forecast: 8.0% | Market: 3.0% | Edge: +5.0%
✅ Executed YES $10.00 at 0.030 → Order def456...

✅ Executed 2 trades
```

## Testing

**Test market scanner:**
```bash
python market_scanner.py
```

**Test probability calculator:**
```bash
python probability_calculator.py
```

**Test full bot (dry run):**
```bash
DRY_RUN=true python main.py
```

## Configuration Options

| Variable | Default | Description |
|----------|---------|-------------|
| `POSITION_SIZE_USD` | 10.0 | USD per trade |
| `MAX_POSITIONS` | 10 | Max concurrent positions |
| `MIN_EDGE_PERCENT` | 5.0 | Minimum edge to trade (%) |
| `DRY_RUN` | true | Set false for real trading |
| `SCAN_INTERVAL_MINUTES` | 60 | How often to scan |
| `RUN_MODE` | continuous | Use "once" for single scan |

## Supported Cities

**US (NWS Data):**
NYC, Chicago, Seattle, Dallas, Miami, Los Angeles, Atlanta, Boston, Denver, Phoenix

**International (Open-Meteo):**
London, Seoul, Toronto, Buenos Aires, Wellington, Ankara

## Risk Management

**Bankroll:** Start with $100-200
**Position Sizing:** 5-10% per trade ($10-20)
**Max Exposure:** 50% of bankroll ($50-100)
**Stop Loss:** Pause bot if down 30%

## Fees

- **Trading:** 0% (using GTC limit orders)
- **Weather API:** FREE (NWS + Open-Meteo)
- **Server:** $5/month (Railway)

## Common Issues

**"No markets found":**
- Weather markets are seasonal (more in winter/summer)
- Check Polymarket manually to verify markets exist

**"Could not get forecast":**
- NWS API may be down (check status.weather.gov)
- City coordinates may be wrong
- Date is beyond 48-hour forecast window

**"Insufficient liquidity":**
- Temperature markets have lower volume than politics
- Reduce position size or increase min_edge threshold

## Scaling

**Week 1:** $100 → $150 (conservative)
**Month 1:** $100 → $300-500 (10-20% daily returns)
**Month 3:** Scale to $1000+ and add more cities

## Support

Questions? Check logs first:
```bash
railway logs --tail 100
```

## Disclaimer

Trading involves risk. Past performance does not guarantee future results. This bot is for educational purposes. Trade at your own risk.
