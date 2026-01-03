import yfinance as yf
import statistics
from dataclasses import dataclass

@dataclass
class PriceContext:
    price_change_1d: float
    price_change_7d: float
    volume_spike: bool
    volatility_level: str


def fetch_price_context_indian(ticker: str) -> PriceContext:
    """
    Fetch price context for Indian stocks using free Yahoo Finance data.
    Example ticker: 'RELIANCE.NS'
    """

    df = yf.download(
        f'{ticker}.ns',
        period="10d",
        interval="1d",
        progress=False,
        auto_adjust=False
    )

    if df.empty or len(df) < 8:
        raise ValueError("Not enough price data")

    closes = df["Close"].iloc[:, 0].tolist()
    volumes = df["Volume"].iloc[:, 0].tolist()

    price_now = closes[-1]
    price_1d_ago = closes[-2]
    price_7d_ago = closes[-8]

    price_change_1d = ((price_now - price_1d_ago) / price_1d_ago) * 100
    price_change_7d = ((price_now - price_7d_ago) / price_7d_ago) * 100

    avg_volume = statistics.mean(volumes[-8:-1])
    volume_spike = volumes[-1] > avg_volume * 1.5

    returns = [
        (closes[i] - closes[i - 1]) / closes[i - 1]
        for i in range(1, len(closes))
    ]
    volatility = statistics.stdev(returns[-7:])

    if volatility < 0.01:
        volatility_level = "low"
    elif volatility < 0.03:
        volatility_level = "medium"
    else:
        volatility_level = "high"

    return PriceContext(
        price_change_1d=round(price_change_1d, 2),
        price_change_7d=round(price_change_7d, 2),
        volume_spike=volume_spike,
        volatility_level=volatility_level,
    )


schema_fetch_price_context_indian = types.FunctionDeclaration(
    name="fetch_price_context_indian",
    description="fetches the volume spikes, price change for 1 day and 7day, and volatility_level",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "ticker": types.Schema(
                type=types.Type.STRING,
                description="stock ticker",
            )
        },
        required=["ticker"]
    ),
)