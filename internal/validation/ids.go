package validation

// Schema $id constants. The path after the host is the file path under
// schemas/ (Load enforces this).
const (
	IDRawEvent             = "https://schemas.cg-etl.internal/pipeline/raw_event.v1.json"
	IDRejectedRecord       = "https://schemas.cg-etl.internal/pipeline/rejected_record.v1.json"
	IDDLQRecord            = "https://schemas.cg-etl.internal/pipeline/dlq_record.v1.json"
	IDSourceMarkets        = "https://schemas.cg-etl.internal/source/coingecko/coins_markets.v1.json"
	IDSourceCoin           = "https://schemas.cg-etl.internal/source/coingecko/coin.v1.json"
	IDMarketSnapshot       = "https://schemas.cg-etl.internal/processed/market_snapshot.v1.json"
	IDCoinSnapshot         = "https://schemas.cg-etl.internal/processed/coin_snapshot.v1.json"
	IDCoinMarketByCurrency = "https://schemas.cg-etl.internal/processed/coin_market_by_currency.v1.json"
	IDTicker               = "https://schemas.cg-etl.internal/processed/ticker.v1.json"
	IDCoinContract         = "https://schemas.cg-etl.internal/processed/coin_contract.v1.json"
)

// baseURL is the common prefix of every $id.
const baseURL = "https://schemas.cg-etl.internal/"
