CREATE TABLE exchange_rate_limits (
  interval TEXT NOT NULL,
  intervalNum INTEGER NOT NULL,
  limit INTEGER NOT NULL,
  rateLimitType TEXT PRIMARY KEY
);

CREATE TABLE exchange_symbols (
  allowTrailingStop BOOLEAN NOT NULL,
  baseAsset TEXT NOT NULL,
  baseAssetPrecision INTEGER NOT NULL,
  baseCommissionPrecision INTEGER NOT NULL,
  cancelReplaceAllowed BOOLEAN NOT NULL,
  defaultSelfTradePreventionMode TEXT NOT NULL,
  icebergAllowed BOOLEAN NOT NULL,
  isMarginTradingAllowed BOOLEAN NOT NULL,
  isSpotTradingAllowed BOOLEAN NOT NULL,
  ocoAllowed BOOLEAN NOT NULL,
  otoAllowed BOOLEAN NOT NULL,
  quoteAsset TEXT NOT NULL,
  quoteAssetPrecision INTEGER NOT NULL,
  quoteCommissionPrecision INTEGER NOT NULL,
  quoteOrderQtyMarketAllowed BOOLEAN NOT NULL,
  status TEXT NOT NULL,
  symbol TEXT PRIMARY KEY
);

CREATE TABLE exchange_symbol_filters (
  filterType TEXT PRIMARY KEY,
  maxPrice TEXT NOT NULL,
  minPrice TEXT NOT NULL,
  tickSize TEXT NOT NULL
);

CREATE TABLE exchange_symbols_exchange_symbol_filters_link (
  exchange_symbols_symbol TEXT NOT NULL,
  exchange_symbol_filters_filterType TEXT NOT NULL,
  PRIMARY KEY (exchange_symbols_symbol, exchange_symbol_filters_filterType)
);

INSERT INTO exchange_rate_limits (interval, intervalNum, limit, rateLimitType) VALUES ('MINUTE', 1, 6000, 'REQUEST_WEIGHT');

INSERT INTO exchange_rate_limits (interval, intervalNum, limit, rateLimitType) VALUES ('SECOND', 10, 100, 'ORDERS');

INSERT INTO exchange_rate_limits (interval, intervalNum, limit, rateLimitType) VALUES ('MINUTE', 5, 61000, 'RAW_REQUESTS');

INSERT INTO exchange_symbol_filters (filterType, maxPrice, minPrice, tickSize) VALUES ('PRICE_FILTER', '100000.00000000', '0.00000100', '0.00000100');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('LOT_SIZE');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('ICEBERG_PARTS');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('MARKET_LOT_SIZE');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('TRAILING_DELTA');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('PERCENT_PRICE_BY_SIDE');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('NOTIONAL');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('MAX_NUM_ORDERS');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('MAX_NUM_ORDER_LISTS');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('MAX_NUM_ALGO_ORDERS');

INSERT INTO exchange_symbol_filters (filterType) VALUES ('MAX_NUM_ORDER_AMENDS');

INSERT INTO exchange_symbols (allowTrailingStop, baseAsset, baseAssetPrecision, baseCommissionPrecision, cancelReplaceAllowed, defaultSelfTradePreventionMode, icebergAllowed, isMarginTradingAllowed, isSpotTradingAllowed, ocoAllowed, otoAllowed, quoteAsset, quoteAssetPrecision, quoteCommissionPrecision, quoteOrderQtyMarketAllowed, status, symbol) VALUES (TRUE, 'BNB', 8, 8, TRUE, 'EXPIRE_MAKER', TRUE, TRUE, TRUE, TRUE, TRUE, 'BTC', 8, 8, TRUE, 'TRADING', 'BNBBTC');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'PRICE_FILTER');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'LOT_SIZE');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'ICEBERG_PARTS');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'MARKET_LOT_SIZE');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'TRAILING_DELTA');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'PERCENT_PRICE_BY_SIDE');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'NOTIONAL');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'MAX_NUM_ORDERS');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'MAX_NUM_ORDER_LISTS');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'MAX_NUM_ALGO_ORDERS');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BNBBTC', 'MAX_NUM_ORDER_AMENDS');

INSERT INTO exchange_symbols (allowTrailingStop, baseAsset, baseAssetPrecision, baseCommissionPrecision, cancelReplaceAllowed, defaultSelfTradePreventionMode, icebergAllowed, isMarginTradingAllowed, isSpotTradingAllowed, ocoAllowed, otoAllowed, quoteAsset, quoteAssetPrecision, quoteCommissionPrecision, quoteOrderQtyMarketAllowed, status, symbol) VALUES (TRUE, 'BTC', 8, 8, TRUE, 'EXPIRE_MAKER', TRUE, TRUE, TRUE, TRUE, TRUE, 'USDT', 8, 8, TRUE, 'TRADING', 'BTCUSDT');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'PRICE_FILTER');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'LOT_SIZE');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'ICEBERG_PARTS');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'MARKET_LOT_SIZE');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'TRAILING_DELTA');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'PERCENT_PRICE_BY_SIDE');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'NOTIONAL');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'MAX_NUM_ORDERS');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'MAX_NUM_ORDER_LISTS');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'MAX_NUM_ALGO_ORDERS');

INSERT INTO exchange_symbols_exchange_symbol_filters_link (exchange_symbols_symbol, exchange_symbol_filters_filterType) VALUES ('BTCUSDT', 'MAX_NUM_ORDER_AMENDS');