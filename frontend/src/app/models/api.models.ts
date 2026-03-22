export interface EventSummary {
  event_slug: string;
  event_title: string;
  market_count: number;
  game_start_time: number;
}

export interface MarketSummary {
  market_id: string;
  object_key: string;
  display_name: string;
  event_slug: string;
  event_title: string;
  market_question: string;
  row_count: number;
  volume: number;
  image_url: string;
}

export interface MarketMetadataDto {
  asset_id: string;
  event_slug: string;
  event_title: string;
  market_question: string;
  outcomes: string;
  min_tick_size: number;
  min_order_size: number;
  is_neg_risk: boolean;
  game_start_time: number;
  // Make sure these are added and the file is SAVED:
  volume: number;
  volume_24hr: number;
  liquidity: number;
  image_url: string;
  resolution_source: string;
  end_date: string;
}

export interface MarketRow {
  timestamp: number;
  best_bid: number;
  best_ask: number;
}

export interface MarketSeriesPoint {
  timestamp: number;
  best_bid: number;
  best_ask: number;
}

export interface MarketStats {
  row_count: number;
  first_ts: number | null;
  last_ts: number | null;
  min_best_bid: number | null;
  max_best_bid: number | null;
  min_best_ask: number | null;
  max_best_ask: number | null;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  limit: number;
  offset: number;
}
