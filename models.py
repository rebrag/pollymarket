from typing import Literal, TypedDict
import datetime

class Market(TypedDict):
    id: int #"id": "1320233",
    question: str #"question": "Golden Knights vs. Penguins",
    conditionId: str #"conditionId": "0x228882ef15410cda88ae94b6fee134e87de5360d8439cf7901b2a0f8ddd2081b",
    slug: str #"slug": "nhl-las-pit-2026-03-01",
    resolutionSource: str #"resolutionSource": "https://www.nhl.com/scores",
    endDate: str #"endDate": "2026-03-01T18:00:00Z",
    liquidity: float #"liquidity": "380512.15172",
    startDate: str #"startDate": "2026-02-02T16:32:42.131344Z",
    image: str #"image": "https://polymarket-upload.s3.us-east-2.amazonaws.com/nhl.png",
    icon: str #"icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/nhl.png",
    description: str #"description": "In the upcoming NHL game, scheduled for March 1 at 1:00PM ET:\nIf the Golden Knights win, the market will resolve to \"Golden Knights\".\nIf the Penguins win, the market will resolve to \"Penguins\".\nIf the game is postponed, this market will remain open until the game has been completed.\nIf the game is canceled entirely, with no make-up game, this market will resolve 50-50.\nThe result will be determined based on the final score including any overtime periods and shootouts. In the event of a shootout, one goal will be added to the winning team's score for the purpose of resolution.",
    outcomes: str #"outcomes": "[\"Golden Knights\", \"Penguins\"]",
    outcomePrice: str #"outcomePrices": "[\"0.0005\", \"0.9995\"]",
    volume: str #"volume": "933335.575111",
    active: bool #"active": true,
    closed: bool #"closed": false,
    marketMakerAddress: str #"marketMakerAddress": "",
    createdAt: str #"createdAt": "2026-02-02T16:30:09.157817Z",
    updatedAt: str #"updatedAt": "2026-03-01T20:30:30.240974Z",
    new: bool  # false
    featured: bool  # false
    submitted_by: str  # "0x91430CaD2d3975766499717fA0D66A78D814E5c5"
    archived: bool  # false
    resolvedBy: str  # "0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7"
    restricted: bool  # true
    groupItemThreshold: str  # "0"
    questionID: str  # "0x3272cb98a891a5bc186aef076dddc09c510a098f56f57609346edb2f6bf2529c"
    enableOrderBook: bool  # true
    orderPriceMinTickSize: float  # 0.001
    orderMinSize: int  # 5
    umaResolutionStatus: str  # "proposed"
    volumeNum: float  # 933335.575111
    liquidityNum: float  # 380512.15172
    endDateIso: str  # "2026-03-01"
    startDateIso: str  # "2026-02-02"
    hasReviewedDates: bool  # true
    volume24hr: float  # 499825.5018999999
    volume1wk: float  # 502353.59157299995
    volume1mo: float  # 502390.847165
    volume1yr: float  # 502390.847165
    gameStartTime: str  # "2026-03-01 18:00:00+00"
    secondsDelay: int  # 3
    clobTokenIds: str  # "[\"541825015334...\", \"547861363324...\"]"
    umaBond: str  # "500"
    umaReward: str  # "2"
    volume24hrClob: float  # 499825.5018999999
    volume1wkClob: float  # 502353.59157299995
    volume1moClob: float  # 502390.847165
    volume1yrClob: float  # 502390.847165
    volumeClob: float  # 933335.575111
    liquidityClob: float  # 380512.15172
    customLiveness: int  # 0
    acceptingOrders: bool  # true
    negRisk: bool  # false
    negRiskRequestID: str  # ""
    ready: bool  # false
    funded: bool  # false
    acceptingOrdersTimestamp: str  # "2026-02-02T16:31:36Z"
    cyom: bool  # false
    competitive: float  # 0.8003199679231757
    pagerDutyNotificationEnabled: bool  # false
    approved: bool  # true
    rewardsMinSize: int  # 50
    rewardsMaxSpread: float  # 4.5
    spread: float  # 0.001
    oneDayPriceChange: float  # 0.04
    oneWeekPriceChange: float  # 0.035
    lastTradePrice: float  # 0.001
    bestBid: float  # 0.001
    bestAsk: float  # 0.001
    automaticallyActive: bool  # true
    clearBookOnStart: bool  # true
    manualActivation: bool  # false
    negRiskOther: bool  # false
    sportsMarketType: str  # "moneyline"
    umaResolutionStatuses: str  # "[\"proposed\"]"
    pendingDeployment: bool  # false
    deploying: bool  # false
    deployingTimestamp: str  # "2026-02-02T16:30:13.656772Z"
    rfqEnabled: bool  # false
    holdingRewardsEnabled: bool  # false
    feesEnabled: bool  # false
    requiresTranslation: bool  # false
    feeType: str  # null
    score: str #"score": "73-65",
    elapsed: str #"elapsed": "07:46",
    period: str #"period": "Q3",
    live: bool #"live": true,
    ended: bool #"ended": false,
    negRiskAugmented: bool #"negRiskAugmented": false,
    gameId: int #"gameId": 20023335,

class Event(TypedDict):
    id: str #string with int inside
    ticker: str #often same as slug
    slug: str
    title: str
    markets: list[Market] 
    startTime: str #"startTime": "2026-03-01T18:00:00Z",
    description: str
    resolutionSource: str
    startDate: str
    creationDate: str
    endDate: str # example: "2026-03-01T20:30:00Z"
    image: str #"https://polymarket-upload.s3.us-east-2.amazonaws.com/super+cool+basketball+in+red+and+blue+wow.png",
    icon: str # "https://polymarket-upload.s3.us-east-2.amazonaws.com/super+cool+basketball+in+red+and+blue+wow.png",
    active: bool # true,note - lowercase true/false
    closed: bool #"closed": false,
    archivte: bool #"archived": false,
    new: bool #"new": false,
    featured: bool#"featured": false,
    restricted: bool #"restricted": true,
    liquidity: float #"liquidity": 837864.0943,
    volume: float #"volume": 1410512.425042,
    openInterest: float | int #"openInterest": 0,
    createdAt: str #"createdAt": "2026-02-24T15:00:45.203748Z",
    updatedAt: str #"updatedAt": "2026-03-01T19:26:19.555878Z",
    competitive: float #"competitive": 1,
    volume24hr: float #"volume24hr": 1074648.5762499992,
    volume1wk: float #"volume1wk": 1104233.4864509995,
    volume1mo: float #"volume1mo": 1104233.4864509995,
    volume1yr: float #"volume1yr": 1104233.4864509995,
    enableOrderBook: bool #"enableOrderBook": true,
    liquidityClob: float #"liquidityClob": 837864.0943,
    negRisk: bool #"negRisk": false,

class Level(TypedDict):
    """Represents a single price point and the total volume available at that price."""
    price: str  # Kept as str to maintain decimal precision from API
    size: str   # Kept as str to avoid float rounding errors during ingestion

class BookEvent(TypedDict):
    """"This is the structure of "event_type": "book" that is received from polymarkets ws feed"""
    market: str #"market": "0x5617f58308c8497fa49e88bd35be661034ea31e56345f9785e8f33f3b9077c98",
    asset_id: str #"asset_id": "90894579050104806647769298739092855504735024765895323784316487334490672781081",
    timestamp: str #"timestamp": "1772422787547",
    hash: str #"hash": "dc7cc1cef57c1664d171305157c48d300a85bee0",
    bids: list[Level] #"bids": [], 
    asks: list[Level] #starts with the highest price first
    tick_size: str #"tick_size": "0.001",
    event_type: str #"event_type": "book",
    last_trade_price: str #"last_trade_price": "0.999"

class AssetUpdate(TypedDict):
    asset_id: str #"asset_id": "15813528336416885208709320741143401537584216609310132819355197271415877230211",
    price: str #"price": "0.19",
    size: str #"size": "0",
    side: str #"side": "BUY",
    hash: str #"hash": "57eeb6a9fb5ec62644cc86c64dd2d76666543dae",
    best_bid: str #"best_bid": "0.14", NOTE When there aren't any ASKS, best_bid is set to "0"
    best_ask: str #"best_ask": "0.43" NOTE When there aren't any BIDS, best_ask is set to "1"

class PriceChangeEvent(TypedDict):
    """"This is the structure of "event_type": "price_chnage" that is received from polymarkets ws feed"""
    market: str #"market": "0x099dffb86b303f61b32b6339b5c3463435bde8351615f750303097d899d4fcd5",
    price_changes: list[AssetUpdate] #"price_changes": [
    timestamp: str #"timestamp": "1772485719977", THE LAST 3 DIGITS ARE THE MS
    event_type: str #"event_type": "price_change"

class LastTradePriceEvent(TypedDict):
    market: str #"market": "0xb2a81a3c63aae22a992eb592ad9970d54f59b782b0d0ad465bf28059f03b1d5e",
    asset_id: str #"asset_id": "2858664973040681902736946699455082869901699606306076636729734795709812001003",
    price: str #"price": "0.13",
    size: str #"size": "2129.64",
    fee_rate_bps: str #"fee_rate_bps": "0",
    side: str #"side": "SELL",
    timestamp: str #"timestamp": "1772487573040",
    event_type: str #"event_type": "last_trade_price",
    transaction_hash: str #"transaction_hash": "0xa0af3300ddabb236b11c495c59fed3d68a407f05e820787598fe2e7825448fc0"


TradeSide = Literal["BUY", "SELL"]


class TradeRow(TypedDict):
    timestamp: float
    asset_id: str
    price: float
    size: float
    side: TradeSide
    fee_rate_bps: float
    transaction_hash: str
    notional_usd: float


class TradeBucket(TypedDict):
    bucket_start_ts: float
    bucket_end_ts: float
    trade_count: int
    total_size: float
    total_notional_usd: float
    max_trade_size: float
    max_notional_usd: float
    buy_count: int
    sell_count: int
    avg_price: float
    is_large_trade_bucket: bool
    is_high_frequency_bucket: bool

class WSPayload(TypedDict, total=False):
    type: str
    action: str
    assets_ids: list[str]
    initial_dump: bool

class Orderbook(TypedDict):
    asset_id: str
    outcome: str          # e.g. "Yes" — the outcome this token represents
    paired_asset_id: str  # the complementary token in the same binary market
    paired_outcome: str   # e.g. "No"
    event_slug: str
    event_title: str
    market_question: str
    outcomes: str
    min_tick_size: float
    min_order_size: int
    is_neg_risk: bool
    bids: dict[str, float]
    asks: dict[str, float]
    lastTradePrice: float
    midpoint: float
    spread: float
    is_active: bool
    last_update: datetime
    game_start_time: float
    volume: float
    volume_24hr: float
    liquidity: float
    image_url: str
    resolution_source: str
    end_date: str

class TickSizeChangeEvent(TypedDict):
    market: str #"market": "0x319911213cb7a5dcf0e2c5e333c29f4e8b2847d981c4c6ad6c556721754cc73b",
    asset_id: str #"asset_id": "31445096247929111175236309910514163630492461229315761864119748084692636371715",
    old_tick_size: str #"old_tick_size": "0.01",
    new_tick_size: str #"new_tick_size": "0.001",
    timestamp: str #"timestamp": "1772598350672",
    event_type: str #"event_type": "tick_size_change"

class BestBidAskEvent(TypedDict):
    market: str
    asset_id: str
    best_bid: str
    best_ask: str
    spread: str
    timestamp: str
    event_type: str #best_bid_ask

class NewMarketWsEvent(TypedDict):
        id: str #"id": "1542085",
        question: str#"question": "Will Viborg FF win on 2026-04-06?",
        market: str #"market": "0x2aa0228436ce503fe33066bfa54f5b547f3e8a4bc19acfb980a6a46852260055",
        slug: str #"slug": "den-vib-agf-2026-04-06-vib",
        description: str #"description": "In the upcoming game, scheduled for April 6, 2026\nIf Viborg FF wins, this market will resolve to \"Yes\".\nOtherwise, this market will resolve to \"No\".\nIf the game is postponed, this market will remain open until the game has been completed.\nIf the game is canceled entirely, with no make-up game, this market will resolve \"No\".\nThis market refers only to the outcome within the first 90 minutes of regular play plus stoppage time.\n\nThe primary resolution source for this market is the official statistics of the event as recognized by the governing body or event organizers. However, if the governing body or event organizers have not published final match statistics within 2 hours after the event's conclusion, a consensus of credible reporting may be used instead.",
        asset_ids: list[str] #"assets_ids": [ "50351309255136140635720305168576833845521330511314321882673853812798453957650", "107121155649098101006139168418369342288278576743170843552540613247626899303724"],
        outcomes: list[str] #"outcomes": ["Yes", "No"],
        # "event_message": {
        #     "id": "257095",
        #     "ticker": "den-vib-agf-2026-04-06",
        #     "slug": "den-vib-agf-2026-04-06",
        #     "title": "Viborg FF vs. Aarhus GF",
        #     "description": "This event is for the upcoming Denmark Superliga game, scheduled for Monday, April 6, 2026 between Viborg FF and Aarhus GF."
        # },
        timestamp: str #"timestamp": "1773086679617",
        event_type: str #"event_type": "new_market",
        tags: list[str | int] #"tags": []
