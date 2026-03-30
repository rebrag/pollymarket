import { Component, OnInit, ViewChild, signal } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { BaseChartDirective } from 'ng2-charts';
import { combineLatest } from 'rxjs';
import { BrnLabelDirective } from '@spartan-ng/ui-label-brain';
import {
  BubbleController,
  BubbleDataPoint,
  CategoryScale,
  Chart,
  ChartConfigurationCustomTypesPerDataset,
  ChartOptions,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  Plugin,
  PointElement,
  ScatterDataPoint,
  Tooltip,
  TooltipItem,
} from 'chart.js';

import {
  MarketMetadataDto,
  MarketRow,
  MarketSeriesPoint,
  MarketSummary,
  MarketStats,
  PaginatedResponse,
  TradeRow,
} from '../../models/api.models';
import { MarketApiService } from '../../services/market-api.service';

type MixedChartData = ChartConfigurationCustomTypesPerDataset<
  'line' | 'bubble',
  (ScatterDataPoint | BubbleDataPoint)[],
  unknown
>['data'];

type MixedChartDataset = MixedChartData['datasets'][number];

Chart.register(BubbleController, LineController, LineElement, PointElement, LinearScale, Tooltip, Legend, CategoryScale);

const hoverGuideLinePlugin: Plugin<'line'> = {
  id: 'hoverGuideLine',
  afterEvent(chart, args) {
    const event = args.event;
    const chartArea = chart.chartArea;
    if (!chartArea) {
      return;
    }

    const chartWithHover = chart as Chart<'line'> & { hoverGuideLineX?: number };
    const eventX = typeof event.x === 'number' ? event.x : null;
    const eventY = typeof event.y === 'number' ? event.y : null;
    const isInsideChart =
      eventX !== null &&
      eventY !== null &&
      eventX >= chartArea.left &&
      eventX <= chartArea.right &&
      eventY >= chartArea.top &&
      eventY <= chartArea.bottom;

    chartWithHover.hoverGuideLineX = isInsideChart ? eventX : undefined;
    if (!isInsideChart) {
      if (chart.tooltip?.getActiveElements().length) {
        chart.tooltip.setActiveElements([], { x: 0, y: 0 });
        args.changed = true;
      }
      return;
    }

    const nativeEvent = event.native;
    if (!(nativeEvent instanceof Event)) {
      return;
    }

    const bubbleElements = chart
      .getElementsAtEventForMode(nativeEvent, 'nearest', { intersect: true, axis: 'xy' }, false)
      .filter((item) => chart.data.datasets[item.datasetIndex]?.label === 'Trade activity');

    let nextActive = bubbleElements;
    if (nextActive.length === 0) {
      const quoteElements = chart
        .getElementsAtEventForMode(nativeEvent, 'index', { intersect: false, axis: 'x' }, false)
        .filter((item) => item.datasetIndex < 3);
      nextActive = quoteElements;
    }

    const currentActive = chart.tooltip?.getActiveElements() ?? [];
    const isSameActiveSet =
      currentActive.length === nextActive.length &&
      currentActive.every(
        (item, index) =>
          item.datasetIndex === nextActive[index]?.datasetIndex && item.index === nextActive[index]?.index,
      );

    if (!isSameActiveSet) {
      chart.tooltip?.setActiveElements(nextActive, { x: eventX, y: eventY });
      args.changed = true;
    }
  },
  afterDatasetsDraw(chart) {
    const chartWithHover = chart as Chart<'line'> & { hoverGuideLineX?: number };
    const x = chartWithHover.hoverGuideLineX;
    if (typeof x !== 'number') {
      return;
    }

    const { top, bottom } = chart.chartArea;
    const ctx = chart.ctx;
    ctx.save();
    ctx.beginPath();
    ctx.moveTo(x, top);
    ctx.lineTo(x, bottom);
    ctx.lineWidth = 1;
    ctx.strokeStyle = '#6b7280';
    ctx.stroke();
    ctx.restore();
  },
};

Chart.register(hoverGuideLinePlugin);

@Component({
  selector: 'app-market-detail-page',
  standalone: true,
  imports: [CommonModule, FormsModule, BaseChartDirective, DatePipe, RouterLink, BrnLabelDirective],
  templateUrl: './market-detail-page.component.html',
  styleUrl: './market-detail-page.component.css',
})
export class MarketDetailPageComponent implements OnInit {
  @ViewChild(BaseChartDirective) private readonly chartDirective?: BaseChartDirective<'line' | 'bubble'>;

  private readonly volumeFormatter = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  });

  readonly Math = Math;
  marketId = '';
  metadata: MarketMetadataDto | null = null;
  stats: MarketStats | null = null;
  rows: MarketRow[] = [];
  showRawData = false;
  outcome1Label = 'Outcome 1';
  outcome2Label = 'Outcome 2';
  currentSeriesPoints: MarketSeriesPoint[] = [];
  currentTradePoints: TradeRow[] = [];
  firstSeriesTimestampMs = 0;
  lastSeriesTimestampMs = 0;
  readonly chartRangeStart = signal('');
  readonly chartRangeEnd = signal('');
  readonly minTradeSizeInput = signal('0');
  readonly appliedMinTradeSize = signal('0');
  timeFilterError = '';
  seriesLoading = false;
  tradePointsLoading = false;
  tradePointState = '';
  dragSelectionVisible = false;
  dragSelectionLeftPx = 0;
  dragSelectionWidthPx = 0;
  dragSelectionTopPx = 0;
  dragSelectionHeightPx = 0;

  private dragStartPixelX = 0;
  private dragCurrentPixelX = 0;

  limit = 100;
  offset = 0;
  total = 0;

  loading = false;
  error = '';
  copyStatus = '';
  relatedMarkets: MarketSummary[] = [];
  selectedRelatedMarketId = '';
  loadingRelatedMarkets = false;
  relatedMarketsError = '';

  chartData: MixedChartData = {
    datasets: [
      { data: [], label: 'Outcome 1', type: 'line', borderColor: '#0b6b36', tension: 0.15, pointRadius: 0 },
      { data: [], label: 'Outcome 2', type: 'line', borderColor: '#8f1f1f', tension: 0.15, pointRadius: 0 },
      { data: [], label: 'Spread (Ask - Bid)', type: 'line', borderColor: '#0f172a', tension: 0.15, pointRadius: 0 },
      { data: [], label: 'Trade activity', type: 'bubble' },
    ],
  };

  chartOptions: ChartOptions<'line' | 'bubble'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    scales: {
      x: {
        type: 'linear',
        min: 0,
        ticks: {
          callback: (value): string => {
            const actualTimestampMs = this.firstSeriesTimestampMs + Number(value);
            return new Date(actualTimestampMs).toLocaleTimeString([], {
              hour: 'numeric',
              minute: '2-digit',
            });
          },
          maxRotation: 45,
          minRotation: 45,
        },
      },
      y: {
        min: 0,
        max: 1,
      },
    },
    plugins: {
      legend: { display: true },
      tooltip: {
        mode: 'nearest',
        intersect: true,
        callbacks: {
          title: (items): string => {
            const tooltipTimestampMs = this.tooltipTimestampMs(items);
            if (tooltipTimestampMs == null) {
              return '';
            }

            return new Date(tooltipTimestampMs).toLocaleString([], {
              month: 'short',
              day: 'numeric',
              hour: 'numeric',
              minute: '2-digit',
              second: '2-digit',
            });
          },
          label: (context): string | string[] => this.tooltipLabel(context),
        },
      },
    },
    interaction: {
      mode: 'nearest',
      intersect: false,
    },
  };

  constructor(
    private readonly route: ActivatedRoute,
    private readonly router: Router,
    private readonly api: MarketApiService,
  ) {}

  ngOnInit(): void {
    combineLatest([this.route.paramMap, this.route.queryParamMap]).subscribe(([params, query]) => {
      this.marketId = params.get('marketId') ?? '';
      this.limit = Number(query.get('limit') ?? 100);
      this.offset = Number(query.get('offset') ?? 0);

      if (!this.marketId) {
        this.error = 'Missing market id';
        return;
      }
      this.loadAll();
    });
  }

  previousPage(): void {
    this.offset = Math.max(0, this.offset - this.limit);
    this.updateQueryParams();
  }

  nextPage(): void {
    if (this.offset + this.limit >= this.total) {
      return;
    }
    this.offset += this.limit;
    this.updateQueryParams();
  }

  toggleRawData(): void {
    this.showRawData = !this.showRawData;
  }

  shortAssetId(assetId: string): string {
    if (!assetId) {
      return '';
    }
    return assetId.length <= 5 ? assetId : `${assetId.slice(0, 5)}...`;
  }

  shouldAnimateTitle(title: string): boolean {
    return title.trim().length > 56;
  }

  async copyAssetId(): Promise<void> {
    const assetId = this.metadata?.asset_id ?? '';
    if (!assetId) {
      return;
    }

    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(assetId);
      } else {
        const input = document.createElement('input');
        input.value = assetId;
        document.body.appendChild(input);
        input.select();
        document.execCommand('copy');
        document.body.removeChild(input);
      }
      this.copyStatus = 'Copied';
    } catch {
      this.copyStatus = 'Copy failed';
    }

    window.setTimeout(() => {
      if (this.copyStatus) {
        this.copyStatus = '';
      }
    }, 1500);
  }

  goToSelectedRelatedMarket(): void {
    if (!this.selectedRelatedMarketId || this.selectedRelatedMarketId === this.marketId) {
      return;
    }

    this.router.navigate(['/markets', this.selectedRelatedMarketId], {
      queryParams: {
        limit: this.limit,
        offset: 0,
      },
    });
  }

  selectRelatedMarket(marketId: string): void {
    this.selectedRelatedMarketId = marketId;
  }

  formatVolume(volume: number): string {
    return this.volumeFormatter.format(volume);
  }

  updateChartRangeStart(value: string): void {
    this.chartRangeStart.set(value);
    this.loadSeriesForCurrentRange();
  }

  updateChartRangeEnd(value: string): void {
    this.chartRangeEnd.set(value);
    this.loadSeriesForCurrentRange();
  }

  clearChartTimeFilter(): void {
    this.chartRangeStart.set(this.defaultChartRangeStart());
    this.chartRangeEnd.set(this.defaultChartRangeEnd());
    this.timeFilterError = '';
    this.loadSeriesForCurrentRange();
  }

  updateMinTradeSizeInput(value: string): void {
    this.minTradeSizeInput.set(value);
  }

  applyMinTradeSize(): void {
    this.appliedMinTradeSize.set(this.minTradeSizeInput());
    this.loadSeriesForCurrentRange();
  }

  onChartPointerDown(event: PointerEvent): void {
    if (this.seriesLoading || this.currentSeriesPoints.length === 0) {
      return;
    }

    const chart = this.chartDirective?.chart;
    const chartArea = chart?.chartArea;
    if (!chart || !chartArea) {
      return;
    }

    if (event.offsetX < chartArea.left || event.offsetX > chartArea.right || event.offsetY < chartArea.top || event.offsetY > chartArea.bottom) {
      return;
    }

    const pointerTarget = event.target;
    if (pointerTarget instanceof Element && 'setPointerCapture' in pointerTarget) {
      pointerTarget.setPointerCapture(event.pointerId);
    }

    this.dragStartPixelX = this.clampToChartX(event.offsetX);
    this.dragCurrentPixelX = this.dragStartPixelX;
    this.dragSelectionTopPx = chartArea.top;
    this.dragSelectionHeightPx = chartArea.bottom - chartArea.top;
    this.updateDragSelectionBox();
  }

  onChartPointerMove(event: PointerEvent): void {
    if (!this.dragSelectionVisible) {
      return;
    }

    this.dragCurrentPixelX = this.clampToChartX(event.offsetX);
    this.updateDragSelectionBox();
  }

  onChartPointerUp(event: PointerEvent): void {
    if (!this.dragSelectionVisible) {
      return;
    }

    const pointerTarget = event.target;
    if (pointerTarget instanceof Element && 'releasePointerCapture' in pointerTarget && pointerTarget.hasPointerCapture(event.pointerId)) {
      pointerTarget.releasePointerCapture(event.pointerId);
    }

    this.dragCurrentPixelX = this.clampToChartX(event.offsetX);
    const selectionWidth = Math.abs(this.dragCurrentPixelX - this.dragStartPixelX);
    if (selectionWidth < 6) {
      this.resetDragSelection();
      return;
    }

    const chart = this.chartDirective?.chart;
    const xScale = chart?.scales['x'];
    if (!chart || !xScale) {
      this.resetDragSelection();
      return;
    }

    const startPixel = Math.min(this.dragStartPixelX, this.dragCurrentPixelX);
    const endPixel = Math.max(this.dragStartPixelX, this.dragCurrentPixelX);
    const startValue = xScale.getValueForPixel(startPixel);
    const endValue = xScale.getValueForPixel(endPixel);
    if (startValue == null || endValue == null) {
      this.resetDragSelection();
      return;
    }

    const startTimestampMs = this.firstSeriesTimestampMs + startValue;
    const endTimestampMs = this.firstSeriesTimestampMs + endValue;

    this.setChartRange(startTimestampMs, endTimestampMs);
    this.resetDragSelection();
  }

  onChartPointerCancel(): void {
    this.resetDragSelection();
  }

  readonly excludedMetadataKeys: string[] = [
    'asset_id',
    'event_slug',
    'event_title',
    'market_question',
    'image_url',
  ];

  formatMetadataKey(key: string): string {
    return key.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase());
  }

  getTimestampMs(value: unknown): number {
    return Number(value) * 1000;
  }

  private loadAll(): void {
    this.loading = true;
    this.error = '';
    this.relatedMarkets = [];
    this.selectedRelatedMarketId = '';
    this.loadingRelatedMarkets = false;
    this.relatedMarketsError = '';
    this.currentTradePoints = [];
    this.tradePointState = '';

    this.api.getMetadata(this.marketId).subscribe({
      next: (metadata) => {
        this.metadata = metadata;
        this.updateOutcomeLabels(metadata.outcomes);
        this.syncDefaultChartRangeInputs();
        this.renderChart(this.currentSeriesPoints, this.currentTradePoints);
        this.loadRelatedMarkets(metadata.event_slug);
      },
      error: (err) => {
        this.error = `Metadata error: ${err?.message ?? 'unknown error'}`;
      },
    });

    this.api.getStats(this.marketId).subscribe({
      next: (stats) => {
        this.stats = stats;
      },
      error: (err) => {
        this.error = `Stats error: ${err?.message ?? 'unknown error'}`;
      },
    });

    this.loadSeries();

    this.api.getRows(this.marketId, this.limit, this.offset).subscribe({
      next: (response: PaginatedResponse<MarketRow>) => {
        this.rows = response.items;
        this.total = response.total;
        this.loading = false;
      },
      error: (err) => {
        this.error = `Rows error: ${err?.message ?? 'unknown error'}`;
        this.loading = false;
      },
    });
  }

  private loadSeriesForCurrentRange(): void {
    const startInput = this.chartRangeStart().trim();
    const endInput = this.chartRangeEnd().trim();
    const startMs = startInput ? this.parseLocalDateTime(startInput) : Number.NEGATIVE_INFINITY;
    const endMs = endInput ? this.parseLocalDateTime(endInput) : Number.POSITIVE_INFINITY;

    if ((startInput && Number.isNaN(startMs)) || (endInput && Number.isNaN(endMs))) {
      this.timeFilterError = 'Enter valid local start and end times.';
      return;
    }

    if (startMs > endMs) {
      this.timeFilterError = 'Start time must be before end time.';
      return;
    }

    this.timeFilterError = '';
    this.loadSeries(
      Number.isFinite(startMs) ? startMs / 1000 : undefined,
      Number.isFinite(endMs) ? endMs / 1000 : undefined,
    );
  }

  private setChartRange(startTimestampMs: number, endTimestampMs: number): void {
    this.chartRangeStart.set(this.formatDateTimeLocalValue(startTimestampMs));
    this.chartRangeEnd.set(this.formatDateTimeLocalValue(endTimestampMs));
    this.timeFilterError = '';
    this.loadSeries(startTimestampMs / 1000, endTimestampMs / 1000);
  }

  private parseLocalDateTime(value: string): number {
    return new Date(value).getTime();
  }

  private setChartData(points: MarketSeriesPoint[]): void {
    this.lastSeriesTimestampMs = (points.at(-1)?.timestamp ?? 0) * 1000;
    this.syncDefaultChartRangeInputs();
    this.renderChart(points, this.currentTradePoints);
  }

  private syncDefaultChartRangeInputs(): void {
    if (!this.chartRangeStart()) {
      this.chartRangeStart.set(this.defaultChartRangeStart());
    }

    if (!this.chartRangeEnd()) {
      this.chartRangeEnd.set(this.defaultChartRangeEnd());
    }
  }

  private defaultChartRangeStart(): string {
    const timestampMs = (this.stats?.first_ts ?? 0) * 1000;
    return timestampMs > 0 ? this.formatDateTimeLocalValue(timestampMs) : '';
  }

  private defaultChartRangeEnd(): string {
    const timestampMs = (this.stats?.last_ts ?? 0) * 1000 || this.lastSeriesTimestampMs;
    return timestampMs > 0 ? this.formatDateTimeLocalValue(timestampMs) : '';
  }

  private formatDateTimeLocalValue(timestampMs: number): string {
    const date = new Date(timestampMs);
    const timezoneOffsetMs = date.getTimezoneOffset() * 60000;
    return new Date(timestampMs - timezoneOffsetMs).toISOString().slice(0, 16);
  }

  private loadSeries(startTs?: number, endTs?: number): void {
    this.seriesLoading = true;
    this.tradePointsLoading = true;
    const minTradeSize: number | undefined = this.parseMinTradeSize();

    this.api.getSeries(this.marketId, 400, startTs, endTs).subscribe({
      next: (series) => {
        this.setChartData(series);
        this.seriesLoading = false;
      },
      error: (err) => {
        this.error = `Series error: ${err?.message ?? 'unknown error'}`;
        this.seriesLoading = false;
      },
    });

    this.api.getTradeSeries(this.marketId, 3000, minTradeSize, startTs, endTs).subscribe({
      next: (trades) => {
        this.currentTradePoints = trades;
        this.tradePointState = trades.length > 0 ? '' : 'No trades matched the selected range and minimum size.';
        this.renderChart(this.currentSeriesPoints, trades);
        this.tradePointsLoading = false;
      },
      error: (err: HttpErrorResponse) => {
        if (err.status === 404) {
          this.currentTradePoints = [];
          this.tradePointState = 'No trade data available yet.';
          this.renderChart(this.currentSeriesPoints, []);
          this.tradePointsLoading = false;
          return;
        }

        this.tradePointState = 'Trade dots unavailable.';
        this.tradePointsLoading = false;
      },
    });
  }

  private clampToChartX(pixelX: number): number {
    const chartArea = this.chartDirective?.chart?.chartArea;
    if (!chartArea) {
      return pixelX;
    }

    return Math.min(Math.max(pixelX, chartArea.left), chartArea.right);
  }

  private updateDragSelectionBox(): void {
    this.dragSelectionVisible = true;
    this.dragSelectionLeftPx = Math.min(this.dragStartPixelX, this.dragCurrentPixelX);
    this.dragSelectionWidthPx = Math.abs(this.dragCurrentPixelX - this.dragStartPixelX);
  }

  private resetDragSelection(): void {
    this.dragSelectionVisible = false;
    this.dragSelectionLeftPx = 0;
    this.dragSelectionWidthPx = 0;
    this.dragSelectionTopPx = 0;
    this.dragSelectionHeightPx = 0;
    this.dragStartPixelX = 0;
    this.dragCurrentPixelX = 0;
  }

  private tooltipLabel(context: TooltipItem<'line' | 'bubble'>): string | string[] {
    if (context.dataset.label === 'Trade activity') {
      const trade = this.currentTradePoints[context.dataIndex];
      if (!trade) {
        return 'Trade activity';
      }

      return [
        `Trade: ${trade.side}`,
        `Chart price: ${this.tradeDisplayPrice(trade).toFixed(3)}`,
        `Raw price: ${trade.price.toFixed(3)}`,
        `Size: ${trade.size.toFixed(3)}`,
        `Notional: ${this.formatVolume(trade.notional_usd)}`,
      ];
    }

    if (context.datasetIndex !== 0) {
      return '';
    }

    const seriesPoint = this.currentSeriesPoints[context.dataIndex];
    if (!seriesPoint) {
      const value = typeof context.parsed.y === 'number' ? context.parsed.y.toFixed(3) : '';
      return `${context.dataset.label}: ${value}`;
    }

    return [
      `${this.outcome1Label}: ${seriesPoint.best_ask.toFixed(3)}`,
      `${this.outcome2Label}: ${(1 - seriesPoint.best_bid).toFixed(3)}`,
      `Spread (Ask - Bid): ${(seriesPoint.best_ask - seriesPoint.best_bid).toFixed(3)}`,
    ];
  }

  private tooltipTimestampMs(items: TooltipItem<'line' | 'bubble'>[]): number | null {
    if (items.length === 0) {
      return null;
    }

    const tradeItem = items.find((item) => item.dataset.label === 'Trade activity');
    if (tradeItem) {
      const trade = this.currentTradePoints[tradeItem.dataIndex];
      return trade ? trade.timestamp * 1000 : null;
    }

    const seriesItem = items[0];
    const seriesPoint = this.currentSeriesPoints[seriesItem.dataIndex];
    if (seriesPoint) {
      return seriesPoint.timestamp * 1000;
    }

    const xValue = seriesItem?.parsed.x;
    if (typeof xValue !== 'number') {
      return null;
    }

    return this.firstSeriesTimestampMs + xValue;
  }

  private renderChart(points: MarketSeriesPoint[], trades: TradeRow[]): void {
    this.currentSeriesPoints = points;
    this.currentTradePoints = trades;

    const firstTimestamp = points[0]?.timestamp ?? trades[0]?.timestamp ?? 0;
    this.firstSeriesTimestampMs = firstTimestamp * 1000;

    const chartPoints: ScatterDataPoint[] = points.map((point): ScatterDataPoint => ({
      x: (point.timestamp - firstTimestamp) * 1000,
      y: point.best_ask,
    }));
    const invertedChartPoints: ScatterDataPoint[] = points.map((point): ScatterDataPoint => ({
      x: (point.timestamp - firstTimestamp) * 1000,
      y: 1.0 - point.best_bid,
    }));
    const spreadPoints: ScatterDataPoint[] = points.map((point): ScatterDataPoint => ({
      x: (point.timestamp - firstTimestamp) * 1000,
      y: point.best_ask - point.best_bid,
    }));

    const maxTradeSize = Math.max(...trades.map((trade) => trade.size), 0);
    const tradeBubblePoints: BubbleDataPoint[] = trades.map((trade): BubbleDataPoint => {
      return {
        x: (trade.timestamp - firstTimestamp) * 1000,
        y: this.tradeDisplayPrice(trade),
        r: this.tradeBubbleRadius(trade.size, maxTradeSize),
      };
    });

    const tradeDataset = this.tradeBubbleDataset(tradeBubblePoints, trades);

    this.chartData = {
      datasets: [
        {
          data: chartPoints,
          label: this.outcome1Label,
          type: 'line',
          borderColor: '#0b6b36',
          pointRadius: 0,
          tension: 0.15,
        },
        {
          data: invertedChartPoints,
          label: this.outcome2Label,
          type: 'line',
          borderColor: '#8f1f1f',
          pointRadius: 0,
          tension: 0.15,
        },
        {
          data: spreadPoints,
          label: 'Spread (Ask - Bid)',
          type: 'line',
          borderColor: '#0f172a',
          pointRadius: 0,
          tension: 0.15,
        },
        tradeDataset,
      ],
    };

    const allXValues = [
      ...chartPoints.map((point) => point.x),
      ...tradeBubblePoints.map((point) => point.x),
    ].filter((value): value is number => typeof value === 'number');
    const lastElapsedMs = allXValues.length > 0 ? Math.max(...allXValues) : 0;
    const xAxisOptions = this.chartOptions.scales?.['x'];
    if (!xAxisOptions) {
      throw new Error('Expected x-axis chart options to be configured.');
    }

    this.chartOptions = {
      ...this.chartOptions,
      scales: {
        ...this.chartOptions.scales,
        x: {
          ...xAxisOptions,
          min: 0,
          max: lastElapsedMs,
        },
      },
    };
  }

  private tradeBubbleDataset(points: BubbleDataPoint[], trades: TradeRow[]): MixedChartDataset {
    const backgroundColor: string[] = trades.map((trade) => {
      if (trade.side === 'BUY') {
        return 'rgba(22, 163, 74, 0.28)';
      }
      if (trade.side === 'SELL') {
        return 'rgba(220, 38, 38, 0.28)';
      }
      return 'rgba(71, 85, 105, 0.26)';
    });

    const borderColor: string[] = trades.map((trade) => {
      if (trade.side === 'BUY') {
        return 'rgba(21, 128, 61, 0.9)';
      }
      if (trade.side === 'SELL') {
        return 'rgba(185, 28, 28, 0.9)';
      }
      return 'rgba(51, 65, 85, 0.8)';
    });

    return {
      data: points,
      label: 'Trade activity',
      type: 'bubble',
      backgroundColor,
      borderColor,
      borderWidth: 2,
      hoverBorderWidth: 3,
    };
  }

  private tradeBubbleRadius(value: number, maxValue: number): number {
    if (maxValue <= 0) {
      return 0;
    }
    const scaled = value / maxValue;
    return 4 + (scaled * 10);
  }

  private tradeDisplayPrice(trade: TradeRow): number {
    return trade.side === 'SELL' ? 1 - trade.price : trade.price;
  }

  private parseMinTradeSize(): number | undefined {
    const rawSignalValue = this.appliedMinTradeSize();
    const rawValue = String(rawSignalValue ?? '').trim();
    if (!rawValue) {
      return undefined;
    }

    const parsedValue = Number(rawValue);
    if (Number.isNaN(parsedValue) || parsedValue < 0) {
      return undefined;
    }

    return parsedValue;
  }

  private updateOutcomeLabels(outcomesRaw: string): void {
    try {
      const parsed: unknown = JSON.parse(outcomesRaw);
      if (Array.isArray(parsed) && parsed.length >= 2) {
        this.outcome1Label = String(parsed[0] ?? 'Outcome 1');
        this.outcome2Label = String(parsed[1] ?? 'Outcome 2');
        return;
      }
    } catch {
      // Use defaults if metadata outcomes is malformed.
    }

    this.outcome1Label = 'Outcome 1';
    this.outcome2Label = 'Outcome 2';
  }

  private loadRelatedMarkets(eventSlug: string): void {
    if (!eventSlug) {
      return;
    }
    this.loadingRelatedMarkets = true;
    this.relatedMarketsError = '';
    this.fetchRelatedMarketsPage(eventSlug, 0, []);
  }

  private fetchRelatedMarketsPage(eventSlug: string, offset: number, acc: MarketSummary[]): void {
    const pageSize = 500;
    this.api.getMarkets(eventSlug, '', pageSize, offset).subscribe({
      next: (response: PaginatedResponse<MarketSummary>) => {
        const nextAcc = [...acc, ...response.items];
        const nextOffset = offset + response.items.length;
        if (nextOffset < response.total) {
          this.fetchRelatedMarketsPage(eventSlug, nextOffset, nextAcc);
          return;
        }

        this.relatedMarkets = nextAcc.sort((a, b) => {
          if (b.volume !== a.volume) {
            return b.volume - a.volume;
          }
          return b.row_count - a.row_count;
        });
        if (this.relatedMarkets.some((m) => m.market_id === this.marketId)) {
          this.selectedRelatedMarketId = this.marketId;
        } else {
          this.selectedRelatedMarketId = this.relatedMarkets[0]?.market_id ?? '';
        }
        this.loadingRelatedMarkets = false;
      },
      error: (err) => {
        this.relatedMarketsError = `Related markets error: ${err?.message ?? 'unknown error'}`;
        this.loadingRelatedMarkets = false;
      },
    });
  }

  private updateQueryParams(): void {
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        limit: this.limit,
        offset: this.offset,
      },
      queryParamsHandling: 'merge',
    });
  }
}
