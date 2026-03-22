import { Component, OnInit, ViewChild, signal } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { BaseChartDirective } from 'ng2-charts';
import { combineLatest } from 'rxjs';
import { BrnLabelDirective } from '@spartan-ng/ui-label-brain';
import {
  Chart,
  ChartData,
  ChartOptions,
  LineController,
  LineElement,
  PointElement,
  LinearScale,
  ScatterDataPoint,
  Tooltip,
  Legend,
  CategoryScale,
  Plugin,
} from 'chart.js';

import {
  MarketMetadataDto,
  MarketRow,
  MarketSeriesPoint,
  MarketSummary,
  MarketStats,
  PaginatedResponse,
} from '../../models/api.models';
import { MarketApiService } from '../../services/market-api.service';

Chart.register(LineController, LineElement, PointElement, LinearScale, Tooltip, Legend, CategoryScale);

const hoverGuideLinePlugin: Plugin<'line'> = {
  id: 'hoverGuideLine',
  afterDatasetsDraw(chart) {
    const active = chart.tooltip?.getActiveElements();
    if (!active || active.length === 0) {
      return;
    }

    const x = active[0].element.x;
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
  @ViewChild(BaseChartDirective) private readonly chartDirective?: BaseChartDirective<'line'>;

  readonly Math = Math;
  marketId = '';
  metadata: MarketMetadataDto | null = null;
  stats: MarketStats | null = null;
  rows: MarketRow[] = [];
  showRawData = false;
  outcome1Label = 'Outcome 1';
  outcome2Label = 'Outcome 2';
  currentSeriesPoints: MarketSeriesPoint[] = [];
  firstSeriesTimestampMs = 0;
  lastSeriesTimestampMs = 0;
  readonly chartRangeStart = signal('');
  readonly chartRangeEnd = signal('');
  timeFilterError = '';
  seriesLoading = false;
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

  chartData: ChartData<'line', ScatterDataPoint[]> = {
    datasets: [
      { data: [], label: 'Outcome 1', borderColor: '#0b6b36', tension: 0.15, pointRadius: 0 },
      { data: [], label: 'Outcome 2', borderColor: '#8f1f1f', tension: 0.15, pointRadius: 0 },
      { data: [], label: 'Spread (Ask - Bid)', borderColor: '#0f172a', tension: 0.15, pointRadius: 0 },
    ],
  };

  chartOptions: ChartOptions<'line'> = {
    responsive: true,
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
        mode: 'index',
        intersect: false,
        callbacks: {
          title: (items): string => {
            const firstItem = items[0];
            const xValue = firstItem?.parsed.x;
            if (xValue == null) {
              return '';
            }

            const pointIndex = firstItem.dataIndex;
            const actualTimestampMs = this.currentSeriesPoints[pointIndex]?.timestamp
              ? this.currentSeriesPoints[pointIndex].timestamp * 1000
              : null;

            if (actualTimestampMs == null) {
              return new Date(this.firstSeriesTimestampMs + xValue).toLocaleString([], {
                month: 'short',
                day: 'numeric',
                hour: 'numeric',
                minute: '2-digit',
                second: '2-digit',
              });
            }

            return new Date(actualTimestampMs).toLocaleString([], {
              month: 'short',
              day: 'numeric',
              hour: 'numeric',
              minute: '2-digit',
              second: '2-digit',
            });
          },
        },
      },
    },
    interaction: {
      mode: 'index',
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

  private loadAll(): void {
    this.loading = true;
    this.error = '';
    this.relatedMarkets = [];
    this.selectedRelatedMarketId = '';
    this.loadingRelatedMarkets = false;
    this.relatedMarketsError = '';

    this.api.getMetadata(this.marketId).subscribe({
      next: (metadata) => {
        this.metadata = metadata;
        this.updateOutcomeLabels(metadata.outcomes);
        this.syncDefaultChartRangeInputs();
        this.renderChart(this.currentSeriesPoints);
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
    this.renderChart(points);
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

  private renderChart(points: MarketSeriesPoint[]): void {
    this.currentSeriesPoints = points;
    const firstTimestamp = points[0]?.timestamp ?? 0;
    this.firstSeriesTimestampMs = firstTimestamp * 1000;

    const chartPoints = points.map((point): ScatterDataPoint => ({
      x: (point.timestamp - firstTimestamp) * 1000,
      y: point.best_ask,
    }));
    const invertedChartPoints = points.map((point): ScatterDataPoint => ({
      x: (point.timestamp - firstTimestamp) * 1000,
      y: 1.0 - point.best_bid,
    }));
    const spreadPoints = points.map((point): ScatterDataPoint => ({
      x: (point.timestamp - firstTimestamp) * 1000,
      y: point.best_ask - point.best_bid,
    }));

    this.chartData = {
      datasets: [
        {
          data: chartPoints,
          label: this.outcome1Label,
          borderColor: '#0b6b36',
          pointRadius: 0,
          tension: 0.15,
        },
        {
          data: invertedChartPoints,
          label: this.outcome2Label,
          borderColor: '#8f1f1f',
          pointRadius: 0,
          tension: 0.15,
        },
        {
          data: spreadPoints,
          label: 'Spread (Ask - Bid)',
          borderColor: '#0f172a',
          pointRadius: 0,
          tension: 0.15,
        },
      ],
    };

    const lastElapsedMs = chartPoints.at(-1)?.x ?? 0;
    if (!this.chartOptions.scales?.['x']) {
      throw new Error('Expected x-axis chart options to be configured.');
    }

    this.chartOptions = {
      ...this.chartOptions,
      scales: {
        ...this.chartOptions.scales,
        x: {
          ...this.chartOptions.scales['x'],
          min: 0,
          max: lastElapsedMs,
        },
      },
    };
  }

  private updateOutcomeLabels(outcomesRaw: string): void {
    try {
      const parsed = JSON.parse(outcomesRaw);
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

        this.relatedMarkets = nextAcc.sort((a, b) => b.row_count - a.row_count);
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

  readonly excludedMetadataKeys: string[] = [
    'asset_id',
    'event_slug',
    'event_title',
    'market_question',
    'image_url' 
  ];

  formatMetadataKey(key: string): string {
    return key.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase());
  }

  getTimestampMs(value: unknown): number {
    return Number(value) * 1000;
  }

  
}
