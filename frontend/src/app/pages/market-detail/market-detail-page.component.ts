import { Component, OnInit } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { BaseChartDirective } from 'ng2-charts';
import { combineLatest } from 'rxjs';
import {
  Chart,
  ChartData,
  ChartOptions,
  LineController,
  LineElement,
  PointElement,
  LinearScale,
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
  imports: [CommonModule, FormsModule, BaseChartDirective, DatePipe, RouterLink],
  templateUrl: './market-detail-page.component.html',
  styleUrl: './market-detail-page.component.css',
})
export class MarketDetailPageComponent implements OnInit {
  readonly Math = Math;
  marketId = '';
  metadata: MarketMetadataDto | null = null;
  stats: MarketStats | null = null;
  rows: MarketRow[] = [];
  showRawData = false;
  outcome1Label = 'Outcome 1';
  outcome2Label = 'Outcome 2';
  currentSeriesPoints: MarketSeriesPoint[] = [];

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

  chartData: ChartData<'line'> = {
    labels: [],
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
        this.setChartData(this.currentSeriesPoints);
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

    this.api.getSeries(this.marketId, 400).subscribe({
      next: (series) => this.setChartData(series),
      error: (err) => {
        this.error = `Series error: ${err?.message ?? 'unknown error'}`;
      },
    });

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

  private setChartData(points: MarketSeriesPoint[]): void {
    this.currentSeriesPoints = points;

    // this.chartData = {
    //   labels: points.map((p) => new Date(p.timestamp * 1000).toLocaleTimeString()),
    //   datasets: [
    //     {
    //       data: points.map((p) => p.best_bid),
    //       label: this.outcome1Label,
    //       borderColor: '#0b6b36',
    //       pointRadius: 0,
    //       tension: 0.15,
    //     },
    //     {
    //       data: points.map((p) => 1.0 - p.best_ask),
    //       label: this.outcome2Label,
    //       borderColor: '#8f1f1f',
    //       pointRadius: 0,
    //       tension: 0.15,
    //     },
    //     {
    //       data: points.map((p) => p.best_ask - p.best_bid),
    //       label: 'Spread (Ask - Bid)',
    //       borderColor: '#0f172a',
    //       pointRadius: 0,
    //       tension: 0.15,
    //     },
    //   ],
    // };

    this.chartData = {
      labels: points.map((p) => new Date(p.timestamp * 1000).toLocaleTimeString()),
      datasets: [
        {
          data: points.map((p) => p.best_ask),
          label: this.outcome1Label,
          borderColor: '#0b6b36',
          pointRadius: 0,
          tension: 0.15,
        },
        {
          data: points.map((p) => 1.0 - p.best_bid),
          label: this.outcome2Label,
          borderColor: '#8f1f1f',
          pointRadius: 0,
          tension: 0.15,
        },
        {
          data: points.map((p) => p.best_ask - p.best_bid),
          label: 'Spread (Ask - Bid)',
          borderColor: '#0f172a',
          pointRadius: 0,
          tension: 0.15,
        },
      ],
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
}
