import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router, RouterLink } from '@angular/router';
import { ActivatedRoute } from '@angular/router';

import { EventSummary, MarketSummary, PaginatedResponse } from '../../models/api.models';
import { MarketApiService } from '../../services/market-api.service';

@Component({
  selector: 'app-explorer-page',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './explorer-page.component.html',
  styleUrl: './explorer-page.component.css',
})
export class ExplorerPageComponent implements OnInit {
  events: EventSummary[] = [];
  marketsByEvent: Record<string, MarketSummary[]> = {};
  expandedEventSlugs = new Set<string>();
  loadingEventSlugs = new Set<string>();

  search = '';
  loading = false;
  error = '';

  constructor(
    private readonly api: MarketApiService,
    private readonly route: ActivatedRoute,
    private readonly router: Router,
  ) {}

  ngOnInit(): void {
    this.loadEvents();

    this.route.queryParamMap.subscribe((params) => {
      const nextSearch = params.get('q') ?? '';
      if (nextSearch !== this.search) {
        this.search = nextSearch;
        this.expandedEventSlugs.clear();
        this.marketsByEvent = {};
      }
    });
  }

  filteredEvents(): EventSummary[] {
    const q = this.search.trim().toLowerCase();
    const compareByGameStart = (a: EventSummary, b: EventSummary): number => {
      const aTs = a.game_start_time ?? 0;
      const bTs = b.game_start_time ?? 0;
      if (aTs <= 0 && bTs <= 0) {
        return a.event_slug.localeCompare(b.event_slug);
      }
      if (aTs <= 0) {
        return 1;
      }
      if (bTs <= 0) {
        return -1;
      }
      if (aTs !== bTs) {
        return aTs - bTs;
      }
      return a.event_slug.localeCompare(b.event_slug);
    };

    if (!q) {
      return [...this.events].sort(compareByGameStart);
    }

    return this.events
      .filter((event) => {
        return event.event_title.toLowerCase().includes(q) || event.event_slug.toLowerCase().includes(q);
      })
      .sort(compareByGameStart);
  }

  applyFilters(): void {
    this.updateQueryParams();
  }

  isExpanded(eventSlug: string): boolean {
    return this.expandedEventSlugs.has(eventSlug);
  }

  marketsForEvent(eventSlug: string): MarketSummary[] {
    return this.marketsByEvent[eventSlug] ?? [];
  }

  isEventLoading(eventSlug: string): boolean {
    return this.loadingEventSlugs.has(eventSlug);
  }

  toggleEvent(eventSlug: string): void {
    if (this.expandedEventSlugs.has(eventSlug)) {
      this.expandedEventSlugs.delete(eventSlug);
      return;
    }

    this.expandedEventSlugs.add(eventSlug);
    this.loadMarketsForEvent(eventSlug);
  }

  private loadEvents(): void {
    this.loading = true;
    this.api.getEvents().subscribe({
      next: (events) => {
        this.events = events;
        this.loading = false;
      },
      error: (err) => {
        this.error = `Failed loading events: ${err?.message ?? 'unknown error'}`;
        this.loading = false;
      },
    });
  }

  private loadMarketsForEvent(eventSlug: string): void {
    if (this.marketsByEvent[eventSlug]) {
      return;
    }

    this.loadingEventSlugs.add(eventSlug);
    this.fetchEventMarketsPage(eventSlug, 0, []);
  }

  private fetchEventMarketsPage(eventSlug: string, offset: number, acc: MarketSummary[]): void {
    const pageSize = 500;
    this.api.getMarkets(eventSlug, this.search, pageSize, offset).subscribe({
      next: (response: PaginatedResponse<MarketSummary>) => {
        const nextAcc = [...acc, ...response.items];
        const nextOffset = offset + response.items.length;
        if (nextOffset < response.total) {
          this.fetchEventMarketsPage(eventSlug, nextOffset, nextAcc);
          return;
        }

        this.marketsByEvent[eventSlug] = nextAcc;
        this.loadingEventSlugs.delete(eventSlug);
      },
      error: (err) => {
        this.error = `Failed loading markets for ${eventSlug}: ${err?.message ?? 'unknown error'}`;
        this.loadingEventSlugs.delete(eventSlug);
      },
    });
  }

  private updateQueryParams(): void {
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        q: this.search || null,
      },
      queryParamsHandling: 'merge',
    });
  }
}
