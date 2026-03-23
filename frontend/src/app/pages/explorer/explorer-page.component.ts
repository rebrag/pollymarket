import { Component, computed, inject, OnInit, signal } from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';

import { EventSummary, MarketSummary, PaginatedResponse } from '../../models/api.models';
import { MarketApiService } from '../../services/market-api.service';

interface HeroStat {
  readonly label: string;
  readonly value: string;
  readonly accent: string;
}

interface ActivityItem {
  readonly title: string;
  readonly meta: string;
  readonly value: string;
}

@Component({
  selector: 'app-explorer-page',
  standalone: true,
  imports: [RouterLink],
  templateUrl: './explorer-page.component.html',
  styleUrl: './explorer-page.component.css',
})
export class ExplorerPageComponent implements OnInit {
  private readonly api = inject(MarketApiService);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly numberFormatter = new Intl.NumberFormat('en-US');
  private readonly currencyFormatter = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  });

  readonly marketActivity: readonly ActivityItem[] = [
    { title: 'PLACEHOLDER', meta: '25m ago', value: '$0' },
    { title: 'PLACEHOLDER', meta: '25m ago', value: '$0' },
    { title: 'PLACEHOLDER', meta: '25m ago', value: '$0' },
    { title: 'PLACEHOLDER', meta: '25m ago', value: '$0' },
    { title: 'PLACEHOLDER', meta: '25m ago', value: '$0' },
  ];

  readonly traderActivity: readonly ActivityItem[] = [
    { title: 'PLACEHOLDER', meta: 'Mar 15', value: '$551,305' },
    { title: 'PLACEHOLDER', meta: 'Mar 15', value: '$315,651' },
    { title: 'PLACEHOLDER', meta: 'Mar 15', value: '$276,593' },
    { title: 'PLACEHOLDER', meta: 'Mar 15', value: '$246,710' },
  ];

  readonly events = signal<EventSummary[]>([]);
  readonly marketsByEvent = signal<Record<string, MarketSummary[]>>({});
  readonly expandedEventSlugs = signal<Set<string>>(new Set<string>());
  readonly loadingEventSlugs = signal<Set<string>>(new Set<string>());
  readonly search = signal<string>('');
  readonly loading = signal<boolean>(false);
  readonly error = signal<string>('');

  readonly heroStats = computed((): readonly HeroStat[] => [
    {
      label: 'Events Recorded',
      value: this.numberFormatter.format(this.events().length),
      accent: 'from-sky-500/30 to-cyan-400/10',
    },
    {
      label: 'Markets Recorded',
      value: this.numberFormatter.format(
        this.events().reduce((total, event) => total + event.market_count, 0),
      ),
      accent: 'from-emerald-500/30 to-teal-400/10',
    },
    { label: 'PLACEHOLDER', value: '139,894,409', accent: 'from-orange-500/30 to-amber-400/10' },
  ]);

  readonly filteredEvents = computed((): EventSummary[] => {
    const q = this.search().trim().toLowerCase();
    const events = this.events();

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
      return [...events].sort(compareByGameStart).reverse();
    }

    return events
      .filter((event) => {
        return event.event_title.toLowerCase().includes(q) || event.event_slug.toLowerCase().includes(q);
      })
      .sort(compareByGameStart)
      .reverse();
  });

  ngOnInit(): void {
    this.loadEvents();

    this.route.queryParamMap.subscribe((params) => {
      const nextSearch = params.get('q') ?? '';
      if (nextSearch !== this.search()) {
        this.search.set(nextSearch);
        this.expandedEventSlugs.set(new Set<string>());
        this.marketsByEvent.set({});
      }
    });
  }

  trackByEventSlug(_: number, event: EventSummary): string {
    return event.event_slug;
  }

  applyFilters(): void {
    this.updateQueryParams();
  }

  onSearchInput(event: Event): void {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) {
      throw new Error('Expected an HTML input element for search input.');
    }

    this.search.set(target.value);
  }

  isExpanded(eventSlug: string): boolean {
    return this.expandedEventSlugs().has(eventSlug);
  }

  marketsForEvent(eventSlug: string): MarketSummary[] {
    return this.marketsByEvent()[eventSlug] ?? [];
  }

  isEventLoading(eventSlug: string): boolean {
    return this.loadingEventSlugs().has(eventSlug);
  }

  formatVolume(volume: number): string {
    return this.currencyFormatter.format(volume);
  }

  toggleEvent(eventSlug: string): void {
    const nextExpanded = new Set<string>(this.expandedEventSlugs());
    if (nextExpanded.has(eventSlug)) {
      nextExpanded.delete(eventSlug);
      this.expandedEventSlugs.set(nextExpanded);
      return;
    }

    nextExpanded.add(eventSlug);
    this.expandedEventSlugs.set(nextExpanded);
    this.loadMarketsForEvent(eventSlug);
  }

  private loadEvents(): void {
    this.loading.set(true);
    this.error.set('');
    this.api.getEvents().subscribe({
      next: (events) => {
        this.events.set(events);
        this.loading.set(false);
      },
      error: (err) => {
        this.error.set(`Failed loading events: ${err?.message ?? 'unknown error'}`);
        this.loading.set(false);
      },
    });
  }

  private loadMarketsForEvent(eventSlug: string): void {
    if (this.marketsByEvent()[eventSlug]) {
      return;
    }

    const nextLoading = new Set<string>(this.loadingEventSlugs());
    nextLoading.add(eventSlug);
    this.loadingEventSlugs.set(nextLoading);
    this.fetchEventMarketsPage(eventSlug, 0, []);
  }

  private fetchEventMarketsPage(eventSlug: string, offset: number, acc: MarketSummary[]): void {
    const pageSize = 500;
    this.api.getMarkets(eventSlug, this.search(), pageSize, offset).subscribe({
      next: (response: PaginatedResponse<MarketSummary>) => {
        const nextAcc = [...acc, ...response.items];
        const nextOffset = offset + response.items.length;
        if (nextOffset < response.total) {
          this.fetchEventMarketsPage(eventSlug, nextOffset, nextAcc);
          return;
        }

        nextAcc.sort((a, b) => {
          if (b.volume !== a.volume) {
            return b.volume - a.volume;
          }
          return b.row_count - a.row_count;
        });

        this.marketsByEvent.update((markets): Record<string, MarketSummary[]> => ({
          ...markets,
          [eventSlug]: nextAcc,
        }));
        this.loadingEventSlugs.update((loadingEventSlugs): Set<string> => {
          const nextLoading = new Set<string>(loadingEventSlugs);
          nextLoading.delete(eventSlug);
          return nextLoading;
        });
      },
      error: (err) => {
        this.error.set(`Failed loading markets for ${eventSlug}: ${err?.message ?? 'unknown error'}`);
        this.loadingEventSlugs.update((loadingEventSlugs): Set<string> => {
          const nextLoading = new Set<string>(loadingEventSlugs);
          nextLoading.delete(eventSlug);
          return nextLoading;
        });
      },
    });
  }

  private updateQueryParams(): void {
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        q: this.search() || null,
      },
      queryParamsHandling: 'merge',
    });
  }
}
