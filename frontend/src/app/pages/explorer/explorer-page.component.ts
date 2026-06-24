import { Component, computed, inject, OnInit, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { catchError, of } from 'rxjs';

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
  readonly url?: string;
  readonly imageUrl?: string;
}

/** Subset of the gamma /events/keyset payload we consume for the live preview. */
interface GammaEvent {
  readonly slug: string;
  readonly title: string;
  readonly startTime: string;
  readonly volume: number;
  readonly volume24hr: number;
  readonly image?: string;
  readonly icon?: string;
}

interface GammaKeysetResponse {
  readonly events: GammaEvent[];
  readonly next_cursor: string | null;
}

interface PolymarketProfitEntry {
  readonly name: string;
  readonly pseudonym: string;
  readonly proxyWalletAddress: string;
  readonly profit: number;
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
  private readonly http = inject(HttpClient);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly numberFormatter = new Intl.NumberFormat('en-US');
  private readonly currencyFormatter = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  });

  readonly marketActivity = signal<ActivityItem[]>([]);
  readonly marketActivityLoading = signal(true);

  readonly traderActivity = signal<ActivityItem[]>([]);
  readonly traderActivityLoading = signal(true);

  readonly pageSize = 50;

  // Live-window + volume thresholds mirrored from fetch_and_filter_gamma_events.py
  private static readonly HOURS_BEFORE = 4;
  private static readonly HOURS_AFTER = 1;
  private static readonly MIN_EVENT_VOL = 500;
  private static readonly MIN_MARKET_VOL = 10_000;

  readonly events = signal<EventSummary[]>([]);
  readonly marketsByEvent = signal<Record<string, MarketSummary[]>>({});
  readonly expandedEventSlugs = signal<Set<string>>(new Set<string>());
  readonly loadingEventSlugs = signal<Set<string>>(new Set<string>());
  readonly search = signal<string>('');
  readonly loading = signal<boolean>(false);
  readonly error = signal<string>('');
  readonly currentPage = signal<number>(0);

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
  ]);

  readonly totalPages = computed((): number => Math.ceil(this.filteredEvents().length / this.pageSize));

  readonly paginatedEvents = computed((): EventSummary[] => {
    const start = this.currentPage() * this.pageSize;
    return this.filteredEvents().slice(start, start + this.pageSize);
  });

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
        return bTs - aTs;
      }
      return a.event_slug.localeCompare(b.event_slug);
    };

    if (!q) {
      return [...events].sort(compareByGameStart);
    }

    return events
      .filter((event) => {
        return event.event_title.toLowerCase().includes(q) || event.event_slug.toLowerCase().includes(q);
      })
      .sort(compareByGameStart);
  });

  ngOnInit(): void {
    this.loadEvents();
    this.loadPolymarketActivity();

    this.route.queryParamMap.subscribe((params) => {
      const nextSearch = params.get('q') ?? '';
      if (nextSearch !== this.search()) {
        this.search.set(nextSearch);
        this.currentPage.set(0);
        this.expandedEventSlugs.set(new Set<string>());
        this.marketsByEvent.set({});
      }
    });
  }

  trackByEventSlug(_: number, event: EventSummary): string {
    return event.event_slug;
  }

  applyFilters(): void {
    this.currentPage.set(0);
    this.updateQueryParams();
  }

  goToPage(page: number): void {
    this.currentPage.set(page);
    this.expandedEventSlugs.set(new Set<string>());
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

  private loadPolymarketActivity(): void {
    this.loadLiveEvents();

    this.http
      .get<PolymarketProfitEntry[]>('https://data-api.polymarket.com/profits?limit=5&interval=1d')
      .pipe(catchError(() => of([])))
      .subscribe((traders) => {
        this.traderActivity.set(
          traders.slice(0, 5).map((t) => {
            const addr = t.proxyWalletAddress ?? '';
            const label = t.pseudonym || t.name || `${addr.slice(0, 6)}…${addr.slice(-4)}`;
            return {
              title: label,
              meta: '24h profit',
              value: this.currencyFormatter.format(t.profit),
            };
          }),
        );
        this.traderActivityLoading.set(false);
      });
  }

  /**
   * Previews the events currently being recorded by poly_parquet_generator.py.
   * Mirrors fetch_and_filter_gamma_events: the gamma /events/keyset feed filtered
   * to a live window (recently started / starting soon) and a minimum event volume.
   */
  private loadLiveEvents(): void {
    const now = Date.now();
    const winStart = new Date(now - ExplorerPageComponent.HOURS_BEFORE * 3_600_000);
    const winEnd = new Date(now + ExplorerPageComponent.HOURS_AFTER * 3_600_000);

    const params = new URLSearchParams({
      limit: '50',
      closed: 'false',
      order: 'volume24hr',
      ascending: 'false',
      start_time_min: winStart.toISOString(),
      start_time_max: winEnd.toISOString(),
      volume_min: String(ExplorerPageComponent.MIN_MARKET_VOL),
      tag_id: '1',
    });

    this.http
      .get<GammaKeysetResponse>(`https://gamma-api.polymarket.com/events/keyset?${params.toString()}`)
      .pipe(catchError(() => of({ events: [], next_cursor: null } as GammaKeysetResponse)))
      .subscribe((response) => {
        const liveEvents = (response.events ?? [])
          .filter((event) => {
            const start = new Date(event.startTime).getTime();
            return (
              start >= winStart.getTime() &&
              start <= winEnd.getTime() &&
              Number(event.volume ?? 0) > ExplorerPageComponent.MIN_EVENT_VOL
            );
          })
          .sort((a, b) => Number(b.volume24hr ?? 0) - Number(a.volume24hr ?? 0));

        this.marketActivity.set(
          liveEvents.slice(0, 8).map((event) => {
            return {
              title: event.title,
              meta: this.eventTiming(event.startTime),
              value: this.compactVolume(Number(event.volume24hr ?? event.volume ?? 0)),
              url: `https://polymarket.com/event/${event.slug}`,
              imageUrl: event.image || event.icon,
            };
          }),
        );
        this.marketActivityLoading.set(false);
      });
  }

  private eventTiming(startTime: string): string {
    const diffMs = new Date(startTime).getTime() - Date.now();
    const mins = Math.round(Math.abs(diffMs) / 60000);
    const label = mins < 60 ? `${mins}m` : `${Math.floor(mins / 60)}h ${mins % 60}m`;
    return diffMs <= 0 ? `Started ${label} ago` : `Starts in ${label}`;
  }

  private compactVolume(volume: number): string {
    if (volume >= 1_000_000) return `$${(volume / 1_000_000).toFixed(1)}M`;
    if (volume >= 1_000) return `$${(volume / 1_000).toFixed(1)}k`;
    return this.currencyFormatter.format(volume);
  }

  private loadEvents(): void {
    let didEmitSync = false;
    this.error.set('');
    this.api.getEvents().subscribe({
      next: (events) => {
        didEmitSync = true;
        this.events.set(events);
        this.loading.set(false);
      },
      error: (err) => {
        didEmitSync = true;
        this.error.set(`Failed loading events: ${err?.message ?? 'unknown error'}`);
        this.loading.set(false);
      },
    });
    if (!didEmitSync) {
      this.loading.set(true);
    }
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
