import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

import {
  EventSummary,
  MarketMetadataDto,
  MarketRow,
  MarketSeriesPoint,
  MarketStats,
  MarketSummary,
  PaginatedResponse,
} from '../models/api.models';
import { environment } from '../../environments/environment';

@Injectable({ providedIn: 'root' })
export class MarketApiService {
  private readonly base = environment.apiBaseUrl;

  constructor(private readonly http: HttpClient) {}

  getEvents(): Observable<EventSummary[]> {
    return this.http.get<EventSummary[]>(`${this.base}/api/v1/events`);
  }

  getMarkets(eventSlug: string, q: string, limit: number, offset: number): Observable<PaginatedResponse<MarketSummary>> {
    let params = new HttpParams().set('limit', limit).set('offset', offset);
    if (eventSlug) {
      params = params.set('event_slug', eventSlug);
    }
    if (q) {
      params = params.set('q', q);
    }
    return this.http.get<PaginatedResponse<MarketSummary>>(`${this.base}/api/v1/markets`, { params });
  }

  getMetadata(marketId: string): Observable<MarketMetadataDto> {
    return this.http.get<MarketMetadataDto>(`${this.base}/api/v1/markets/${marketId}/metadata`);
  }

  getRows(marketId: string, limit: number, offset: number): Observable<PaginatedResponse<MarketRow>> {
    const params = new HttpParams().set('limit', limit).set('offset', offset);
    return this.http.get<PaginatedResponse<MarketRow>>(`${this.base}/api/v1/markets/${marketId}/rows`, { params });
  }

  getSeries(marketId: string, maxPoints: number, startTs?: number, endTs?: number): Observable<MarketSeriesPoint[]> {
    let params = new HttpParams().set('max_points', maxPoints);
    if (startTs != null) {
      params = params.set('start_ts', startTs);
    }
    if (endTs != null) {
      params = params.set('end_ts', endTs);
    }
    return this.http.get<MarketSeriesPoint[]>(`${this.base}/api/v1/markets/${marketId}/series`, { params });
  }

  getStats(marketId: string): Observable<MarketStats> {
    return this.http.get<MarketStats>(`${this.base}/api/v1/markets/${marketId}/stats`);
  }
}
