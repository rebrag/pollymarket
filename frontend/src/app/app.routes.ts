import { Routes } from '@angular/router';

import { ExplorerPageComponent } from './pages/explorer/explorer-page.component';
import { MarketDetailPageComponent } from './pages/market-detail/market-detail-page.component';

export const routes: Routes = [
  { path: '', component: ExplorerPageComponent },
  { path: 'markets/:marketId', component: MarketDetailPageComponent },
  { path: '**', redirectTo: '' },
];
