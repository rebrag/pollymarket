import { Component } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';

interface NavItem {
  readonly label: string;
  readonly href: string;
}

@Component({
  selector: 'app-root',
  imports: [RouterLink, RouterLinkActive, RouterOutlet],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  readonly navItems: readonly NavItem[] = [
    { label: 'Home', href: '/' },
    { label: 'PLACEHOLDER', href: '/' },
    { label: 'PLACEHOLDER', href: '/' },
    { label: 'PLACEHOLDER', href: '/' },
  ];
}
