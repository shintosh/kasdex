import { IndexerStatusPanel } from '../features/indexer/IndexerStatusPanel';
import { RecentBlocks } from '../features/dashboard/RecentBlocks';

export function App() {
  return (
    <main class="app-shell">
      <section class="topbar">
        <div>
          <h1>Kasdex</h1>
          <p>Local Kaspa indexer dashboard</p>
        </div>
        <a href="/docs" target="_blank" rel="noreferrer">
          API docs
        </a>
      </section>

      <section class="dashboard-grid">
        <IndexerStatusPanel />
        <RecentBlocks />
      </section>
    </main>
  );
}
