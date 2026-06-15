import { useQuery } from '@tanstack/preact-query';
import { api } from '../../api/client';
import { queryKeys } from '../../api/queryKeys';

export function IndexerStatusPanel() {
  const status = useQuery({
    queryKey: queryKeys.indexerStatus,
    queryFn: async () => {
      const { data, error } = await api.GET('/api/v1/indexer/status');
      if (error) {
        throw new Error('failed to load indexer status');
      }
      return data;
    },
  });

  if (status.isLoading) {
    return <section class="panel">Loading indexer status</section>;
  }

  if (status.isError) {
    return <section class="panel panel-error">Indexer status unavailable</section>;
  }

  if (!status.data) {
    return <section class="panel">Indexer status unavailable</section>;
  }

  return (
    <section class={`panel ${status.data.state === 'error' ? 'panel-error' : ''}`}>
      <h2>Indexer</h2>
      <dl class="metric-list">
        <div>
          <dt>State</dt>
          <dd>{status.data.state}</dd>
        </div>
        <div>
          <dt>Network</dt>
          <dd>{status.data.network}</dd>
        </div>
        <div>
          <dt>Indexed score</dt>
          <dd>{status.data.indexed_score ?? 'unknown'}</dd>
        </div>
        <div>
          <dt>Node score</dt>
          <dd>{status.data.virtual_daa_score ?? 'unknown'}</dd>
        </div>
        <div>
          <dt>DAA lag</dt>
          <dd>{status.data.lag_daa_score ?? 'unknown'}</dd>
        </div>
        <div>
          <dt>Last poll</dt>
          <dd>{formatTime(status.data.last_poll_finished_at)}</dd>
        </div>
        <div>
          <dt>Last batch</dt>
          <dd>
            {status.data.last_indexed_blocks ?? 0} blocks ·{' '}
            {status.data.last_indexed_transactions ?? 0} tx
          </dd>
        </div>
        <div>
          <dt>Duration</dt>
          <dd>{formatDuration(status.data.last_poll_duration_ms)}</dd>
        </div>
        <div>
          <dt>Source</dt>
          <dd>{status.data.source}</dd>
        </div>
        {status.data.last_error ? (
          <div>
            <dt>Last error</dt>
            <dd class="error-text">{status.data.last_error}</dd>
          </div>
        ) : null}
      </dl>
    </section>
  );
}

function formatTime(value?: string | null) {
  if (!value) {
    return 'unknown';
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'short',
    timeStyle: 'medium',
  }).format(new Date(value));
}

function formatDuration(value?: number | null) {
  if (value == null) {
    return 'unknown';
  }

  if (value < 1000) {
    return `${value} ms`;
  }

  return `${(value / 1000).toFixed(1)} s`;
}
