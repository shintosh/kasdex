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
    <section class="panel">
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
      </dl>
    </section>
  );
}
