import { useQuery } from '@tanstack/preact-query';
import { api } from '../../api/client';
import { queryKeys } from '../../api/queryKeys';

export function RecentBlocks() {
  const limit = 5;
  const blocks = useQuery({
    queryKey: queryKeys.blocks(limit),
    queryFn: async () => {
      const { data, error } = await api.GET('/api/v1/blocks', {
        params: { query: { limit } },
      });
      if (error) {
        throw new Error(error.message);
      }
      return data;
    },
  });

  return (
    <section class="panel">
      <h2>Recent Blocks</h2>
      {blocks.isLoading ? <p>Loading blocks</p> : null}
      {blocks.isError ? <p class="error-text">Blocks unavailable</p> : null}
      {blocks.data ? (
        blocks.data.items.length > 0 ? (
          <ol class="block-list">
            {blocks.data.items.map((block) => (
              <li key={block.hash}>
                <div>
                  <span>{block.hash}</span>
                  <small>
                    DAA {block.daa_score} · {block.tx_count} tx · {block.timestamp}
                  </small>
                </div>
                <strong>{block.blue_score}</strong>
              </li>
            ))}
          </ol>
        ) : (
          <p class="muted-text">No indexed blocks yet</p>
        )
      ) : null}
    </section>
  );
}
