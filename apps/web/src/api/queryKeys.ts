export const queryKeys = {
  indexerStatus: ['indexer', 'status'] as const,
  blocks: (limit: number) => ['blocks', { limit }] as const,
};
