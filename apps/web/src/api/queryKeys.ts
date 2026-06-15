export const queryKeys = {
  indexerStatus: ['indexer', 'status'] as const,
  blocks: (limit: number) => ['blocks', { limit }] as const,
  transaction: (txid: string) => ['transaction', txid] as const,
};
