import { useState } from 'preact/hooks';
import { useQuery } from '@tanstack/preact-query';
import { api } from '../../api/client';
import { queryKeys } from '../../api/queryKeys';

const TXID_PATTERN = /^[0-9a-fA-F]{64}$/;

export function TransactionBrowser() {
  const [input, setInput] = useState('');
  const txid = input.trim().toLowerCase();
  const isValid = TXID_PATTERN.test(txid);

  const transaction = useQuery({
    queryKey: queryKeys.transaction(txid),
    enabled: isValid,
    retry: false,
    queryFn: async () => {
      const { data, error } = await api.GET('/api/v1/transactions/{txid}', {
        params: { path: { txid } },
      });
      if (error) {
        throw new Error(error.message);
      }
      return data;
    },
  });

  return (
    <section class="panel transaction-browser">
      <h2>Transaction Browser</h2>
      <form onSubmit={(event) => event.preventDefault()}>
        <input
          aria-label="Transaction ID"
          autocapitalize="none"
          autocomplete="off"
          inputMode="text"
          placeholder="Paste transaction id"
          spellcheck={false}
          value={input}
          onInput={(event) => setInput(event.currentTarget.value)}
        />
      </form>

      {txid && !isValid ? <p class="error-text">Enter a 64-character hex transaction id</p> : null}
      {transaction.isLoading ? <p class="muted-text">Looking up transaction</p> : null}
      {transaction.isError ? <p class="error-text">Transaction not found in local index</p> : null}
      {transaction.data ? (
        <dl class="metric-list">
          <div>
            <dt>Transaction</dt>
            <dd>{transaction.data.txid}</dd>
          </div>
          <div>
            <dt>Accepting block</dt>
            <dd>{transaction.data.accepting_block_hash ?? 'unknown'}</dd>
          </div>
          <div>
            <dt>Inputs</dt>
            <dd>{transaction.data.input_count}</dd>
          </div>
          <div>
            <dt>Outputs</dt>
            <dd>{transaction.data.output_count}</dd>
          </div>
        </dl>
      ) : null}
    </section>
  );
}
