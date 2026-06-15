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
        <div class="transaction-result">
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
            <div>
              <dt>Detail</dt>
              <dd>
                {transaction.data.detail_available
                  ? transaction.data.detail_complete
                    ? 'complete'
                    : 'partial'
                  : 'summary only'}
              </dd>
            </div>
          </dl>

          {transaction.data.detail ? (
            <div class="transaction-detail-grid">
              <section>
                <h3>Context</h3>
                <dl class="metric-list">
                  <div>
                    <dt>DAA score</dt>
                    <dd>{transaction.data.detail.accepting_daa_score}</dd>
                  </div>
                  <div>
                    <dt>Timestamp</dt>
                    <dd>{transaction.data.detail.accepting_timestamp}</dd>
                  </div>
                  <div>
                    <dt>Mass</dt>
                    <dd>{transaction.data.detail.mass}</dd>
                  </div>
                  <div>
                    <dt>Compute mass</dt>
                    <dd>{transaction.data.detail.compute_mass}</dd>
                  </div>
                  <div>
                    <dt>Storage mass</dt>
                    <dd>{transaction.data.detail.storage_mass}</dd>
                  </div>
                  <div>
                    <dt>Payload bytes</dt>
                    <dd>{transaction.data.detail.payload_size}</dd>
                  </div>
                </dl>
              </section>

              <section>
                <h3>Inputs</h3>
                <ol class="transaction-io-list">
                  {transaction.data.detail.inputs.map((input, index) => (
                    <li key={index}>
                      <span>{input.previous_txid ?? 'coinbase or unresolved'}</span>
                      <small>
                        {input.previous_output_index == null
                          ? 'no outpoint'
                          : `output ${input.previous_output_index}`}
                        {' · '}
                        {input.previous_output_resolved ? 'resolved' : 'unresolved'}
                      </small>
                    </li>
                  ))}
                </ol>
              </section>

              <section>
                <h3>Outputs</h3>
                <ol class="transaction-io-list">
                  {transaction.data.detail.outputs.map((output) => (
                    <li key={output.output_index}>
                      <span>{output.script_public_key_address ?? 'address unavailable'}</span>
                      <small>
                        {output.amount} sompi · {output.script_public_key_type ?? 'unknown script'}
                      </small>
                    </li>
                  ))}
                </ol>
              </section>
            </div>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
