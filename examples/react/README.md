# React example (skeleton)

```tsx
import { createConnectTransport } from "@connectrpc/connect-web";
import { createClient } from "@connectrpc/connect";
import { LedgerService } from "./gen/proto/ledger/v1/ledger_connect";

const transport = createConnectTransport({ baseUrl: "http://localhost:8080" });
const client = createClient(LedgerService, transport);

export async function fetchBalance(tenantId: string, accountId: string, currency: string) {
  return client.getBalance(
    { tenantId, accountId, currency },
    { headers: { "X-Tenant-Id": tenantId } }
  );
}
```

Generate the TypeScript types with `buf generate` after adding a `protoc-gen-es` plugin to `buf.gen.yaml`. Not included in this MVP.
