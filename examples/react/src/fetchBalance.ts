import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { LedgerService } from "./gen/ledger/v1/ledger_connect.js";

const transport = createConnectTransport({ baseUrl: "http://localhost:8080" });
const client = createClient(LedgerService, transport);

export async function fetchBalance(
  tenantId: string,
  accountId: string,
  currency: string,
) {
  return client.getBalance(
    { tenantId, accountId, currency },
    { headers: { "X-Tenant-Id": tenantId } },
  );
}
