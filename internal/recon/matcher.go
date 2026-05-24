// matcher.go: core reconciliation matching algorithm.
package recon

import (
	"context"
	"time"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// MatchResult summarises a reconciliation pass.
// Discrepancies are returned without IDs/BatchID/Status/CreatedAt set —
// the caller fills those in when persisting.
type MatchResult struct {
	Ingested          int32
	Matched           int32
	Mismatched        int32
	MissingInLedger   int32
	MissingInExternal int32
	Discrepancies     []ledger.Discrepancy
}

// Run loads external records and journals for (tenantID, source, window) and
// produces a MatchResult. It updates each external record's match_status
// (and matched_journal_id) inside tx. Discrepancy rows are NOT inserted here —
// the caller writes them after assigning batch_id and persisting.
func Run(
	ctx context.Context,
	tx repo.Tx,
	store repo.Store,
	tenantID, source string,
	windowStart, windowEnd time.Time,
) (MatchResult, error) {
	ext, err := store.ListExternalRecordsForRecon(ctx, tenantID, source, windowStart, windowEnd)
	if err != nil {
		return MatchResult{}, err
	}
	journals, err := store.ListJournalsForRecon(ctx, tenantID, source, windowStart, windowEnd)
	if err != nil {
		return MatchResult{}, err
	}

	byEventID := make(map[string]*ledger.Journal, len(journals))
	for i := range journals {
		j := &journals[i]
		byEventID[j.EventID] = j
	}

	res := MatchResult{Ingested: int32(len(ext))}

	for _, e := range ext {
		j, ok := byEventID[e.ExternalRef]
		if !ok {
			res.MissingInLedger++
			res.Discrepancies = append(res.Discrepancies, ledger.Discrepancy{
				TenantID:         tenantID,
				Type:             ledger.DiscrepancyMissingInLedger,
				ExternalRecordID: e.ID,
			})
			continue
		}
		// Match by ref. Verify amount if anchor account is set.
		if e.AccountID != "" {
			debits, credits, sErr := tx.SumJournalEntries(ctx, tenantID, j.ID, e.AccountID, e.Currency)
			if sErr != nil {
				return MatchResult{}, sErr
			}
			signed := debits.Sub(credits)
			if !signed.Equal(e.Amount) {
				if uErr := tx.UpdateExternalRecordMatch(ctx, tenantID, e.ID, ledger.ExternalMismatched, j.ID); uErr != nil {
					return MatchResult{}, uErr
				}
				res.Mismatched++
				res.Discrepancies = append(res.Discrepancies, ledger.Discrepancy{
					TenantID:         tenantID,
					Type:             ledger.DiscrepancyAmountMismatch,
					ExternalRecordID: e.ID,
					JournalID:        j.ID,
				})
				delete(byEventID, e.ExternalRef)
				continue
			}
		}
		if uErr := tx.UpdateExternalRecordMatch(ctx, tenantID, e.ID, ledger.ExternalMatched, j.ID); uErr != nil {
			return MatchResult{}, uErr
		}
		res.Matched++
		delete(byEventID, e.ExternalRef)
	}

	// Whatever remains in byEventID had no matching external record.
	for _, j := range byEventID {
		res.MissingInExternal++
		res.Discrepancies = append(res.Discrepancies, ledger.Discrepancy{
			TenantID:  tenantID,
			Type:      ledger.DiscrepancyMissingInExternal,
			JournalID: j.ID,
		})
	}

	return res, nil
}
