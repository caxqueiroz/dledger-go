package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// GetReservation fetches a single reservation by (tenantID, reservationID).
// Returns CodeReservationNotFound DomainError when absent — matching the
// sqlite contract.
func (s *Store) GetReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error) {
	var it reservationItem
	found, err := s.getItem(ctx, reservationPK(tenantID, reservationID), &it)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ledger.NewDomainError(ledger.CodeReservationNotFound, reservationID)
	}
	r, _, err := reservationFromItem(it)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// ListExpiredReservations returns up to limit reservations whose expiry time
// is strictly before now and that are still in an active (non-terminal) status.
//
// It queries GSI1 with:
//
//	gsi1pk = "RESEXP"
//	gsi1sk < fmtTime(now)
//
// The GSI1SK format is "<expires_at>#<tenant>#<id>". Terminal reservations
// are removed from the GSI (gsi1pk/gsi1sk are omitempty on the item), so only
// active expiring reservations appear.
func (s *Store) ListExpiredReservations(ctx context.Context, now time.Time, limit int) ([]repo.ExpiredReservation, error) {
	if limit <= 0 {
		limit = 100
	}
	cutoff := fmtTime(now)

	resp, err := s.db.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		IndexName:              aws.String(gsi1),
		KeyConditionExpression: aws.String("gsi1pk = :pk AND gsi1sk < :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: gsiReservationExpiry},
			":sk": &types.AttributeValueMemberS{Value: cutoff},
		},
		Limit: aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, fmt.Errorf("ListExpiredReservations query: %w", err)
	}

	out := make([]repo.ExpiredReservation, 0, len(resp.Items))
	for _, raw := range resp.Items {
		var it reservationItem
		if err := attributevalue.UnmarshalMap(raw, &it); err != nil {
			return nil, fmt.Errorf("ListExpiredReservations unmarshal: %w", err)
		}
		out = append(out, repo.ExpiredReservation{
			ID:       it.ReservationID,
			TenantID: it.TenantID,
		})
	}
	return out, nil
}

// ListReservations returns reservations filtered by ListReservationsInput.
//
// Index strategy:
//   - If OwnerType and OwnerID are set: query GSI2 (gsi2pk =
//     "RESOWN#<tenant>#<ownerType>#<ownerID>"); then filter by Status client-side.
//   - If neither is set: caller provided only tenant + optional status; we fall
//     back to a GSI2 broad query keyed on a tenant-wide partition. Since the
//     plan forbids scans, and there is no tenant-wide partition in GSI2, we use
//     a FilterExpression over the full GSI2 range — but that requires a HashKey.
//     In practice, GetWallet always provides OwnerType + OwnerID, so the
//     tenant-only path is unused by production code; we return an empty slice
//     for that case rather than scanning.
//
// Scans are FORBIDDEN. When OwnerType or OwnerID is empty the method returns
// an empty result so callers that do not set those filters get a safe (if
// incomplete) response.
func (s *Store) ListReservations(ctx context.Context, in repo.ListReservationsInput) ([]ledger.Reservation, error) {
	limit := in.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	// Without owner filter we cannot query without a scan — return empty.
	if in.OwnerType == "" || in.OwnerID == "" {
		return []ledger.Reservation{}, nil
	}

	gsi2pk := gsiResOwnerPK(in.TenantID, in.OwnerType, in.OwnerID)

	resp, err := s.db.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		IndexName:              aws.String(gsi2),
		KeyConditionExpression: aws.String("gsi2pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: gsi2pk},
		},
		// Fetch up to 3× limit to account for client-side status filtering.
		Limit: aws.Int32(int32(limit * 3)),
	})
	if err != nil {
		return nil, fmt.Errorf("ListReservations query: %w", err)
	}

	out := make([]ledger.Reservation, 0, len(resp.Items))
	for _, raw := range resp.Items {
		var it reservationItem
		if err := attributevalue.UnmarshalMap(raw, &it); err != nil {
			return nil, fmt.Errorf("ListReservations unmarshal: %w", err)
		}
		// Client-side status filter (if requested).
		if in.Status != "" && it.Status != in.Status {
			continue
		}
		r, _, err := reservationFromItem(it)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
