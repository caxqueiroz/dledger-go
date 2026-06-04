// Package dynamo implements repo.Store on DynamoDB-compatible storage using
// strongly-consistent reads, an in-memory write buffer, and a single
// TransactWriteItems commit guarded by optimistic version conditions.
package dynamo

const (
	gsi1                 = "gsi1"
	gsiOutboxPending     = "OBX#PENDING"
	gsiReservationExpiry = "RESEXP"
)

func accountPK(tenantID, accountID string) string { return "ACC#" + tenantID + "#" + accountID }

func accountUniqPK(tenantID, ownerType, ownerID, accountType, currency string) string {
	return "ACCU#" + tenantID + "#" + ownerType + "#" + ownerID + "#" + accountType + "#" + currency
}

func balancePK(tenantID, accountID, currency string) string {
	return "BAL#" + tenantID + "#" + accountID + "#" + currency
}

func journalPK(tenantID, journalID string) string { return "JRN#" + tenantID + "#" + journalID }
func eventUniqPK(eventID string) string           { return "EVT#" + eventID }
func flowPK(tenantID, flowRunID string) string    { return "FLOW#" + tenantID + "#" + flowRunID }
func flowIdempPK(tenantID, key string) string     { return "FIDEMP#" + tenantID + "#" + key }
func outboxPK(id string) string                   { return "OBX#" + id }
func reservationPK(tenantID, id string) string    { return "RES#" + tenantID + "#" + id }
func resIdempPK(tenantID, key string) string      { return "RIDEMP#" + tenantID + "#" + key }
