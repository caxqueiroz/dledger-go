package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func (s *Server) PostJournal(ctx context.Context, req *connect.Request[ledgerv1.PostJournalRequest]) (*connect.Response[ledgerv1.PostJournalResponse], error) {
	r := req.Msg
	fr := &ledgerv1.ExecuteFlowRequest{
		TenantId:       r.GetTenantId(),
		FlowType:       "POST_JOURNAL",
		IdempotencyKey: r.GetIdempotencyKey(),
		SourceService:  r.GetSourceService(),
		ActorId:        r.GetActorId(),
		Steps: []*ledgerv1.Step{
			{StepId: "post", Journal: r.GetJournal()},
		},
	}
	res, err := s.ExecuteFlow(ctx, connect.NewRequest(fr))
	if err != nil {
		return nil, err
	}
	var journalID string
	if len(res.Msg.GetSteps()) > 0 {
		journalID = res.Msg.GetSteps()[0].GetJournalId()
	}
	return connect.NewResponse(&ledgerv1.PostJournalResponse{
		JournalId: journalID, FlowRunId: res.Msg.GetFlowRunId(),
	}), nil
}
