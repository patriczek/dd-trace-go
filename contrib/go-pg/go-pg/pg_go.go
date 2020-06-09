package pggo

import (
	"context"
	"github.com/go-pg/pg/v10"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"time"
)

const (
	gopgStartSpan  = "dd-trace-go:span"
)

func Wrap(db *pg.DB) *pg.DB {
	db.AddQueryHook(&Hook{})
	return db
}

type Hook struct {}

func (h *Hook) BeforeQuery(ctx context.Context,qe *pg.QueryEvent) (context.Context, error){
	if qe.Stash == nil{
		qe.Stash = make(map[interface{}]interface{})
	}
	qe.Stash[gopgStartSpan] = time.Now()
	return ctx, qe.Err
}


func (h *Hook) AfterQuery(ctx context.Context,qe *pg.QueryEvent) error{
	startSpan, ok := qe.Stash[gopgStartSpan]
	if !ok{
		return nil // TODO Return some error
	}
	
	spanStart := startSpan.(time.Time)
	unformatedSql, _ := qe.UnformattedQuery()

	opts := []ddtrace.StartSpanOption{
		tracer.StartTime(spanStart),
		//tracer.ServiceName(tracer), // Should be took from context, or consider if can be set through WithMethod
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.ResourceName(string(unformatedSql)),
	}

	span, _ := tracer.StartSpanFromContext(ctx, "SELECT", opts...) // TODO Write sql scanner for operation name
	span.Finish(tracer.WithError(qe.Err))

	return qe.Err
}
