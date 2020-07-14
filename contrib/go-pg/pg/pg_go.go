// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package pg

import (
	"context"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/go-pg/pg/v10"
)

// Hook wraps the given DB to generate APM data.
func Hook(db *pg.DB) *pg.DB {
	db.AddQueryHook(&queryHook{})
	return db
}

type queryHook struct{}

func (h *queryHook) BeforeQuery(ctx context.Context, qe *pg.QueryEvent) (context.Context, error) {
	query, err := qe.UnformattedQuery()
	if err != nil {
		query = []byte("unknown")
	}

	opts := []ddtrace.StartSpanOption{
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.ResourceName(string(query)),
	}
	_, ctx = tracer.StartSpanFromContext(ctx, "gopg", opts...)
	return ctx, qe.Err
}

func (h *queryHook) AfterQuery(ctx context.Context, qe *pg.QueryEvent) error {
	span, ok := tracer.SpanFromContext(ctx)
	if !ok {
		return nil
	}
	span.Finish(tracer.WithError(qe.Err))

	return qe.Err
}