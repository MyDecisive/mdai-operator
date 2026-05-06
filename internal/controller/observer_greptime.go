package controller

import (
	"bytes"
	"errors"
	"fmt"
	"html"
	"html/template"
	"regexp"
	"strings"

	"github.com/decisiveai/mdai-operator/internal/greptimedb"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

type Greptime struct {
	greptimeDb        gorm.DB
	greptimeTemplates map[string]string
}

const greptimeSinkTableTtl = "3months"
const greptimeAggregateInterval = "5 second"

var greptimePipelineBaseNames = map[string]struct {
	sinkTable string
	flowName  string
}{
	"traffic": {
		sinkTable: "golden_signals_traffic",
		flowName:  "golden_signals_traffic_flow",
	},
	"latency": {
		sinkTable: "golden_signals_duration",
		flowName:  "golden_signals_duration_flow",
	},
	"errors": {
		sinkTable: "golden_signals_errors",
		flowName:  "golden_signals_errors_flow",
	},
}

type Pipeline struct {
	Name         string
	Schema       string
	SourceTable  string
	SinkTable    string
	FlowName     string
	SinkTemplate string
	FlowTemplate string
	ValueColumn  string
	ValueType    string
	ValueExpr    string
	PrimaryKeys  []string
	TimeInterval string
	SinkTableTTL string
	WhereClause  string
}

type TemplateData struct {
	Schema         string
	SourceTable    string
	SinkTable      string
	FlowName       string
	Dimensions     []string
	PrimaryKeys    []string
	SinkTableTTL   string
	ValueColumn    string
	ValueType      string
	ValueExpr      string
	TimeSelectExpr string
	TimeGroupExpr  string
	WhereClause    string
}

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var columnPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
var intervalPattern = regexp.MustCompile(`^[0-9]+\s+(millisecond|milliseconds|second|seconds|minute|minutes|hour|hours)$`)
var ttlPattern = regexp.MustCompile(`(?i)\bttl\s*=\s*'([^']+)'`)
var flowIntervalPattern = regexp.MustCompile(`(?i)date_bin\('([^']+)'::INTERVAL,\s*timestamp\)`)
var primaryKeyPattern = regexp.MustCompile(`(?i)PRIMARY KEY\s*\(([^)]*)\)`)

var errFlowNotFound = errors.New("flow not found")

type sinkTableChange int

const (
	sinkTableUnchanged sinkTableChange = iota
	sinkTableTTLUpdated
	sinkTableRecreated
)

func doGreptime(greptime Greptime, observerName string, dimensions []string, primaryKey string, sinkTableTTL string, aggregateInterval string) error {
	db := greptime.greptimeDb
	templates := greptime.greptimeTemplates
	schema := greptimedb.DatabaseNameFromEnv()

	pipelines := []Pipeline{
		{
			Name:         "traffic",
			Schema:       schema,
			SourceTable:  "opentelemetry_traces",
			SinkTable:    prefixedGreptimeObjectName(observerName, greptimePipelineBaseNames["traffic"].sinkTable),
			FlowName:     prefixedGreptimeObjectName(observerName, greptimePipelineBaseNames["traffic"].flowName),
			SinkTemplate: "sink_traffic",
			FlowTemplate: "flow_traffic",
			ValueColumn:  "total_count",
			ValueType:    "INT64",
			ValueExpr:    fmt.Sprintf("COUNT(%s)", primaryKey),
			PrimaryKeys:  []string{primaryKey},
			TimeInterval: aggregateInterval,
			SinkTableTTL: sinkTableTTL,
			WhereClause:  "span_status_code = 'STATUS_CODE_UNSET'",
		},
		{
			Name:         "latency",
			Schema:       schema,
			SourceTable:  "opentelemetry_traces",
			SinkTable:    prefixedGreptimeObjectName(observerName, greptimePipelineBaseNames["latency"].sinkTable),
			FlowName:     prefixedGreptimeObjectName(observerName, greptimePipelineBaseNames["latency"].flowName),
			SinkTemplate: "sink_latency",
			FlowTemplate: "flow_latency",
			ValueColumn:  "latency_sketch",
			ValueType:    "BINARY",
			ValueExpr:    "uddsketch_state(128, 0.01, duration_nano)",
			PrimaryKeys:  []string{primaryKey},
			TimeInterval: aggregateInterval,
			SinkTableTTL: sinkTableTTL,
			WhereClause:  "span_status_code = 'STATUS_CODE_UNSET'",
		},
		{
			Name:         "errors",
			Schema:       schema,
			SourceTable:  "opentelemetry_traces",
			SinkTable:    prefixedGreptimeObjectName(observerName, greptimePipelineBaseNames["errors"].sinkTable),
			FlowName:     prefixedGreptimeObjectName(observerName, greptimePipelineBaseNames["errors"].flowName),
			SinkTemplate: "sink_errors",
			FlowTemplate: "flow_errors",
			ValueColumn:  "total_count",
			ValueType:    "INT64",
			ValueExpr:    fmt.Sprintf("COUNT(%s)", primaryKey),
			PrimaryKeys:  []string{primaryKey},
			TimeInterval: aggregateInterval,
			SinkTableTTL: sinkTableTTL,
			WhereClause:  "span_status_code = 'STATUS_CODE_ERROR'",
		},
	}

	for _, p := range pipelines {
		data := TemplateData{
			Schema:         p.Schema,
			SourceTable:    p.SourceTable,
			SinkTable:      p.SinkTable,
			FlowName:       p.FlowName,
			Dimensions:     dimensions,
			PrimaryKeys:    p.PrimaryKeys,
			SinkTableTTL:   p.SinkTableTTL,
			ValueColumn:    p.ValueColumn,
			ValueType:      p.ValueType,
			ValueExpr:      p.ValueExpr,
			TimeSelectExpr: buildTimeSelectExpr(p.TimeInterval),
			TimeGroupExpr:  buildTimeGroupExpr(p.TimeInterval),
			WhereClause:    p.WhereClause,
		}

		if err := validatePipeline(p, data); err != nil {
			return fmt.Errorf("pipeline %s: %w", p.Name, err)
		}

		sinkDDL, err := renderSQLTemplate(templates, p.SinkTemplate, data)
		if err != nil {
			return fmt.Errorf("pipeline %s sink template: %w", p.Name, err)
		}

		sinkChange, err := ensureSinkTable(&db, data, sinkDDL)
		if err != nil {
			return fmt.Errorf("pipeline %s ensure sink table: %w", p.Name, err)
		}

		flowDDL, err := renderSQLTemplate(templates, p.FlowTemplate, data)
		if err != nil {
			return fmt.Errorf("pipeline %s flow template: %w", p.Name, err)
		}
		flowChanged, err := ensureFlow(&db, data, flowDDL, sinkChange == sinkTableRecreated)
		if err != nil {
			return fmt.Errorf("pipeline %s ensure flow: %w", p.Name, err)
		}
		if !flowChanged {
			continue
		}
	}
	return nil
}

func ensureFlow(db *gorm.DB, d TemplateData, flowDDL string, sinkChanged bool) (bool, error) {
	if sinkChanged {
		if err := db.Exec(flowDDL).Error; err != nil {
			return false, fmt.Errorf("create/replace flow: %w", err)
		}
		return true, nil
	}

	existingCreateStatement, err := showCreateFlow(db, d.FlowName)
	if err != nil {
		if isFlowNotFoundError(err) {
			if err := db.Exec(flowDDL).Error; err != nil {
				return false, fmt.Errorf("create missing flow: %w", err)
			}
			return true, nil
		}
		return false, fmt.Errorf("show create flow: %w", err)
	}

	if !flowNeedsUpdate(existingFlowAggregateInterval(existingCreateStatement), d.TimeGroupExpr) {
		return false, nil
	}

	if err := db.Exec(flowDDL).Error; err != nil {
		return false, fmt.Errorf("create/replace flow: %w", err)
	}
	return true, nil
}

func ensureSinkTable(db *gorm.DB, d TemplateData, sinkDDL string) (sinkTableChange, error) {
	existingCols, err := listTableColumns(db, d.Schema, d.SinkTable)
	if err != nil {
		return sinkTableUnchanged, fmt.Errorf("list table columns: %w", err)
	}

	if len(existingCols) == 0 {
		if err := db.Exec(sinkDDL).Error; err != nil {
			return sinkTableUnchanged, fmt.Errorf("create sink table: %w", err)
		}
		return sinkTableRecreated, nil
	}

	existingCreateStatement, err := showCreateTable(db, d.Schema, d.SinkTable)
	if err != nil {
		return sinkTableUnchanged, fmt.Errorf("show create table: %w", err)
	}

	if sinkTableNeedsRecreate(existingCols, existingPrimaryKeys(existingCreateStatement), d) {
		dropFlow := fmt.Sprintf("DROP FLOW IF EXISTS %s", quoteIdentifier(d.FlowName))
		if err := db.Exec(dropFlow).Error; err != nil {
			return sinkTableUnchanged, fmt.Errorf("drop flow: %w", err)
		}
		dropTable := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(d.SinkTable))
		if err := db.Exec(dropTable).Error; err != nil {
			return sinkTableUnchanged, fmt.Errorf("drop sink table: %w", err)
		}
		if err := db.Exec(sinkDDL).Error; err != nil {
			return sinkTableUnchanged, fmt.Errorf("recreate sink table: %w", err)
		}
		return sinkTableRecreated, nil
	}

	if sinkTableTTLNeedsUpdate(existingSinkTableTTL(existingCreateStatement), d.SinkTableTTL) {
		if err := alterSinkTableTTL(db, d.Schema, d.SinkTable, d.SinkTableTTL); err != nil {
			return sinkTableUnchanged, fmt.Errorf("alter sink table ttl: %w", err)
		}
		return sinkTableTTLUpdated, nil
	}

	return sinkTableUnchanged, nil
}

func sinkTableNeedsRecreate(existingCols []string, existingPrimaryKeys []string, d TemplateData) bool {
	existingDimensions := existingDimensionColumns(existingCols, d)
	if !sameStringSet(existingDimensions, d.Dimensions) {
		return true
	}
	return !sameStringSet(existingPrimaryKeys, d.PrimaryKeys)
}

func sinkTableTTLNeedsUpdate(existingTTL, desiredTTL string) bool {
	return normalizeTTL(existingTTL) != normalizeTTL(desiredTTL)
}

func existingDimensionColumns(existingCols []string, d TemplateData) []string {
	primaryKeys := make(map[string]struct{}, len(d.PrimaryKeys))
	for _, c := range d.PrimaryKeys {
		primaryKeys[c] = struct{}{}
	}

	managedNonDimension := map[string]struct{}{
		d.ValueColumn: {},
		"time_window": {},
		"update_at":   {},
	}

	dims := make([]string, 0, len(existingCols))
	for _, c := range existingCols {
		if _, ok := managedNonDimension[c]; ok {
			continue
		}
		// FIX: primary keys should be managed as well
		//if _, ok := primaryKeys[c]; ok {
		//	continue
		//}
		dims = append(dims, c)
	}
	return dims
}

func validatePipeline(p Pipeline, d TemplateData) error {
	for _, id := range []struct {
		name  string
		label string
	}{
		{name: p.Name, label: "pipeline name"},
		{name: d.Schema, label: "schema"},
		{name: d.SourceTable, label: "source table"},
		{name: d.ValueColumn, label: "value column"},
	} {
		if !identPattern.MatchString(id.name) {
			return fmt.Errorf("invalid %s identifier: %s", id.label, id.name)
		}
	}

	//if len(d.Dimensions) == 0 {
	//	return fmt.Errorf("at least one dimension is required")
	//}
	if len(d.PrimaryKeys) == 0 {
		return fmt.Errorf("at least one primary key column is required")
	}

	for _, dim := range d.Dimensions {
		if !columnPattern.MatchString(dim) {
			return fmt.Errorf("invalid dimension name: %s", dim)
		}
	}
	for _, pk := range d.PrimaryKeys {
		if !columnPattern.MatchString(pk) {
			return fmt.Errorf("invalid primary key name: %s", pk)
		}
	}

	for _, field := range []struct {
		value string
		label string
	}{
		{value: p.SinkTemplate, label: "sink template"},
		{value: p.FlowTemplate, label: "flow template"},
		{value: p.TimeInterval, label: "time interval"},
		{value: d.ValueType, label: "value type"},
		{value: d.ValueExpr, label: "value expression"},
		{value: d.TimeSelectExpr, label: "time select expression"},
		{value: d.TimeGroupExpr, label: "time group expression"},
		{value: d.WhereClause, label: "where clause"},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("empty %s", field.label)
		}
	}
	if !intervalPattern.MatchString(p.TimeInterval) {
		return fmt.Errorf("invalid time interval: %s", p.TimeInterval)
	}

	return nil
}

func buildTimeGroupExpr(interval string) string {
	return fmt.Sprintf("date_bin('%s'::INTERVAL, timestamp)", interval)
}

func buildTimeSelectExpr(interval string) string {
	return fmt.Sprintf("%s AS time_window", buildTimeGroupExpr(interval))
}

func loadTemplates() map[string]string {
	return map[string]string{
		"sink_traffic": sinkTrafficTemplate,
		"flow_traffic": flowTrafficTemplate,
		"sink_latency": sinkLatencyTemplate,
		"flow_latency": flowLatencyTemplate,
		"sink_errors":  sinkErrorsTemplate,
		"flow_errors":  flowErrorsTemplate,
	}
}

func renderSQLTemplate(templates map[string]string, templateName string, data TemplateData) (string, error) {
	templateText, ok := templates[templateName]
	if !ok {
		return "", fmt.Errorf("template not found: %s", templateName)
	}

	tmpl, err := template.New(templateName).Funcs(template.FuncMap{
		"quoteIdentifier": quoteIdentifier,
		"dimensionProjection": func(col string) string {
			q := quoteIdentifier(col)
			return fmt.Sprintf("%s AS %s", q, q)
		},
		"joinDimensionNames": func(dimensions []string) string {
			names := make([]string, 0, len(dimensions))
			for _, d := range dimensions {
				names = append(names, quoteIdentifier(d))
			}
			return strings.Join(names, ", ")
		},
		"joinPrimaryKeys": func(keys []string) string {
			names := make([]string, 0, len(keys))
			for _, k := range keys {
				names = append(names, quoteIdentifier(k))
			}
			return strings.Join(names, ", ")
		},
	}).Parse(templateText)
	if err != nil {
		return "", err
	}

	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return "", err
	}
	return html.UnescapeString(out.String()), nil
}

func listTableColumns(db *gorm.DB, schema, table string) ([]string, error) {
	var columns []string
	err := db.Raw(
		`SELECT column_name
		 FROM information_schema.columns
		 WHERE table_schema = ? AND table_name = ?`,
		schema, table,
	).Scan(&columns).Error
	if err != nil {
		return nil, err
	}
	return columns, nil
}

func showCreateTable(db *gorm.DB, schema, table string) (string, error) {
	rows, err := db.Raw(
		fmt.Sprintf("SHOW CREATE TABLE %s.%s", quoteIdentifier(schema), quoteIdentifier(table)),
	).Rows()
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if !rows.Next() {
		return "", fmt.Errorf("no SHOW CREATE TABLE result for %s.%s", schema, table)
	}

	var tableName, createStatement string
	if err := rows.Scan(&tableName, &createStatement); err != nil {
		return "", err
	}

	return createStatement, nil
}

func alterSinkTableTTL(db *gorm.DB, schema, table, ttl string) error {
	return db.Exec(
		fmt.Sprintf(
			"ALTER TABLE %s.%s SET 'ttl' = '%s'",
			quoteIdentifier(schema),
			quoteIdentifier(table),
			ttl,
		),
	).Error
}

func showCreateFlow(db *gorm.DB, flow string) (string, error) {
	rows, err := db.Raw(
		fmt.Sprintf("SHOW CREATE FLOW %s", quoteIdentifier(flow)),
	).Rows()
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if !rows.Next() {
		return "", fmt.Errorf("%w: %s", errFlowNotFound, flow)
	}

	var flowName, createStatement string
	if err := rows.Scan(&flowName, &createStatement); err != nil {
		return "", err
	}

	return createStatement, nil
}

func isFlowNotFoundError(err error) bool {
	if errors.Is(err, errFlowNotFound) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P01" && strings.Contains(pgErr.Message, "Flow not found") {
		return true
	}

	return false
}

func existingSinkTableTTL(createStatement string) string {
	matches := ttlPattern.FindStringSubmatch(createStatement)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func existingPrimaryKeys(createStatement string) []string {
	matches := primaryKeyPattern.FindStringSubmatch(createStatement)
	if len(matches) < 2 {
		return nil
	}

	parts := strings.Split(matches[1], ",")
	keys := make([]string, 0, len(parts))
	for _, part := range parts {
		key := strings.TrimSpace(part)
		key = strings.Trim(key, `"`)
		if key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func normalizeTTL(ttl string) string {
	return strings.TrimSpace(ttl)
}

func existingFlowAggregateInterval(createStatement string) string {
	matches := flowIntervalPattern.FindStringSubmatch(createStatement)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func flowNeedsUpdate(existingAggregateInterval, desiredTimeGroupExpr string) bool {
	return normalizeInterval(existingAggregateInterval) != normalizeInterval(existingFlowAggregateInterval(desiredTimeGroupExpr))
}

func normalizeInterval(interval string) string {
	return strings.TrimSpace(interval)
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		seen[v]--
		if seen[v] < 0 {
			return false
		}
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}

const sinkTrafficTemplate = `
CREATE TABLE IF NOT EXISTS {{ quoteIdentifier .SinkTable }} (
{{- range .Dimensions }}
  {{ quoteIdentifier . }} STRING INVERTED INDEX,
{{- end }}
  {{ .ValueColumn }} {{ .ValueType }},
  time_window TIMESTAMP TIME INDEX,
  update_at TIMESTAMP,
  PRIMARY KEY ({{ joinPrimaryKeys .PrimaryKeys }})
) WITH (ttl='{{ .SinkTableTTL }}');`

const flowTrafficTemplate = `
CREATE OR REPLACE FLOW {{ quoteIdentifier .FlowName }}
SINK TO {{ quoteIdentifier .SinkTable }}
AS
SELECT
{{- range .Dimensions }}
  {{ dimensionProjection . }},
{{- end }}
  {{ .ValueExpr }} AS {{ .ValueColumn }},
  {{ .TimeSelectExpr }}
FROM {{ .SourceTable }}
WHERE {{ .WhereClause }}
GROUP BY
{{- range .Dimensions }}
  {{ quoteIdentifier . }},
{{- end }}
  time_window;`

const sinkLatencyTemplate = `
CREATE TABLE IF NOT EXISTS {{ quoteIdentifier .SinkTable }} (
{{- range .Dimensions }}
  {{ quoteIdentifier . }} STRING INVERTED INDEX,
{{- end }}
  {{ .ValueColumn }} {{ .ValueType }},
  time_window TIMESTAMP TIME INDEX,
  update_at TIMESTAMP,
  PRIMARY KEY ({{ joinPrimaryKeys .PrimaryKeys }})
) WITH (ttl='{{ .SinkTableTTL }}');`

const flowLatencyTemplate = `
CREATE OR REPLACE FLOW {{ quoteIdentifier .FlowName }}
SINK TO {{ quoteIdentifier .SinkTable }}
AS
SELECT
{{- range .Dimensions }}
  {{ dimensionProjection . }},
{{- end }}
  {{ .ValueExpr }} AS {{ .ValueColumn }},
  {{ .TimeSelectExpr }}
FROM {{ .SourceTable }}
WHERE {{ .WhereClause }}
GROUP BY
{{- range .Dimensions }}
  {{ quoteIdentifier . }},
{{- end }}
  time_window;`

const sinkErrorsTemplate = `
CREATE TABLE IF NOT EXISTS {{ quoteIdentifier .SinkTable }} (
{{- range .Dimensions }}
  {{ quoteIdentifier . }} STRING INVERTED INDEX,
{{- end }}
  {{ .ValueColumn }} {{ .ValueType }},
  time_window TIMESTAMP TIME INDEX,
  update_at TIMESTAMP,
  PRIMARY KEY ({{ joinPrimaryKeys .PrimaryKeys }})
) WITH (ttl='{{ .SinkTableTTL }}');`

const flowErrorsTemplate = `
CREATE OR REPLACE FLOW {{ quoteIdentifier .FlowName }}
SINK TO {{ quoteIdentifier .SinkTable }}
AS
SELECT
{{- range .Dimensions }}
  {{ dimensionProjection . }},
{{- end }}
  {{ .ValueExpr }} AS {{ .ValueColumn }},
  {{ .TimeSelectExpr }}
FROM {{ .SourceTable }}
WHERE {{ .WhereClause }}
GROUP BY
{{- range .Dimensions }}
  {{ quoteIdentifier . }},
{{- end }}
  time_window;`

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func prefixedGreptimeObjectName(observerName, baseName string) string {
	return fmt.Sprintf("%s_%s", observerName, baseName)
}
